/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.broker.offset.order;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.SelectMappedBufferResult;

/**
 * Sharding Key缓存管理类
 * 负责管理可用的过期消息和被释放锁的消息，提供快速访问能力
 *
 * 核心设计：双缓存机制
 * - 可用消息缓存 (Available Cache)：存放因为锁被释放（通过 ACK 或超时）而被"激活"的消息
 * - 不可用消息缓存 (Unavailable Cache)：存放那些被乐观读取出来，但因 ShardingKey 被锁定而无法立即分发的消息
 */
public class ShardingKeyCache {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    /**
     * 缓存的消息信息
     * 一个 CachedMessage 代表一批相同 shardingKey 的消息，共享同一个 GetMessageResult
     */
    public static class CachedMessage {
        private final String topic;
        private final String group;
        private final int queueId;
        private final String shardingKey;
        private final GetMessageResult messageResult;
        private final List<Long> offsets;
        private final long createTime;

        public CachedMessage(String topic, String group, int queueId, String shardingKey,
            GetMessageResult messageResult, List<Long> offsets) {
            this.topic = topic;
            this.group = group;
            this.queueId = queueId;
            this.shardingKey = shardingKey;
            this.messageResult = messageResult;
            this.offsets = new ArrayList<>(offsets); // 防止外部修改
            this.createTime = System.currentTimeMillis();
        }

        /**
         * 检查消息是否过期
         */
        public boolean isExpired(long maxCacheTime) {
            return System.currentTimeMillis() - createTime > maxCacheTime;
        }

        /**
         * 匹配队列
         */
        public boolean matchQueue(String topic, String group, int queueId) {
            return this.topic.equals(topic) && this.group.equals(group) && this.queueId == queueId;
        }

        /**
         * 匹配 shardingKey
         */
        public boolean matchShardingKey(String shardingKey) {
            return this.shardingKey.equals(shardingKey);
        }

        /**
         * 获取最小 offset
         */
        public long getMinOffset() {
            return offsets.isEmpty() ? -1L : Collections.min(offsets);
        }

        // Getters
        public String getTopic() {
            return topic;
        }

        public String getGroup() {
            return group;
        }

        public int getQueueId() {
            return queueId;
        }

        public String getShardingKey() {
            return shardingKey;
        }

        public GetMessageResult getMessageResult() {
            return messageResult;
        }

        public List<Long> getOffsets() {
            return new ArrayList<>(offsets); // 返回副本防止外部修改
        }

        public long getCreateTime() {
            return createTime;
        }

        @Override
        public String toString() {
            return "CachedMessage{" +
                "topic='" + topic + '\'' +
                ", group='" + group + '\'' +
                ", queueId=" + queueId +
                ", shardingKey='" + shardingKey + '\'' +
                ", offsets=" + offsets +
                ", createTime=" + createTime +
                '}';
        }
    }

    // 可用消息队列，按 topic@group@queueId 分组，不用区分 shardingKey 了
    private final ConcurrentHashMap<String, ConcurrentLinkedQueue<CachedMessage>> availableMessagesMap;

    // 不可用消息缓存：QueueKey -> ShardingKey -> 消息
    private final ConcurrentHashMap<String/*queueKey*/, ConcurrentHashMap<String/*shardingKey*/, CachedMessage>> unavailableMessagesMap;

    // 缓存统计信息
    private final AtomicLong totalCachedMessages;
    private final AtomicLong totalHits;
    private final AtomicLong totalMisses;

    // 缓存配置
    private static final long MAX_CACHE_TIME = 30 * 1000; // 30秒最大缓存时间
    private static final int MAX_QUEUE_SIZE = 1000; // 单个队列最大缓存消息数

    public ShardingKeyCache() {
        this.availableMessagesMap = new ConcurrentHashMap<>();
        this.unavailableMessagesMap = new ConcurrentHashMap<>();
        this.totalCachedMessages = new AtomicLong(0);
        this.totalHits = new AtomicLong(0);
        this.totalMisses = new AtomicLong(0);
    }

    /**
     * 添加可用消息到缓存
     *
     * @param topic         主题
     * @param group         消费组
     * @param queueId       队列ID
     * @param shardingKey   shardingKey
     * @param messageResult 消息结果
     * @param offsets       消息offset列表
     */
    public void addAvailableMessage(String topic, String group, int queueId, String shardingKey,
        GetMessageResult messageResult, List<Long> offsets) {
        if (messageResult == null || offsets == null || offsets.isEmpty()) {
            return;
        }

        String queueKey = MessageShardingKeyUtil.buildTopicGroupQueueIdentifier(topic, group, queueId);
        ConcurrentLinkedQueue<CachedMessage> queue = availableMessagesMap.computeIfAbsent(
            queueKey, k -> new ConcurrentLinkedQueue<>());

        // 检查队列大小限制
        if (queue.size() >= MAX_QUEUE_SIZE) {
            CachedMessage removed = queue.poll();
            if (removed != null) {
                totalCachedMessages.decrementAndGet();
                log.warn("缓存队列已满，移除最旧消息: {}", removed);
            }
        }

        CachedMessage availableMessage = new CachedMessage(topic, group, queueId, shardingKey, messageResult, offsets);
        queue.offer(availableMessage);
        totalCachedMessages.incrementAndGet();

        log.info("添加可用消息批次到缓存: topic={}, group={}, queueId={}, shardingKey={}, 消息数量={}",
            topic, group, queueId, shardingKey, offsets.size());
    }

    /**
     * 添加暂时不可用的消息到缓存
     *
     * @param topic       主题
     * @param group       消费组
     * @param queueId     队列ID
     * @param shardingKey shardingKey
     * @param messageResult 消息结果
     * @param offsets     消息offset列表
     */
    public void addUnavailableMessage(String topic, String group, int queueId, String shardingKey,
        GetMessageResult messageResult, List<Long> offsets) {
        if (messageResult == null || offsets == null || offsets.isEmpty()) {
            return;
        }

        String queueKey = MessageShardingKeyUtil.buildTopicGroupQueueIdentifier(topic, group, queueId);
        ConcurrentHashMap<String, CachedMessage> shardingKeyMap =
            unavailableMessagesMap.computeIfAbsent(queueKey, k -> new ConcurrentHashMap<>());

        // 一批相同 shardingKey 的消息创建一个 GetMessageResult
        CachedMessage unavailableMessage = new CachedMessage(topic, group, queueId, shardingKey, messageResult, offsets);
        shardingKeyMap.put(shardingKey, unavailableMessage);
        totalCachedMessages.incrementAndGet();

        log.info("添加不可用消息批次到缓存: topic={}, group={}, queueId={}, shardingKey={}, 消息数量={}",
            topic, group, queueId, shardingKey, offsets.size());
    }

    /**
     * 激活指定shardingKey的消息
     * 将不可用缓存中的消息转移到可用缓存
     *
     * @param topic       主题
     * @param group       消费组
     * @param queueId     队列ID
     * @param shardingKey shardingKey
     * @return 是否成功激活了消息
     */
    public boolean activateMessages(String topic, String group, int queueId, String shardingKey) {
        String queueKey = MessageShardingKeyUtil.buildTopicGroupQueueIdentifier(topic, group, queueId);
        ConcurrentHashMap<String, CachedMessage> shardingKeyMap = unavailableMessagesMap.get(queueKey);
        if (shardingKeyMap == null) {
            return false;
        }

        CachedMessage cachedMessage = shardingKeyMap.remove(shardingKey);
        if (cachedMessage == null) {
            return false;
        }

        // 将整个消息批次移动到可用队列
        ConcurrentLinkedQueue<CachedMessage> availableQueue = availableMessagesMap.computeIfAbsent(queueKey, k -> new ConcurrentLinkedQueue<>());
        availableQueue.offer(cachedMessage);

        log.info("激活消息批次成功: topic={}, group={}, queueId={}, shardingKey={}, 激活消息数量={}",
            topic, group, queueId, shardingKey, cachedMessage.getOffsets().size());
        return true;
    }

    /**
     * 获取指定队列的可用消息（优先从可用缓存获取）
     */
    public List<CachedMessage> getAvailableMessages(String topic, String group, int queueId, int maxCount) {
        String queueKey = MessageShardingKeyUtil.buildTopicGroupQueueIdentifier(topic, group, queueId);
        ConcurrentLinkedQueue<CachedMessage> queue = availableMessagesMap.get(queueKey);

        if (queue == null || queue.isEmpty()) {
            totalMisses.incrementAndGet();
            return new ArrayList<>();
        }

        List<CachedMessage> result = new ArrayList<>();
        int count = 0;

        while (count < maxCount && !queue.isEmpty()) {
            CachedMessage message = queue.poll();
            if (message == null) {
                break;
            }

            // 检查消息是否过期
            if (message.isExpired(MAX_CACHE_TIME)) {
                totalCachedMessages.decrementAndGet();
                log.warn("移除过期缓存消息: {}", message);
                continue;
            }

            result.add(message);
            totalCachedMessages.decrementAndGet();
            count++;
        }

        if (!result.isEmpty()) {
            totalHits.incrementAndGet();
            log.info("从缓存中获取可用消息成功: topic={}, group={}, queueId={}, 获取消息批次数量={}",
                topic, group, queueId, result.size());
        } else {
            totalMisses.incrementAndGet();
            log.debug("从缓存中未找到可用消息: topic={}, group={}, queueId={}", topic, group, queueId);
        }

        return result;
    }

    /**
     * 获取指定 shardingKey 的可用消息
     */
    public List<CachedMessage> getAvailableMessagesByShardingKey(String topic, String group, int queueId,
        String shardingKey, int maxCount) {
        String queueKey = MessageShardingKeyUtil.buildTopicGroupQueueIdentifier(topic, group, queueId);
        ConcurrentLinkedQueue<CachedMessage> queue = availableMessagesMap.get(queueKey);

        if (queue == null || queue.isEmpty()) {
            totalMisses.incrementAndGet();
            return new ArrayList<>();
        }

        List<CachedMessage> result = new ArrayList<>();
        List<CachedMessage> toRequeue = new ArrayList<>();
        int count = 0;

        // 遍历队列寻找匹配的 shardingKey
        while (count < maxCount && !queue.isEmpty()) {
            CachedMessage message = queue.poll();
            if (message == null) {
                break;
            }

            // 检查消息是否过期
            if (message.isExpired(MAX_CACHE_TIME)) {
                totalCachedMessages.decrementAndGet();
                log.warn("移除过期缓存消息: {}", message);
                continue;
            }

            if (message.matchShardingKey(shardingKey)) {
                result.add(message);
                totalCachedMessages.decrementAndGet();
                count++;
            } else {
                // 不匹配的消息重新放回队列
                toRequeue.add(message);
            }
        }

        // 将不匹配的消息重新放回队列
        for (CachedMessage message : toRequeue) {
            queue.offer(message);
        }

        if (!result.isEmpty()) {
            totalHits.incrementAndGet();
            log.info("按ShardingKey从缓存中获取可用消息成功: topic={}, group={}, queueId={}, shardingKey={}, 获取消息批次数量={}",
                topic, group, queueId, shardingKey, result.size());
        } else {
            totalMisses.incrementAndGet();
            log.debug("按ShardingKey从缓存中未找到可用消息: topic={}, group={}, queueId={}, shardingKey={}",
                topic, group, queueId, shardingKey);
        }

        return result;
    }

    /**
     * 检查指定队列是否有可用消息
     */
    public boolean hasAvailableMessages(String topic, String group, int queueId) {
        return getQueueCacheSize(topic, group, queueId) > 0;
    }

    /**
     * 获取指定队列的可用缓存大小
     */
    public int getQueueCacheSize(String topic, String group, int queueId) {
        String queueKey = MessageShardingKeyUtil.buildTopicGroupQueueIdentifier(topic, group, queueId);
        ConcurrentLinkedQueue<CachedMessage> queue = availableMessagesMap.get(queueKey);
        return queue != null ? queue.size() : 0;
    }

    /**
     * 获取指定队列的不可用缓存大小
     */
    public int getUnavailableQueueCacheSize(String topic, String group, int queueId) {
        String queueKey = MessageShardingKeyUtil.buildTopicGroupQueueIdentifier(topic, group, queueId);
        ConcurrentHashMap<String, CachedMessage> shardingKeyMap = unavailableMessagesMap.get(queueKey);
        if (shardingKeyMap == null) {
            return 0;
        }

        return shardingKeyMap.size();
    }

    /**
     * 清除指定队列的所有缓存消息
     */
    public void clearQueueCache(String topic, String group, int queueId) {
        String queueKey = MessageShardingKeyUtil.buildTopicGroupQueueIdentifier(topic, group, queueId);

        // 清除可用缓存
        ConcurrentLinkedQueue<CachedMessage> availableQueue = availableMessagesMap.remove(queueKey);
        int removedCount = 0;
        if (availableQueue != null) {
            removedCount += availableQueue.size();
            totalCachedMessages.addAndGet(-availableQueue.size());
        }

        // 清除不可用缓存
        ConcurrentHashMap<String, CachedMessage> unavailableMap = unavailableMessagesMap.remove(queueKey);
        if (unavailableMap != null) {
            removedCount += unavailableMap.size();
            totalCachedMessages.addAndGet(-unavailableMap.size());
        }

        if (removedCount > 0) {
            log.info("Cleared {} cached message batches for topic: {}, group: {}, queueId: {}",
                removedCount, topic, group, queueId);
        }
    }

    /**
     * 清理过期的缓存消息
     */
    public void cleanupExpiredMessages() {
        int expiredCount = 0;

        // 清理可用缓存中的过期消息
        for (ConcurrentLinkedQueue<CachedMessage> queue : availableMessagesMap.values()) {
            if (queue.isEmpty()) {
                continue;
            }

            List<CachedMessage> toRequeue = new ArrayList<>();

            while (!queue.isEmpty()) {
                CachedMessage message = queue.poll();
                if (message == null) {
                    break;
                }

                if (message.isExpired(MAX_CACHE_TIME)) {
                    totalCachedMessages.decrementAndGet();
                    expiredCount++;
                } else {
                    // 未过期的消息重新放回队列
                    toRequeue.add(message);
                }
            }

            // 将未过期的消息重新放回队列
            for (CachedMessage message : toRequeue) {
                queue.offer(message);
            }
        }

        // 清理不可用缓存中的过期消息
        for (ConcurrentHashMap<String, CachedMessage> shardingKeyMap : unavailableMessagesMap.values()) {
            List<String> expiredKeys = new ArrayList<>();

            for (Map.Entry<String, CachedMessage> entry : shardingKeyMap.entrySet()) {
                if (entry.getValue().isExpired(MAX_CACHE_TIME)) {
                    expiredKeys.add(entry.getKey());
                    expiredCount++;
                }
            }

            for (String key : expiredKeys) {
                shardingKeyMap.remove(key);
                totalCachedMessages.decrementAndGet();
            }
        }

        if (expiredCount > 0) {
            log.info("Cleaned up {} expired cached message batches", expiredCount);
        }
    }

    /**
     * 获取缓存统计信息
     */
    public CacheStatistics getStatistics() {
        return new CacheStatistics(
            totalCachedMessages.get(),
            totalHits.get(),
            totalMisses.get(),
            availableMessagesMap.size() + unavailableMessagesMap.size()
        );
    }

    /**
     * 清空所有缓存
     */
    public void clear() {
        int totalCleared = totalCachedMessages.intValue();
        availableMessagesMap.clear();
        unavailableMessagesMap.clear();
        totalCachedMessages.set(0);
        log.info("Cleared all cached messages, total: {}", totalCleared);
    }

    /**
     * 缓存统计信息
     */
    public static class CacheStatistics {
        private final long totalCachedMessages;
        private final long totalHits;
        private final long totalMisses;
        private final int queueCount;

        public CacheStatistics(long totalCachedMessages, long totalHits, long totalMisses, int queueCount) {
            this.totalCachedMessages = totalCachedMessages;
            this.totalHits = totalHits;
            this.totalMisses = totalMisses;
            this.queueCount = queueCount;
        }

        public long getTotalCachedMessages() {
            return totalCachedMessages;
        }

        public long getTotalHits() {
            return totalHits;
        }

        public long getTotalMisses() {
            return totalMisses;
        }

        public int getQueueCount() {
            return queueCount;
        }

        public double getHitRatio() {
            long total = totalHits + totalMisses;
            return total > 0 ? (double) totalHits / total : 0.0;
        }

        @Override
        public String toString() {
            return String.format("CacheStatistics{cachedMessages=%d, hits=%d, misses=%d, hitRatio=%.2f%%, queues=%d}",
                totalCachedMessages, totalHits, totalMisses, getHitRatio() * 100, queueCount);
        }
    }
}