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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.GetMessageResult;

/**
 * Sharding Key缓存管理类
 * 负责管理可用的过期消息和被释放锁的消息，提供快速访问能力
 */
public class ShardingKeyCache {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    /**
     * 可用消息信息
     */
    public static class CachedMessage {
        private final String topic;
        private final String group;
        private final int queueId;
        private final String shardingKey;
        private final GetMessageResult messageResult;
        private final long createTime;

        public CachedMessage(String topic, String group, int queueId, String shardingKey,
            GetMessageResult messageResult) {
            this.topic = topic;
            this.group = group;
            this.queueId = queueId;
            this.shardingKey = shardingKey;
            this.messageResult = messageResult;
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

        public long getCreateTime() {
            return createTime;
        }

        @Override
        public String toString() {
            return "AvailableMessage{" +
                "topic='" + topic + '\'' +
                ", group='" + group + '\'' +
                ", queueId=" + queueId +
                ", shardingKey='" + shardingKey + '\'' +
                ", createTime=" + createTime +
                '}';
        }
    }

    // 可用消息队列，按 topic@group@queueId 分组
    private final ConcurrentHashMap<String, ConcurrentLinkedQueue<CachedMessage>> availableMessagesMap;
    private final ConcurrentHashMap<String, ConcurrentLinkedQueue<CachedMessage>> unavailableMessagesMap;

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
     */
    public void addAvailableMessage(String topic, String group, int queueId, String shardingKey,
        GetMessageResult messageResult) {
        if (messageResult == null || messageResult.getMessageMapedList() == null ||
            messageResult.getMessageMapedList().isEmpty()) {
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
                log.warn("Cache queue is full, removed oldest message: {}", removed);
            }
        }

        CachedMessage availableMessage = new CachedMessage(topic, group, queueId, shardingKey, messageResult);
        queue.offer(availableMessage);
        totalCachedMessages.incrementAndGet();

        log.debug("Added available message to cache: {}", availableMessage);
    }

    /**
     * 添加暂时不可用的消息到缓存
     *
     * @param topic       主题
     * @param group       消费组
     * @param queueId     队列ID
     * @param shardingKey shardingKey
     * @param offsets     消息 offset
     */
    public void addUnavailableMessage(String topic, String group, int queueId, String shardingKey, List<Long> offsets) {
        if (offsets == null || offsets.isEmpty()) {
            return;
        }
        String queueKey = MessageShardingKeyUtil.buildTopicGroupQueueIdentifier(topic, group, queueId);
        ConcurrentLinkedQueue<CachedMessage> queue = availableMessagesMap.computeIfAbsent(
            queueKey, k -> new ConcurrentLinkedQueue<>());
    }

    /**
     * 获取指定队列的可用消息
     *
     * @param topic    主题
     * @param group    消费组
     * @param queueId  队列ID
     * @param maxCount 最大返回数量
     * @return 可用消息列表
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
                log.warn("Removed expired cached message: {}", message);
                continue;
            }

            result.add(message);
            totalCachedMessages.decrementAndGet();
            count++;
        }

        if (!result.isEmpty()) {
            totalHits.incrementAndGet();
        } else {
            totalMisses.incrementAndGet();
        }

        log.debug("Retrieved {} available messages from cache for topic: {}, group: {}, queueId: {}",
            result.size(), topic, group, queueId);

        return result;
    }

    /**
     * 获取指定 shardingKey 的可用消息
     *
     * @param topic       主题
     * @param group       消费组
     * @param queueId     队列ID
     * @param shardingKey shardingKey
     * @param maxCount    最大返回数量
     * @return 可用消息列表
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
                log.warn("Removed expired cached message: {}", message);
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
        } else {
            totalMisses.incrementAndGet();
        }

        log.debug("Retrieved {} available messages for shardingKey: {} from cache for topic: {}, group: {}, queueId: {}",
            result.size(), shardingKey, topic, group, queueId);

        return result;
    }

    /**
     * 清除指定队列的所有缓存消息
     */
    public void clearQueueCache(String topic, String group, int queueId) {
        String queueKey = MessageShardingKeyUtil.buildTopicGroupQueueIdentifier(topic, group, queueId);
        ConcurrentLinkedQueue<CachedMessage> queue = availableMessagesMap.remove(queueKey);

        if (queue != null) {
            int removedCount = queue.size();
            totalCachedMessages.addAndGet(-removedCount);
            log.info("Cleared {} cached messages for topic: {}, group: {}, queueId: {}",
                removedCount, topic, group, queueId);
        }
    }

    /**
     * 清理过期的缓存消息
     */
    public void cleanupExpiredMessages() {
        int expiredCount = 0;

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

        if (expiredCount > 0) {
            log.info("Cleaned up {} expired cached messages", expiredCount);
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
            availableMessagesMap.size()
        );
    }

    /**
     * 获取指定队列的缓存大小
     */
    public int getQueueCacheSize(String topic, String group, int queueId) {
        String queueKey = MessageShardingKeyUtil.buildTopicGroupQueueIdentifier(topic, group, queueId);
        ConcurrentLinkedQueue<CachedMessage> queue = availableMessagesMap.get(queueKey);
        return queue != null ? queue.size() : 0;
    }

    /**
     * 检查指定队列是否有缓存消息
     */
    public boolean hasAvailableMessages(String topic, String group, int queueId) {
        return getQueueCacheSize(topic, group, queueId) > 0;
    }

    /**
     * 清空所有缓存
     */
    public void clear() {
        int totalCleared = totalCachedMessages.intValue();
        availableMessagesMap.clear();
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