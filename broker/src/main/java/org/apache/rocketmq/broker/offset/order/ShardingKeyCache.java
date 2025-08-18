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
 * <p>
 * 核心设计：双缓存机制
 * - 可用消息缓存 (Available Cache)：存放因为锁被释放（通过 ACK 或超时）而被"激活"的消息
 * - 不可用消息缓存 (Unavailable Cache)：存放那些被乐观读取出来，但因 ShardingKey 被锁定而无法立即分发的消息
 */
public class ShardingKeyCache {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    public static final int ShardingKeyMaxMessageCount = 20;

    // 可用消息队列，按 topic@group@queueId 分组，里面的都是可用的，不用区分 shardingKey 了
    // 分发出去的时候，根据 shardingKey 加锁就好
    private final ConcurrentHashMap<String, ConcurrentLinkedQueue<CachedMessage>> availableMessagesMap;

    // 不可用消息缓存：QueueKey -> ShardingKey -> 消息
    private final ConcurrentHashMap<String/*queueKey*/, ConcurrentHashMap<String/*shardingKey*/, CachedMessage>> unavailableMessagesMap;

    // 缓存统计信息
    private final AtomicLong totalCachedBodyMessages;
    private final AtomicLong totalHits;
    private final AtomicLong totalMisses;

    // 缓存配置
    private static final long MAX_CACHE_TIME = 30 * 1000; // 30秒最大缓存时间
    private static final int MAX_QUEUE_SIZE = 200; // 单个队列最大缓存消息数

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
            this.offsets = new ArrayList<>(offsets);
            this.createTime = System.currentTimeMillis();
        }

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
            return offsets;
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

    public ShardingKeyCache() {
        this.availableMessagesMap = new ConcurrentHashMap<>();
        this.unavailableMessagesMap = new ConcurrentHashMap<>();
        this.totalCachedBodyMessages = new AtomicLong(0);
        this.totalHits = new AtomicLong(0);
        this.totalMisses = new AtomicLong(0);
    }

    /**
     * 添加可用消息到缓存
     * ack 的时候添加有消息体的消息
     * 过期的时候添加没有消息体的消息
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
        if (offsets == null || offsets.isEmpty()) {
            return;
        }

        String queueKey = MessageShardingKeyUtil.buildTopicGroupQueueIdentifier(topic, group, queueId);
        ConcurrentLinkedQueue<CachedMessage> queue = availableMessagesMap.computeIfAbsent(
            queueKey, k -> new ConcurrentLinkedQueue<>());

        CachedMessage availableMessage = new CachedMessage(topic, group, queueId, shardingKey, messageResult, offsets);
        queue.offer(availableMessage);
//        totalCachedBodyMessages.addAndGet(messageResult.getMessageCount());

        log.info("添加可用消息批次到缓存: topic={}, group={}, queueId={}, shardingKey={}, 有内容的消息数量={}, offset={}",
            topic, group, queueId, shardingKey, messageResult.getMessageCount(), offsets);
    }

    public boolean checkBlock(String topic, String group, int queueId) {
        // TODO: 需要改成 MAX_QUEUE_SIZE * QUEUE_NUM
        // 当前只有一个 queue
        return totalCachedBodyMessages.get() > MAX_QUEUE_SIZE;
    }

    /**
     * 添加暂时不可用的消息到缓存
     *
     * @param topic         主题
     * @param group         消费组
     * @param queueId       队列ID
     * @param shardingKey   shardingKey
     * @param messageResult 消息结果
     * @param offsets       消息offset列表
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
        shardingKeyMap.compute(shardingKey, (k, v) -> {
            if (v == null) {
                return new CachedMessage(topic, group, queueId, shardingKey, messageResult, offsets);
            } else {
                for (int i = 0; i < offsets.size(); i++) {
                    // TODO: 一直加可能会超
                    v.getMessageResult().addMessage(messageResult.getMessageMapedList().get(i), offsets.get(i));
                    v.getMessageResult().setMaxOffset(messageResult.getMaxOffset());
                    v.getMessageResult().setMinOffset(messageResult.getMinOffset());
                    v.getMessageResult().setNextBeginOffset(messageResult.getNextBeginOffset());
                    v.getOffsets().addAll(offsets);
                }
                return v;
            }
        });
        totalCachedBodyMessages.addAndGet(offsets.size());

        log.info("添加不可用消息批次到缓存: topic={}, group={}, queueId={}, shardingKey={}, 消息数量={}",
            topic, group, queueId, shardingKey, offsets.size());

        log.info("当前不可用消息缓存状态为: {}", unavailableMessagesMap);
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
            log.info("未找到 queue 级别不可用消息缓存: topic={}, group={}, queueId={}", topic, group, queueId);
            return false;
        }

        CachedMessage cachedMessage = shardingKeyMap.remove(shardingKey);
        if (cachedMessage == null) {
            log.info("未找到 shardingKey 级别不可用消息缓存: topic={}, group={}, queueId={}, shardingKey={}", topic, group, queueId, shardingKey);
            return false;
        }

        // 将整个消息批次移动到可用队列
        availableMessagesMap.computeIfAbsent(queueKey, k -> new ConcurrentLinkedQueue<>()).offer(cachedMessage);

        log.info("激活消息批次成功: topic={}, group={}, queueId={}, shardingKey={}, 激活消息数量={}, 缓存状态为={}",
            topic, group, queueId, shardingKey, cachedMessage.getOffsets().size(), availableMessagesMap);
        totalCachedBodyMessages.addAndGet(-cachedMessage.getOffsets().size());
        return true;
    }

    /**
     * 获取指定队列的可用消息（优先从可用缓存获取）
     */
    public CachedMessage getAvailableMessages(String topic, String group, int queueId, int maxCount) {
        log.info("shardingKeyCache 从缓存中获取可用消息: topic={}, group={}, queueId={}, 最大获取消息批次数量={}", topic, group, queueId, maxCount);
        String queueKey = MessageShardingKeyUtil.buildTopicGroupQueueIdentifier(topic, group, queueId);
        ConcurrentLinkedQueue<CachedMessage> queue = availableMessagesMap.get(queueKey);

        if (queue == null || queue.isEmpty()) {
            log.info("可用消息缓存的 queue 为空: topic={}, group={}, queueId={}", topic, group, queueId);
            totalMisses.incrementAndGet();
            return null;
        }

        CachedMessage result = queue.poll();

        if (result != null) {
            totalHits.incrementAndGet();
            log.info("从缓存中获取可用消息成功: topic={}, group={}, queueId={}, 获取消息批次数量={}",
                topic, group, queueId, result.getOffsets().size());
//            totalCachedBodyMessages.addAndGet(-result.getOffsets().size());
        } else {
            totalMisses.incrementAndGet();
            log.debug("从缓存中未找到可用消息: topic={}, group={}, queueId={}", topic, group, queueId);
        }

        return result; // 先只加一条
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
            totalCachedBodyMessages.addAndGet(-availableQueue.size());
        }

        // 清除不可用缓存
        ConcurrentHashMap<String, CachedMessage> unavailableMap = unavailableMessagesMap.remove(queueKey);
        if (unavailableMap != null) {
            removedCount += unavailableMap.size();
            totalCachedBodyMessages.addAndGet(-unavailableMap.size());
        }

        if (removedCount > 0) {
            log.info("Cleared {} cached message batches for topic: {}, group: {}, queueId: {}",
                removedCount, topic, group, queueId);
        }
    }

    /**
     * 获取缓存统计信息
     */
    public CacheStatistics getStatistics() {
        return new CacheStatistics(
            totalCachedBodyMessages.get(),
            totalHits.get(),
            totalMisses.get(),
            availableMessagesMap.size() + unavailableMessagesMap.size()
        );
    }

    /**
     * 清空所有缓存
     */
    public void clear() {
        int totalCleared = totalCachedBodyMessages.intValue();
        availableMessagesMap.clear();
        unavailableMessagesMap.clear();
        totalCachedBodyMessages.set(0);
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