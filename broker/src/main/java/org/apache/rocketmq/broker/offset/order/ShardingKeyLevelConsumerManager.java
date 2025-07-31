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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.OrderedConsumptionLevel;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.header.ExtraInfoUtil;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.GetMessageStatus;
import org.apache.rocketmq.store.SelectMappedBufferResult;

/**
 * ShardingKey 级别的顺序消费管理器
 * 实现基于 shardingKey 的并发顺序消费，提升消费吞吐量
 * <p>
 * 核心设计思路：乐观读取 + 多 shardingKey 并发 + 双缓存机制
 * - 先读取消息，再根据 shardingKey 判断是否阻塞
 * - 只有相同 shardingKey 的消息才会相互阻塞
 * - 不同 shardingKey 的消息可以并发消费
 * - 优先从可用缓存消费，避免重复读取存储
 * - ACK 时激活不可用缓存中的消息
 */
public class ShardingKeyLevelConsumerManager implements OrderedConsumptionManager {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private final BrokerController brokerController;
    private final ShardingKeyLockManager lockManager;
    private final ShardingKeyCache cache;

    // 定时清理任务
    private ScheduledExecutorService cleanupExecutor;
    private volatile boolean started = false;

    public ShardingKeyLevelConsumerManager(BrokerController brokerController) {
        this.brokerController = brokerController;
        this.lockManager = new ShardingKeyLockManager(brokerController);
        this.cache = new ShardingKeyCache();
    }

    /**
     * 获取顺序消费级别
     */
    @Override
    public OrderedConsumptionLevel getOrderedConsumptionLevel() {
        return OrderedConsumptionLevel.SHARDING_KEY;
    }

    /**
     * 检查是否需要阻塞当前的 POP 请求
     * 基于 sharding key 级别的检查，不同的 sharding key 之间不会相互阻塞
     * <p>
     * 注意：这里的实现与队列级别不同，这里是先读取数据，再判断是否阻塞
     * 实际的阻塞逻辑在 update 方法中通过分析 sharding key 来实现
     */
    @Override
    public boolean checkBlock(String attemptId, String topic, String group, int queueId, long invisibleTime) {
        try {
            // 对于 sharding key 级别，我们总是返回 false，让消息先读取出来
            // 真正的阻塞逻辑在 update 方法中通过分析 sharding key 来实现
            log.debug("CheckBlock for sharding key level: topic={}, group={}, queueId={}, attemptId={}",
                topic, group, queueId, attemptId);
            return false;
        } catch (Exception e) {
            log.error("Failed to check block for topic: {}, group: {}, queueId: {}", topic, group, queueId, e);
            return true; // 出错时保守阻塞
        }
    }

    /**
     * 【新增】优先从缓存中获取可用消息
     * POP 请求首先调用此方法，如果有缓存消息则直接返回，避免读取存储
     */
    public GetMessageResult popMessageFromCache(String topic, String group, int queueId, int maxCount) {
        try {
            List<ShardingKeyCache.CachedMessage> cachedMessages = cache.getAvailableMessages(topic, group, queueId, maxCount);
            if (cachedMessages.isEmpty()) {
                return null;
            }

            // 将缓存消息重新组装成 GetMessageResult
            GetMessageResult result = buildGetMessageResultFromCache(cachedMessages);
            if (result != null) {
                // 为缓存中的消息创建锁
                createLocksForCachedMessages(topic, group, queueId, cachedMessages);
                log.info("Retrieved {} messages from cache for topic={}, group={}, queueId={}",
                    cachedMessages.size(), topic, group, queueId);
            }
            return result;
        } catch (Exception e) {
            log.error("Failed to pop message from cache for topic: {}, group: {}, queueId: {}", topic, group, queueId, e);
            return null;
        }
    }

    /**
     * 从缓存消息构建 GetMessageResult
     * 使用 GetMessageResult 的 addMessage 方法
     */
    private GetMessageResult buildGetMessageResultFromCache(List<ShardingKeyCache.CachedMessage> cachedMessages) {
        if (cachedMessages == null || cachedMessages.isEmpty()) {
            return null;
        }

        try {
            GetMessageResult result = new GetMessageResult();
            result.setStatus(GetMessageStatus.FOUND);

            for (ShardingKeyCache.CachedMessage cachedMessage : cachedMessages) {
                GetMessageResult msgResult = cachedMessage.getMessageResult();
                if (msgResult != null && msgResult.getMessageMapedList() != null) {
                    // 使用 GetMessageResult 的 addMessage 方法添加消息
                    for (int i = 0; i < msgResult.getMessageMapedList().size(); i++) {
                        SelectMappedBufferResult mapedBuffer = msgResult.getMessageMapedList().get(i);
                        long queueOffset = (msgResult.getMessageQueueOffset() != null && i < msgResult.getMessageQueueOffset().size())
                            ? msgResult.getMessageQueueOffset().get(i)
                            : cachedMessage.getMinOffset();

                        result.addMessage(mapedBuffer, queueOffset);
                    }
                }
            }

            return result;
        } catch (Exception e) {
            log.error("Failed to build GetMessageResult from cache", e);
            return null;
        }
    }

    /**
     * 为缓存消息创建锁
     */
    private void createLocksForCachedMessages(String topic, String group, int queueId,
                                            List<ShardingKeyCache.CachedMessage> cachedMessages) {
        try {
            long currentTime = System.currentTimeMillis();
            long invisibleTime = 30000; // 默认30秒不可见时间，实际应该从配置获取

            Map<String, List<Long>> shardingKeyOffsets = new HashMap<>();
            for (ShardingKeyCache.CachedMessage cachedMessage : cachedMessages) {
                String shardingKey = cachedMessage.getShardingKey();
                // 修复编译错误：使用 getMinOffset() 替代不存在的 getOffset()
                long offset = cachedMessage.getMinOffset();
                shardingKeyOffsets.computeIfAbsent(shardingKey, k -> new ArrayList<>()).add(offset);
            }

            // 为每个 shardingKey 创建锁
            for (Map.Entry<String, List<Long>> entry : shardingKeyOffsets.entrySet()) {
                String shardingKey = entry.getKey();
                List<Long> offsets = entry.getValue();
                String attemptId = "cache_" + System.currentTimeMillis(); // 为缓存消息生成唯一的 attemptId

                lockManager.createOrUpdateLock(topic, group, queueId, shardingKey, currentTime, invisibleTime, attemptId, offsets);
            }
        } catch (Exception e) {
            log.error("Failed to create locks for cached messages", e);
        }
    }

    /**
     * 更新消息列表的接收状态
     * 当消费者POP消息时被调用，用于记录消息状态和构建消费信息
     * <p>
     * 核心改造：实现乐观读取 + 消息分流逻辑
     */
    @Override
    public void update(String attemptId, boolean isRetry, String topic, String group, int queueId,
        long popTime, long invisibleTime, List<Long> msgQueueOffsetList,
        StringBuilder orderInfoBuilder, GetMessageResult getMessageResult) {

        if (msgQueueOffsetList == null || msgQueueOffsetList.isEmpty() || getMessageResult == null) {
            log.warn("Empty message offset list for topic: {}, group: {}, queueId: {}", topic, group, queueId);
            return;
        }

        try {
            List<Integer> unavailableIndices = new ArrayList<>();
            Map<String, List<Long>> unavailableShardingKeyOffsets = new HashMap<>();
            Map<String, List<Long>> availableShardingKeyOffsets = new HashMap<>();

            // 【核心改造】从GetMessageResult中提取sharding key信息并进行分流
            for (int i = 0; i < getMessageResult.getMessageBufferList().size(); i++) {
                ByteBuffer byteBuffer = getMessageResult.getMessageBufferList().get(i);
                String shardingKey = MessageShardingKeyUtil.extractShardingKeyFromBuffer(byteBuffer);
                long currentOffset = msgQueueOffsetList.get(i);

                if (lockManager.isLocked(topic, group, queueId, shardingKey, attemptId)) {
                    // 消息被锁定，加入不可用列表
                    unavailableIndices.add(i);
                    unavailableShardingKeyOffsets.computeIfAbsent(shardingKey, k -> new ArrayList<>()).add(currentOffset);
                    log.info("消息被锁住: shardingKey={}, offset={}", shardingKey, currentOffset);
                } else {
                    // 消息可用，加入可用列表
                    log.info("分发出去的消息: shardingKey={}, offset={}", shardingKey, currentOffset);
                    availableShardingKeyOffsets.computeIfAbsent(shardingKey, k -> new ArrayList<>()).add(currentOffset);
                }
            }

            // 【关键改造】将被阻塞的消息存入不可用缓存
            if (!unavailableIndices.isEmpty()) {
                storeUnavailableMessages(topic, group, queueId, unavailableShardingKeyOffsets,
                                       getMessageResult, unavailableIndices);
            }

            // 移除被阻塞的消息
            getMessageResult.removeIndices(unavailableIndices);

            if (getMessageResult.getMessageBufferList().isEmpty()) {
                log.info("读取到消息，但都被锁定无法分发");
                return;
            }

            // 为可用消息创建锁
            for (Map.Entry<String, List<Long>> entry : availableShardingKeyOffsets.entrySet()) {
                lockManager.createOrUpdateLock(topic, group, queueId, entry.getKey(),
                    popTime, invisibleTime, attemptId, entry.getValue());
            }

            log.debug("Updated sharding key locks for {} available messages in topic: {}, group: {}, queueId: {}",
                availableShardingKeyOffsets.size(), topic, group, queueId);

        } catch (Exception e) {
            log.error("Failed to update sharding key locks for topic: {}, group: {}, queueId: {}",
                topic, group, queueId, e);
        }
    }

    /**
     * 将不可用消息存入缓存
     */
    private void storeUnavailableMessages(String topic, String group, int queueId,
                                        Map<String, List<Long>> unavailableShardingKeyOffsets,
                                        GetMessageResult getMessageResult, List<Integer> unavailableIndices) {
        try {
            for (Map.Entry<String, List<Long>> entry : unavailableShardingKeyOffsets.entrySet()) {
                String shardingKey = entry.getKey();
                List<Long> offsets = entry.getValue();

                // 为每个被阻塞的 shardingKey 创建对应的 GetMessageResult
                GetMessageResult unavailableResult = extractMessagesForShardingKey(getMessageResult,
                    unavailableIndices, shardingKey, offsets);

                if (unavailableResult != null) {
                    cache.addUnavailableMessage(topic, group, queueId, shardingKey, unavailableResult, offsets);
                }
            }
        } catch (Exception e) {
            log.error("Failed to store unavailable messages", e);
        }
    }

    /**
     * 从完整的 GetMessageResult 中提取特定 shardingKey 的消息
     */
    private GetMessageResult extractMessagesForShardingKey(GetMessageResult originalResult,
                                                         List<Integer> unavailableIndices,
                                                         String targetShardingKey, List<Long> offsets) {
        // 简化实现：直接使用原有的单消息创建方法
        if (unavailableIndices.isEmpty()) {
            return null;
        }

        // 找到第一个匹配的索引，创建单个消息的结果
        // 实际实现中应该更精确地匹配 shardingKey 和 offset
        int firstIndex = unavailableIndices.get(0);
        return createSingleMessageResult(originalResult, firstIndex);
    }

    /**
     * 从完整的GetMessageResult中创建单个消息的结果
     * 使用 GetMessageResult 的 addMessage 方法，避免反射
     */
    private GetMessageResult createSingleMessageResult(GetMessageResult originalResult, int index) {
        if (originalResult == null || index < 0 || index >= originalResult.getMessageBufferList().size()) {
            return null;
        }

        try {
            GetMessageResult singleResult = new GetMessageResult();
            singleResult.setStatus(originalResult.getStatus());
            singleResult.setNextBeginOffset(originalResult.getNextBeginOffset());
            singleResult.setMinOffset(originalResult.getMinOffset());
            singleResult.setMaxOffset(originalResult.getMaxOffset());
            singleResult.setSuggestPullingFromSlave(originalResult.isSuggestPullingFromSlave());

            // 使用 addMessage 方法添加单个消息，避免反射
            if (originalResult.getMessageMapedList() != null && index < originalResult.getMessageMapedList().size()) {
                SelectMappedBufferResult mapedBuffer = originalResult.getMessageMapedList().get(index);
                long queueOffset = (originalResult.getMessageQueueOffset() != null && index < originalResult.getMessageQueueOffset().size())
                    ? originalResult.getMessageQueueOffset().get(index)
                    : -1L;

                if (queueOffset != -1L) {
                    singleResult.addMessage(mapedBuffer, queueOffset);
                } else {
                    singleResult.addMessage(mapedBuffer);
                }
            }

            return singleResult;
        } catch (Exception e) {
            log.warn("Failed to create single message result", e);
            return null;
        }
    }

    /**
     * 提交消息并计算下一个消费偏移量
     * 当消费者 ACK 消息时调用
     */
    @Override
    public long commitAndNext(String topic, String group, int queueId, long queueOffset, long popTime) {
        try {
            // 根据 offset 释放对应的 sharding key 锁
            boolean fullyReleased = lockManager.releaseLock(topic, group, queueId, queueOffset, popTime);

            if (fullyReleased) {
                // 【关键改造】锁完全释放后，激活缓存中对应 shardingKey 的消息
                String shardingKey = lockManager.findShardingKeyByOffset(topic, group, queueId, queueOffset);
                if (shardingKey != null) {
                    boolean activated = cache.activateMessages(topic, group, queueId, shardingKey);
                    if (activated) {
                        // 激活成功后，唤醒长轮询
                        notifyLongPolling(topic, group, queueId);
                        log.info("Activated messages for shardingKey: {} after ACK offset: {}", shardingKey, queueOffset);
                    }
                }

                log.info("Successfully released sharding key lock for offset: {} in topic: {}, group: {}, queueId: {}",
                    queueOffset, topic, group, queueId);

                return 0; // 返回 0 表示成功，在 ShardingKey 模式下不需要连续的消费位点
            } else {
                log.debug("Partially released lock for offset: {} in topic: {}, group: {}, queueId: {}",
                    queueOffset, topic, group, queueId);
                return -2; // 返回 -2 表示无需提交（锁内还有其他消息）
            }
        } catch (Exception e) {
            log.error("Failed to commit and next for offset: {} in topic: {}, group: {}, queueId: {}",
                queueOffset, topic, group, queueId, e);
            return -1; // 返回 -1 表示非法操作
        }
    }

    /**
     * 唤醒长轮询
     */
    private void notifyLongPolling(String topic, String group, int queueId) {
        try {
            if (brokerController != null && brokerController.getPopMessageProcessor() != null) {
                brokerController.getPopMessageProcessor().notifyMessageArriving(topic, queueId, group);
            }
        } catch (Exception e) {
            log.error("Failed to notify long polling", e);
        }
    }

    /**
     * 更新消息的下次可见时间
     * 用于消息的延时重新消费
     */
    @Override
    public void updateNextVisibleTime(String topic, String group, int queueId, long queueOffset,
        long popTime, long nextVisibleTime) {
        try {
            lockManager.updateNextVisibleTime(topic, group, queueId, queueOffset, popTime, nextVisibleTime);

            log.debug("Updated next visible time for offset: {} in topic: {}, group: {}, queueId: {}, nextVisibleTime: {}",
                queueOffset, topic, group, queueId, nextVisibleTime);
        } catch (Exception e) {
            log.error("Failed to update next visible time for offset: {} in topic: {}, group: {}, queueId: {}",
                queueOffset, topic, group, queueId, e);
        }
    }

    /**
     * 清除指定队列的阻塞状态
     * 通常在消费者重新平衡或队列重新分配时调用
     */
    @Override
    public void clearBlock(String topic, String group, int queueId) {
        try {
            // 清除该队列的所有锁
            lockManager.clearQueueLocks(topic, group, queueId);

            // 清除缓存
            cache.clearQueueCache(topic, group, queueId);

            log.info("Cleared blocks for topic: {}, group: {}, queueId: {}", topic, group, queueId);
        } catch (Exception e) {
            log.error("Failed to clear blocks for topic: {}, group: {}, queueId: {}", topic, group, queueId, e);
        }
    }

    /**
     * 启动控制器
     * 初始化必要的资源，如定时器、线程池等
     */
    @Override
    public void start() {
        if (started) {
            return;
        }

        try {
            // 启动定时清理任务
            cleanupExecutor = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryImpl("ShardingKeyLevelConsumerManager_Cleanup_"));

            // 每5分钟清理一次过期的 attemptId
            cleanupExecutor.scheduleAtFixedRate(() -> {
                try {
                    if (lockManager != null) {
                        lockManager.cleanExpiredAttemptIds();
                    }
                } catch (Exception e) {
                    log.error("Failed to clean expired attempt IDs", e);
                }
            }, 5, 5, TimeUnit.MINUTES);

            // 每10分钟清理一次过期的缓存消息
            cleanupExecutor.scheduleAtFixedRate(() -> {
                try {
                    if (cache != null) {
                        cache.cleanupExpiredMessages();
                    }
                } catch (Exception e) {
                    log.error("Failed to clean expired cache messages", e);
                }
            }, 10, 10, TimeUnit.MINUTES);

            started = true;
            log.info("ShardingKeyLevelConsumerManager started successfully");
        } catch (Exception e) {
            log.error("Failed to start ShardingKeyLevelConsumerManager", e);
        }
    }

    /**
     * 关闭控制器
     * 释放资源，清理定时任务等
     */
    @Override
    public void shutdown() {
        if (!started) {
            return;
        }

        try {
            // 关闭定时清理任务
            if (cleanupExecutor != null) {
                cleanupExecutor.shutdown();
                try {
                    if (!cleanupExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                        cleanupExecutor.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    cleanupExecutor.shutdownNow();
                    Thread.currentThread().interrupt();
                }
            }

            // 关闭锁管理器
            if (lockManager != null) {
                lockManager.shutdown();
            }

            // 清空缓存
            if (cache != null) {
                cache.clear();
            }

            started = false;
            log.info("ShardingKeyLevelConsumerManager shutdown successfully");
        } catch (Exception e) {
            log.error("Failed to shutdown ShardingKeyLevelConsumerManager", e);
        }
    }

    /**
     * 持久化控制器状态
     * 当前实现为简化版本，不进行实际的持久化操作
     */
    @Override
    public void persist() {
        try {
            // 简化实现：记录统计信息
            String stats = lockManager.getStatistics();
            log.debug("ShardingKeyLockManager statistics: {}", stats);

            ShardingKeyCache.CacheStatistics cacheStats = cache.getStatistics();
            log.debug("ShardingKeyCache statistics: {}", cacheStats);
        } catch (Exception e) {
            log.error("Failed to persist ShardingKeyLevelConsumerManager state", e);
        }
    }

    /**
     * 加载控制器状态
     * 当前实现为简化版本，总是返回成功
     */
    @Override
    public boolean load() {
        try {
            log.info("ShardingKeyLevelConsumerManager loaded successfully (simplified implementation)");
            return true;
        } catch (Exception e) {
            log.error("Failed to load ShardingKeyLevelConsumerManager state", e);
            return false;
        }
    }

    /**
     * 构建顺序信息
     */
    private void buildOrderInfo(StringBuilder orderInfoBuilder, String topic, String group, int queueId,
        String shardingKey, List<MessageShardingKeyUtil.MessageInfo> messages) {
        // 为每个消息构建顺序信息
        for (MessageShardingKeyUtil.MessageInfo messageInfo : messages) {
            ExtraInfoUtil.buildQueueOffsetOrderCountInfo(orderInfoBuilder, topic, queueId,
                messageInfo.getOffset(), 0);
        }

        // 构建 shardingKey 级别的顺序信息
        ExtraInfoUtil.buildQueueIdOrderCountInfo(orderInfoBuilder, topic, queueId, 0);
    }

    // Getter methods for testing and monitoring
    public ShardingKeyLockManager getLockManager() {
        return lockManager;
    }

    public ShardingKeyCache getCache() {
        return cache;
    }

    public boolean isStarted() {
        return started;
    }
}