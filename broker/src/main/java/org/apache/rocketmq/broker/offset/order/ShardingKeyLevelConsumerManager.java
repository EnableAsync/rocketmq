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

/**
 * ShardingKey 级别的顺序消费管理器
 * 实现基于 shardingKey 的并发顺序消费，提升消费吞吐量
 * <p>
 * 核心设计思路：乐观读取 + 多 shardingKey 并发
 * - 先读取消息，再根据 shardingKey 判断是否阻塞
 * - 只有相同 shardingKey 的消息才会相互阻塞
 * - 不同 shardingKey 的消息可以并发消费
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
     * 实际的阻塞逻辑在 PopMessageProcessor 中通过 GetMessageResult 进行判断
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
     * 更新消息列表的接收状态
     * 当消费者POP消息时被调用，用于记录消息状态和构建消费信息
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
            // List<Integer> availableIndices = new ArrayList<>();
            Map<String, List<Long>> availableShardingKeyOffsets = new HashMap<>();

            // 从GetMessageResult中提取sharding key信息
            for (int i = 0; i < getMessageResult.getMessageBufferList().size(); i++) {
                ByteBuffer byteBuffer = getMessageResult.getMessageBufferList().get(i);
                String shardingKey = MessageShardingKeyUtil.extractShardingKeyFromBuffer(byteBuffer);
                if (lockManager.isLocked(topic, group, queueId, shardingKey, attemptId)) {
                    unavailableIndices.add(i);
                    unavailableShardingKeyOffsets.computeIfAbsent(shardingKey, k -> new ArrayList<>()).add(msgQueueOffsetList.get(i));
                } else {
                    // availableIndices.add(i);
                    availableShardingKeyOffsets.computeIfAbsent(shardingKey, k -> new ArrayList<>()).add(msgQueueOffsetList.get(i));
                }
            }

            // 记录未消费的消息
            for (Map.Entry<String, List<Long>> entry : unavailableShardingKeyOffsets.entrySet()) {
                cache.addUnavailableMessage(topic, group, queueId, entry.getKey(), entry.getValue());
            }

            // 移除被阻塞的消息
            getMessageResult.removeIndices(unavailableIndices);

            // 创建锁
            for (Map.Entry<String, List<Long>> entry : availableShardingKeyOffsets.entrySet()) {
                lockManager.createOrUpdateLock(topic, group, queueId, entry.getKey(), popTime, invisibleTime, attemptId, entry.getValue());
            }

            log.debug("Updated sharding key locks for {} messages in topic: {}, group: {}, queueId: {}",
                msgQueueOffsetList.size(), topic, group, queueId);

        } catch (Exception e) {
            log.error("Failed to update sharding key locks for topic: {}, group: {}, queueId: {}",
                topic, group, queueId, e);
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
            boolean released = lockManager.releaseLock(topic, group, queueId, queueOffset, popTime);

            if (released) {
                log.debug("Successfully released sharding key lock for offset: {} in topic: {}, group: {}, queueId: {}",
                    queueOffset, topic, group, queueId);

                // 返回下一个偏移量
                return queueOffset + 1;
            } else {
                log.warn("Failed to release sharding key lock for offset: {} in topic: {}, group: {}, queueId: {}",
                    queueOffset, topic, group, queueId);

                // 如果释放失败，返回 -2 表示无需提交
                return -2;
            }
        } catch (Exception e) {
            log.error("Failed to commit and next for offset: {} in topic: {}, group: {}, queueId: {}",
                queueOffset, topic, group, queueId, e);
            return -1; // 返回 -1 表示非法
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
            if (lockManager != null) {
                String stats = lockManager.getStatistics();
                log.debug("ShardingKeyLockManager statistics: {}", stats);
            }

            if (cache != null) {
                ShardingKeyCache.CacheStatistics cacheStats = cache.getStatistics();
                log.debug("ShardingKeyCache statistics: {}", cacheStats);
            }
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