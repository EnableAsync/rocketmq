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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.OrderedConsumptionLevel;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.GetMessageStatus;

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
        this.cache = new ShardingKeyCache();
        this.lockManager = new ShardingKeyLockManager(brokerController, this.cache);
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
//            return cache.checkBlockAddUnavailableMessages()

            if (cache.checkBlock(topic, group, queueId)) {
                log.info("QUEUE 中的消息数量已到达上限，开始阻塞: topic={}, group={}, queueId={}", topic, group, queueId);
                return true;
            } else {
                return false;
            }
//            return false;
        } catch (Exception e) {
            log.error("Failed to check block for topic: {}, group: {}, queueId: {}", topic, group, queueId, e);
            return true; // 出错时保守阻塞
        }
    }

    @Override
    public GetMessageResult getAvailableMessageResult(String attemptId, long popTime, long invisibleTime,
        String topicId, String groupId, int queueId, int batchSize, StringBuilder orderCountInfoBuilder) {
        return popMessageFromCache(attemptId, popTime, invisibleTime, topicId, groupId, queueId, batchSize, orderCountInfoBuilder);
    }

    /**
     * 优先从缓存中获取可用消息
     * POP 请求首先调用此方法，如果有缓存消息则直接返回，避免读取存储
     */
    public GetMessageResult popMessageFromCache(String attemptId, long popTime, long invisibleTime, String topic,
        String group, int queueId, int maxCount, StringBuilder orderCountInfoBuilder) {
        try {
            ShardingKeyCache.CachedMessage cachedMessages = cache.getAvailableMessages(topic, group, queueId, maxCount);
            if (cachedMessages == null) {
                log.debug("缓存中无可用消息: topic={}, group={}, queueId={}", topic, group, queueId);
                return null;
            }

            // 合并构建GetMessageResult和创建锁的操作，避免重复遍历
            GetMessageResult result = buildGetMessageResultAndCreateLocks(attemptId, popTime, invisibleTime,
                topic, group, queueId, cachedMessages, orderCountInfoBuilder);
            if (result != null) {
                log.info("从缓存中成功获取消息: topic={}, group={}, queueId={}, 消息批次数量={}, attemptId={}, offsets={}",
                    topic, group, queueId, result.getMessageCount(), attemptId, result.getMessageQueueOffset());
            }
            return result;
        } catch (Exception e) {
            log.error("从缓存中获取消息失败: topic={}, group={}, queueId={}, attemptId={}",
                topic, group, queueId, attemptId, e);
            return null;
        }
    }

    /**
     * 从 store 中读取出多个 offset 的消息
     * TODO: 可以优化，现在是每个都去读一遍
     */
    private CompletableFuture<GetMessageResult> getMessagesAsync(String topic, String group, int queueId,
        List<Long> offsets) {
        // 为每个 offset 创建一个异步任务
        List<CompletableFuture<GetMessageResult>> futures = offsets.stream()
            .map(offset -> brokerController.getMessageStore().getMessageAsync(group, topic, queueId, offset, 1, null))
            .collect(Collectors.toList());

        // 用 allOf 等待所有任务完成
        CompletableFuture<Void> allDoneFuture = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));

        // 当所有任务都完成后，处理和合并结果
        return allDoneFuture.thenApply(v -> {
            // 因为 allDoneFuture 完成时，可以保证 futures 列表中的所有 future 也都已完成，
            // 所以这里调用 join() 不会阻塞。
            List<GetMessageResult> results = futures.stream()
                .map(CompletableFuture::join)
                .collect(Collectors.toList());

            // 合并所有 GetMessageResult
            return mergeGetMessageResults(results);
        });
    }

    /**
     * 合并多个 GetMessageResult
     *
     * @param results 多个查询的结果列表
     * @return 一个合并后的 GetMessageResult
     */
    private GetMessageResult mergeGetMessageResults(List<GetMessageResult> results) {
        if (results == null || results.isEmpty()) {
            GetMessageResult emptyResult = new GetMessageResult();
            emptyResult.setStatus(GetMessageStatus.NO_MESSAGE_IN_QUEUE);
            return emptyResult;
        }

        GetMessageResult mergedResult = new GetMessageResult();
        long minOffset = Long.MAX_VALUE;
        long maxOffset = -1;
        long nextBeginOffset = -1;
        boolean messageFound = false;

        for (GetMessageResult result : results) {
            if (result.getStatus() == GetMessageStatus.FOUND) {
                messageFound = true;

                for (int i = 0; i < result.getMessageMapedList().size(); i++) {
                    mergedResult.addMessage(result.getMessageMapedList().get(i), result.getMessageQueueOffset().get(i));
                }

                minOffset = Math.min(minOffset, result.getMinOffset());
                maxOffset = Math.max(maxOffset, result.getMaxOffset());
                nextBeginOffset = Math.max(nextBeginOffset, result.getNextBeginOffset());
            }
        }

        if (messageFound) {
            mergedResult.setStatus(GetMessageStatus.FOUND);
            mergedResult.setMinOffset(minOffset);
            mergedResult.setMaxOffset(maxOffset);
            mergedResult.setNextBeginOffset(nextBeginOffset);
        } else {
            // 如果一条消息都没找到，设置一个合适的状态
            // 这里假设如果所有请求都是 NO_MESSAGE_IN_QUEUE，最终结果也是它
            // 你可以根据业务逻辑选择更精确的状态
            mergedResult.setStatus(results.get(0).getStatus());
            mergedResult.setNextBeginOffset(results.get(results.size() - 1).getNextBeginOffset());
        }
        return mergedResult;
    }

    /**
     * 从缓存消息构建 GetMessageResult 并同时创建锁
     * 一次遍历完成两个操作，提高性能
     */
    private GetMessageResult buildGetMessageResultAndCreateLocks(String attemptId, long popTime, long invisibleTime,
        String topic, String group, int queueId, ShardingKeyCache.CachedMessage cachedMessage, StringBuilder orderInfoBuilder) {
        if (cachedMessage == null) {
            return null;
        }

        // 因为每个 CachedMessage 代表一批完整的消息，可以直接返回
        GetMessageResult result = cachedMessage.getMessageResult();
        result.setStatus(GetMessageStatus.FOUND);
        // 过期的消息，需要重新从 store 读取
        // 并且构建 orderCountInfoBuilder
        if (!cachedMessage.getOffsets().isEmpty() && result.getMessageCount() == 0) {
            log.info("缓存中存在过期消息，从store读取: topic={}, group={}, queueId={}", topic, group, queueId);
            CompletableFuture<GetMessageResult> futureResult = getMessagesAsync(topic, group, queueId, cachedMessage.getOffsets());
            try {
                // 阻塞当前线程，直到异步操作完成并返回结果
                result = futureResult.join();
                // 或者 futureResult.get()，但 join() 不抛出受检异常，更简洁
            } catch (Exception e) {
                // 处理可能发生的异常，例如 CompletionException
                log.error("过期消息取数据失败了: {}", cachedMessage.getOffsets(), e);
                return null;
            }

            // 增加重试次数
            lockManager.increaseCountInfo(topic, group, queueId, cachedMessage.getShardingKey(), cachedMessage.getOffsets());

            // 构建重试次数
            lockManager.buildCountInfo(topic, group, queueId, cachedMessage.getShardingKey(), cachedMessage.getOffsets(), orderInfoBuilder);
        }

        log.info("构建缓存消息、重试次数并创建锁: topic={}, group={}, queueId={}, 处理消息批次数量={}",
            topic, group, queueId, result.getMessageCount());

        // 给出去的消息要加锁
        lockManager.createOrUpdateLock(topic, group, queueId, cachedMessage.getShardingKey(),
            popTime, invisibleTime, attemptId, cachedMessage.getOffsets());

        return result;
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
            Map<String, List<Integer>> unavailableShardingKeyIndices = new HashMap<>();
            Map<String, List<Long>> availableShardingKeyOffsets = new HashMap<>();

            // 从 GetMessageResult 中提取 sharding key 信息并进行分流
            for (int i = 0; i < getMessageResult.getMessageBufferList().size(); i++) {
                ByteBuffer byteBuffer = getMessageResult.getMessageBufferList().get(i);
                String shardingKey = MessageShardingKeyUtil.extractShardingKeyFromBuffer(byteBuffer);
                long currentOffset = msgQueueOffsetList.get(i);

                if (lockManager.isLocked(topic, group, queueId, shardingKey, attemptId)) {
                    // 消息被锁定，加入不可用列表
                    unavailableIndices.add(i);
                    unavailableShardingKeyIndices.computeIfAbsent(shardingKey, k -> new ArrayList<>()).add(i);
                    log.info("消息被锁住: shardingKey={}, offset={}", shardingKey, currentOffset);
                } else {
                    // 消息可用，加入可用列表
                    log.info("分发出去的消息: shardingKey={}, offset={}", shardingKey, currentOffset);
                    availableShardingKeyOffsets.computeIfAbsent(shardingKey, k -> new ArrayList<>()).add(currentOffset);
                }
                lockManager.updateOffsetToShardingKeyMapping(topic, group, queueId, currentOffset, shardingKey);
            }

            // Cache 中需要存储 GetMessageResult 中的 offset、SelectMappedBufferResult
            // 将被阻塞的消息存入不可用缓存
            // 之后需要从缓存中恢复出来 GetMessageResult
            if (!unavailableIndices.isEmpty()) {
                addUnavailableMessages(topic, group, queueId, unavailableShardingKeyIndices,
                    getMessageResult);
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
    private void addUnavailableMessages(String topic, String group, int queueId,
        Map<String, List<Integer>> unavailableShardingKeyIndices,
        GetMessageResult getMessageResult) {
        try {
            for (Map.Entry<String, List<Integer>> entry : unavailableShardingKeyIndices.entrySet()) {
                String shardingKey = entry.getKey();
                List<Integer> indices = entry.getValue();

                List<Long> offsets = new ArrayList<>();
                for (Integer index : indices) {
                    if (index < getMessageResult.getMessageQueueOffset().size()) {
                        offsets.add(getMessageResult.getMessageQueueOffset().get(index));
                    }
                }

                if (!offsets.isEmpty()) {
                    GetMessageResult extractedResult = extractGetMessagesResultForShardingKey(getMessageResult, indices, shardingKey, offsets);
                    if (extractedResult != null) {
                        cache.addUnavailableMessage(topic, group, queueId, shardingKey, extractedResult, offsets);
                    }
                }
            }
        } catch (Exception e) {
            log.error("添加不可用消息到缓存失败: topic={}, group={}, queueId={}", topic, group, queueId, e);
        }
    }

    /**
     * 从完整的 GetMessageResult 中提取特定 shardingKey 的消息
     */
    private GetMessageResult extractGetMessagesResultForShardingKey(GetMessageResult originalResult,
        List<Integer> indices, String shardingKey, List<Long> offsets) {
        if (originalResult == null) {
            return null;
        }

        try {
            GetMessageResult result = new GetMessageResult();
            result.setMaxOffset(originalResult.getMaxOffset());
            result.setMinOffset(originalResult.getMinOffset());
            result.setNextBeginOffset(originalResult.getNextBeginOffset());
            for (Integer index : indices) {
                result.addMessage(originalResult.getMessageMapedList().get(index), originalResult.getMessageQueueOffset().get(index));
            }

            return result;
        } catch (Exception e) {
            log.warn("Failed to create shardingKey message result", e);
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
            boolean isAcked = lockManager.releaseLock(topic, group, queueId, queueOffset, popTime);

            if (isAcked) {
                // ack 成功才提位点
                // 在 releaseLock 中已经删除了当前要确认的 ack
                // 于是这里返回当前未被 ack 的最小 offset
                return lockManager.getMinInFlightOffset(topic, group, queueId, queueOffset);
            } else {
                return -1;
            }
        } catch (Exception e) {
            log.error("消息确认失败: offset={}, topic={}, group={}, queueId={}",
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
            log.info("更新不可见时间 offset: {} in topic: {}, group: {}, queueId: {}, nextVisibleTime: {}",
                queueOffset, topic, group, queueId, nextVisibleTime);

            lockManager.updateNextVisibleTime(topic, group, queueId, queueOffset, popTime, nextVisibleTime);
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

//            // 启动定时清理任务
//            cleanupExecutor = Executors.newSingleThreadScheduledExecutor(
//                new ThreadFactoryImpl("ShardingKeyLevelConsumerManager_Cleanup_"));
//
//            // 每5分钟清理一次过期的 attemptId
//            cleanupExecutor.scheduleAtFixedRate(() -> {
//                try {
//                    if (lockManager != null) {
//                        lockManager.cleanExpiredAttemptIds();
//                    }
//                } catch (Exception e) {
//                    log.error("Failed to clean expired attempt IDs", e);
//                }
//            }, 5, 5, TimeUnit.MINUTES);

            // 每10分钟清理一次过期的缓存消息
//            cleanupExecutor.scheduleAtFixedRate(() -> {
//                try {
//                    if (cache != null) {
//                        cache.cleanupExpiredMessages();
//                    }
//                } catch (Exception e) {
//                    log.error("Failed to clean expired cache messages", e);
//                }
//            }, 10, 10, TimeUnit.MINUTES);

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

            // 记录重试次数存储统计信息
            String retryStorageStats = lockManager.getRetryStorageStatistics();
            log.debug("ShardingKeyRetryStorage statistics: {}", retryStorageStats);
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
            // 启动锁管理器（包括重试次数持久化存储）
            if (lockManager != null) {
                lockManager.load();
            }
            log.info("ShardingKeyLevelConsumerManager loaded successfully (simplified implementation)");
            return true;
        } catch (Exception e) {
            log.error("Failed to load ShardingKeyLevelConsumerManager state", e);
            return false;
        }
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