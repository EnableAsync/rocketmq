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
package org.apache.rocketmq.broker.pop;

import com.alibaba.fastjson.JSON;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.KeyBuilder;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.constant.ConsumeInitMode;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.utils.ConcurrentHashMapUtils;
import org.apache.rocketmq.remoting.protocol.header.ExtraInfoUtil;
import org.apache.rocketmq.store.AppendMessageStatus;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.GetMessageStatus;
import org.apache.rocketmq.store.MessageFilter;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.exception.ConsumeQueueException;
import org.apache.rocketmq.store.pop.PopCheckPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pop消费者服务
 * <p>
 * 这是RocketMQ Pop模式的核心服务，负责：
 * 1. 处理Pop消息请求的完整生命周期管理
 * 2. 管理消息的不可见时间（invisibility timeout）
 * 3. 处理消息的自动重试和恢复机制
 * 4. 维护Pop消息的状态存储（缓存+持久化）
 * 5. 支持顺序消息的阻塞机制
 * <p>
 * 设计目标：
 * - 提供比传统Pull模式更好的消费体验
 * - 避免消息重复消费（通过不可见机制）
 * - 确保消息最终一致性（通过重试机制）
 * - 支持高并发场景下的消费者负载均衡
 */
public class PopConsumerService extends ServiceThread {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_POP_LOGGER_NAME);

    /**
     * 偏移量不存在的标识
     */
    private static final long OFFSET_NOT_EXIST = -1L;
    /**
     * RocksDB存储目录名
     */
    private static final String ROCKSDB_DIRECTORY = "kvStore";

    /**
     * 重试间隔数组（单位：秒）- 使用指数退避策略避免消息堆积
     */
    private static final int[] REWRITE_INTERVALS_IN_SECONDS =
        new int[] {10, 30, 60, 120, 180, 240, 300, 360, 420, 480, 540, 600, 1200, 1800, 3600, 7200};

    /**
     * 消费者运行状态标识 - 用于优雅关闭时等待写操作完成
     */
    private final AtomicBoolean consumerRunning;
    /**
     * Broker配置
     */
    private final BrokerConfig brokerConfig;
    /**
     * Broker控制器
     */
    private final BrokerController brokerController;
    /**
     * 当前扫描时间 - 用于控制恢复任务的扫描进度
     */
    private final AtomicLong currentTime;
    /**
     * 上次清理锁的时间 - 避免频繁清理过期锁
     */
    private final AtomicLong lastCleanupLockTime;
    /**
     * Pop消费者缓存 - 提高热点数据访问性能
     */
    private final PopConsumerCache popConsumerCache;
    /**
     * Pop消费者持久化存储 - 确保数据不丢失
     */
    private final PopConsumerKVStore popConsumerStore;
    /**
     * 消费者锁服务 - 防止同一消费组的并发操作冲突
     */
    private final PopConsumerLockService consumerLockService;
    /**
     * 请求计数表 - 用于负载均衡和重试策略
     */
    private final ConcurrentMap<String /* groupId@topicId*/, AtomicLong> requestCountTable;

    /**
     * 构造函数
     *
     * @param brokerController Broker控制器
     */
    public PopConsumerService(BrokerController brokerController) {
        this.brokerController = brokerController;
        this.brokerConfig = brokerController.getBrokerConfig();
        this.consumerRunning = new AtomicBoolean(false);
        this.requestCountTable = new ConcurrentHashMap<>();
        // 初始时间设置为3秒，避免启动时立即扫描大量过期数据
        this.currentTime = new AtomicLong(TimeUnit.SECONDS.toMillis(3));
        this.lastCleanupLockTime = new AtomicLong(System.currentTimeMillis());
        // 锁超时时间设置为2分钟，平衡性能和资源占用
        this.consumerLockService = new PopConsumerLockService(TimeUnit.MINUTES.toMillis(2));
        // 使用RocksDB作为持久化存储，支持高性能的KV操作
        this.popConsumerStore = new PopConsumerRocksdbStore(Paths.get(
            brokerController.getMessageStoreConfig().getStorePathRootDir(), ROCKSDB_DIRECTORY).toString());
        // 只有启用缓冲区合并时才创建缓存，节省内存
        this.popConsumerCache = brokerConfig.isEnablePopBufferMerge() ? new PopConsumerCache(
            brokerController, this.popConsumerStore, this.consumerLockService, this::revive) : null;

        log.info("PopConsumerService init, buffer={}, rocksdb filePath={}",
            brokerConfig.isEnablePopBufferMerge(), this.popConsumerStore.getFilePath());
    }

    /**
     * 判断是否应该停止Pop操作
     * <p>
     * 为什么需要这个机制：
     * 1. 防止消费者消费能力不足导致的消息堆积
     * 2. 避免内存中accumulating过多未确认消息
     * 3. 提供流控保护，确保系统稳定性
     * <p>
     * In-flight messages are those that have been received from a queue
     * by a consumer but have not yet been deleted. For standard queues,
     * there is a limit on the number of in-flight messages, depending on queue traffic and message backlog.
     */
    public boolean isPopShouldStop(String group, String topic, int queueId) {
        return brokerConfig.isEnablePopMessageThreshold() && popConsumerCache != null &&
            popConsumerCache.getPopInFlightMessageCount(group, topic, queueId) >=
                brokerConfig.getPopInflightMessageThreshold();
    }

    /**
     * 获取待过滤的消息数量
     * <p>
     * 为什么需要这个信息：
     * 1. 用于长轮询服务判断是否还有消息可消费
     * 2. 提供给客户端用于负载均衡决策
     * 3. 监控消费滞后情况
     */
    public long getPendingFilterCount(String groupId, String topicId, int queueId) {
        try {
            long maxOffset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topicId, queueId);
            long consumeOffset = this.brokerController.getConsumerOffsetManager().queryOffset(groupId, topicId, queueId);
            return maxOffset - consumeOffset;
        } catch (ConsumeQueueException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 重新编码重试消息
     * <p>
     * 为什么需要重新编码：
     * 1. 重试消息需要将topic名称从重试topic改回原始topic
     * 2. 需要添加Pop相关的检查点信息到消息属性中
     * 3. 清除存储大小以便重新计算编码后的大小
     * 4. 确保客户端接收到的消息格式正确
     */
    public GetMessageResult recodeRetryMessage(GetMessageResult getMessageResult,
        String topicId, long offset, long popTime, long invisibleTime) {
        if (getMessageResult.getMessageCount() == 0 ||
            getMessageResult.getMessageMapedList().isEmpty()) {
            return getMessageResult;
        }

        GetMessageResult result = new GetMessageResult(getMessageResult.getMessageCount());
        result.setStatus(GetMessageStatus.FOUND);
        String brokerName = brokerConfig.getBrokerName();

        for (SelectMappedBufferResult bufferResult : getMessageResult.getMessageMapedList()) {
            List<MessageExt> messageExtList = MessageDecoder.decodesBatch(
                bufferResult.getByteBuffer(), true, false, true);
            bufferResult.release(); // 及时释放原始buffer避免内存泄漏

            for (MessageExt messageExt : messageExtList) {
                try {
                    // When override retry message topic to origin topic,
                    // need clear message store size to recode
                    String ckInfo = ExtraInfoUtil.buildExtraInfo(offset, popTime, invisibleTime, 0,
                        messageExt.getTopic(), brokerName, messageExt.getQueueId(), messageExt.getQueueOffset());
                    // 添加检查点信息，用于消息确认和恢复
                    messageExt.getProperties().putIfAbsent(MessageConst.PROPERTY_POP_CK, ckInfo);
                    // 将重试topic改回原始topic，对客户端透明
                    messageExt.setTopic(topicId);
                    // 清除存储大小，强制重新编码
                    messageExt.setStoreSize(0);

                    byte[] encode = MessageDecoder.encode(messageExt, false);
                    ByteBuffer buffer = ByteBuffer.wrap(encode);
                    SelectMappedBufferResult tmpResult = new SelectMappedBufferResult(
                        bufferResult.getStartOffset(), buffer, encode.length, null);
                    result.addMessage(tmpResult);
                } catch (Exception e) {
                    log.error("PopConsumerService exception in recode retry message, topic={}", topicId, e);
                }
            }
        }
        return result;
    }

    /**
     * 处理获取消息结果
     * <p>
     * 为什么需要这个方法：
     * 1. 统一处理顺序消息和普通消息的不同逻辑
     * 2. 管理消费偏移量的提交策略
     * 3. 处理缓存合并优化
     * 4. 构建响应头信息
     */
    public PopConsumerContext handleGetMessageResult(PopConsumerContext context, GetMessageResult result,
        String topicId, int queueId, PopConsumerRecord.RetryType retryType, long offset) {

        if (GetMessageStatus.FOUND.equals(result.getStatus()) && !result.getMessageQueueOffset().isEmpty()) {
            if (context.isFifo()) {
                // 顺序消息需要设置阻塞状态，确保消息按顺序消费
                this.setFifoBlocked(context, context.getGroupId(), topicId, queueId, result.getMessageQueueOffset());
            }
            // 在这里构建响应头信息
            context.addGetMessageResult(result, topicId, queueId, retryType, offset);

            if (brokerConfig.isPopConsumerKVServiceLog()) {
                log.info("PopConsumerService pop, time={}, invisible={}, " +
                        "groupId={}, topic={}, queueId={}, offset={}, attemptId={}",
                    context.getPopTime(), context.getInvisibleTime(), context.getGroupId(),
                    topicId, queueId, result.getMessageQueueOffset(), context.getAttemptId());
            }
        }

        long commitOffset = offset;
        if (context.isFifo()) {
            // 顺序消息的偏移量提交策略
            if (!GetMessageStatus.FOUND.equals(result.getStatus())) {
                commitOffset = result.getNextBeginOffset();
            }
        } else {
            // 普通消息的偏移量提交策略
            // 先提交Pull偏移量，用于与传统Pull模式兼容
            this.brokerController.getConsumerOffsetManager().commitPullOffset(
                context.getClientHost(), context.getGroupId(), topicId, queueId, result.getNextBeginOffset());

            // 如果启用缓冲区合并，使用缓存中的最小偏移量作为提交偏移量
            // 这样可以避免缓存中的消息丢失
            if (brokerConfig.isEnablePopBufferMerge() && popConsumerCache != null) {
                long minOffset = popConsumerCache.getMinOffsetInCache(context.getGroupId(), topicId, queueId);
                if (minOffset != OFFSET_NOT_EXIST) {
                    commitOffset = minOffset;
                }
            }
        }
        // 提交最终的消费偏移量
        this.brokerController.getConsumerOffsetManager().commitOffset(
            context.getClientHost(), context.getGroupId(), topicId, queueId, commitOffset);
        return context;
    }

    /**
     * 获取Pop偏移量
     * <p>
     * 为什么需要特殊的偏移量获取逻辑：
     * 1. 支持多种初始化模式（从最小偏移量或最大偏移量开始）
     * 2. 处理偏移量重置逻辑
     * 3. 兼容传统的Pull偏移量
     * 4. 在没有偏移量时提供合理的默认值
     */
    public long getPopOffset(String groupId, String topicId, int queueId, int initMode) {
        // 先查询Pull偏移量，保持与传统模式的兼容性
        long offset = this.brokerController.getConsumerOffsetManager().queryPullOffset(groupId, topicId, queueId);

        if (offset < 0L) {
            // 如果没有找到偏移量，使用初始化逻辑
            try {
                offset = this.brokerController.getPopMessageProcessor()
                    .getInitOffset(topicId, groupId, queueId, initMode, true);
                log.info("PopConsumerService init offset, groupId={}, topicId={}, queueId={}, init={}, offset={}",
                    groupId, topicId, queueId, ConsumeInitMode.MIN == initMode ? "min" : "max", offset);
            } catch (ConsumeQueueException e) {
                throw new RuntimeException(e);
            }
        }

        // 检查是否有重置偏移量的需求
        Long resetOffset =
            this.brokerController.getConsumerOffsetManager().queryThenEraseResetOffset(topicId, groupId, queueId);
        if (resetOffset != null) {
            // 重置时需要清理相关缓存和状态
            this.clearCache(groupId, topicId, queueId);
            this.brokerController.getConsumerOrderInfoManager().clearBlock(topicId, groupId, queueId);
            this.brokerController.getConsumerOffsetManager()
                .commitOffset("ResetPopOffset", groupId, topicId, queueId, resetOffset);
        }

        return resetOffset != null ? resetOffset : offset;
    }

    /**
     * 异步获取消息
     * <p>
     * 为什么使用异步方式：
     * 1. 避免阻塞Pop请求处理线程
     * 2. 支持更高的并发处理能力
     * 3. 可以更好地处理存储异常情况
     * 4. 与RocketMQ的整体异步架构保持一致
     */
    public CompletableFuture<GetMessageResult> getMessageAsync(String clientHost,
        String groupId, String topicId, int queueId, long offset, int batchSize, MessageFilter filter) {

        log.debug("PopConsumerService getMessageAsync, groupId={}, topicId={}, queueId={}, offset={}, batchSize={}, filter={}",
            groupId, topicId, offset, queueId, batchSize, filter != null);

        CompletableFuture<GetMessageResult> getMessageFuture =
            brokerController.getMessageStore().getMessageAsync(groupId, topicId, queueId, offset, batchSize, filter);

        // 参考 org.apache.rocketmq.broker.processor.PopMessageProcessor#popMsgFromQueue
        return getMessageFuture.thenCompose(result -> {
            if (result == null) {
                return CompletableFuture.completedFuture(null);
            }

            // 可能存储偏移量不正确的情况
            if (GetMessageStatus.OFFSET_TOO_SMALL.equals(result.getStatus()) ||
                GetMessageStatus.OFFSET_OVERFLOW_BADLY.equals(result.getStatus()) ||
                GetMessageStatus.OFFSET_FOUND_NULL.equals(result.getStatus())) {

                // 提交偏移量，因为偏移量不正确
                // 如果存储中的偏移量大于cq偏移量，会导致重复消息，
                // 因为PopBuffer中的偏移量没有被提交。
                this.brokerController.getConsumerOffsetManager().commitOffset(
                    clientHost, groupId, topicId, queueId, result.getNextBeginOffset());

                log.warn("PopConsumerService getMessageAsync, initial offset because store is no correct, " +
                        "groupId={}, topicId={}, queueId={}, batchSize={}, offset={}->{}",
                    groupId, topicId, queueId, batchSize, offset, result.getNextBeginOffset());

                // 使用正确的偏移量重新获取消息
                return brokerController.getMessageStore().getMessageAsync(
                    groupId, topicId, queueId, result.getNextBeginOffset(), batchSize, filter);
            }
            return CompletableFuture.completedFuture(result);
        }).whenComplete((result, throwable) -> {
            if (throwable != null) {
                log.error("Pop getMessageAsync error", throwable);
            }
        });
    }

    /**
     * 设置顺序消息阻塞状态
     * <p>
     * 为什么顺序消息需要阻塞机制：
     * 1. 确保消息按顺序消费，前一条消息未确认时后续消息不能被消费
     * 2. 避免乱序消费导致的业务逻辑错误
     * 3. 顺序消息在broker端不支持重试功能，需要特殊处理
     * <p>
     * Fifo message does not have retry feature in broker
     */
    public void setFifoBlocked(PopConsumerContext context,
        String groupId, String topicId, int queueId, List<Long> queueOffsetList) {
        brokerController.getConsumerOrderInfoManager().update(
            context.getAttemptId(), false, topicId, groupId, queueId,
            context.getPopTime(), context.getInvisibleTime(), queueOffsetList, context.getOrderCountInfoBuilder());
    }

    /**
     * 检查顺序消息是否被阻塞
     */
    public boolean isFifoBlocked(PopConsumerContext context, String groupId, String topicId, int queueId) {
        return brokerController.getConsumerOrderInfoManager().checkBlock(
            context.getAttemptId(), topicId, groupId, queueId, context.getInvisibleTime());
    }

    /**
     * 异步获取消息的递归实现
     * <p>
     * 为什么使用递归方式：
     * 1. 支持从多个队列和重试topic获取消息
     * 2. 在消息数量不足时继续获取
     * 3. 实现优雅的异步链式调用
     * 4. 避免阻塞和回调地狱
     */
    protected CompletableFuture<PopConsumerContext> getMessageAsync(CompletableFuture<PopConsumerContext> future,
        String clientHost, String groupId, String topicId, int queueId, int batchSize, MessageFilter filter,
        PopConsumerRecord.RetryType retryType) {

        return future.thenCompose(result -> {
            // pop请求过多，不应该在这里添加剩余计数
            if (isPopShouldStop(groupId, topicId, queueId)) {
                return CompletableFuture.completedFuture(result);
            }

            // 当前请求会计算等待过滤的消息总数，用于长轮询服务中的新消息到达通知，
            // 需要忽略顺序消费场景中的积压。如果剩余消息数包括被阻塞队列的积压，
            // 会导致长轮询请求频繁的不必要唤醒，造成不必要的CPU使用。
            // 当客户端确认消息时，长轮询请求会通过AckMessageProcessor.ackOrderly()得到通知，
            // 消息不会被延迟。
            if (result.isFifo() && isFifoBlocked(result, groupId, topicId, queueId)) {
                // 这里不应该添加积压（最大偏移量 - 消费者偏移量）
                return CompletableFuture.completedFuture(result);
            }

            int remain = batchSize - result.getMessageCount();
            if (remain <= 0) {
                // 已经获取足够的消息，添加剩余计数信息
                result.addRestCount(this.getPendingFilterCount(groupId, topicId, queueId));
                return CompletableFuture.completedFuture(result);
            } else {
                // 继续获取消息
                final long consumeOffset = this.getPopOffset(groupId, topicId, queueId, result.getInitMode());
                return getMessageAsync(clientHost, groupId, topicId, queueId, consumeOffset, remain, filter)
                    .thenApply(getMessageResult -> handleGetMessageResult(
                        result, getMessageResult, topicId, queueId, retryType, consumeOffset));
            }
        });
    }

    /**
     * 异步Pop消息的主入口方法
     * <p>
     * 这是整个Pop消费流程的核心方法，包含完整的业务逻辑：
     * 1. 参数验证和锁获取
     * 2. 重试消息和普通消息的获取策略
     * 3. 数据持久化（缓存或存储）
     * 4. 消息重编码（针对重试消息）
     * 5. 资源清理和日志记录
     */
    public CompletableFuture<PopConsumerContext> popAsync(String clientHost, long popTime, long invisibleTime,
        String groupId, String topicId, int queueId, int batchSize, boolean fifo, String attemptId, int initMode,
        MessageFilter filter) {

        PopConsumerContext popConsumerContext =
            new PopConsumerContext(clientHost, popTime, invisibleTime, groupId, fifo, initMode, attemptId);

        // 检查topic配置和获取锁
        TopicConfig topicConfig = brokerController.getTopicConfigManager().selectTopicConfig(topicId);
        if (topicConfig == null || !consumerLockService.tryLock(groupId, topicId)) {
            return CompletableFuture.completedFuture(popConsumerContext);
        }

        log.debug("PopConsumerService popAsync, groupId={}, topicId={}, queueId={}, " +
                "batchSize={}, invisibleTime={}, fifo={}, attemptId={}, filter={}",
            groupId, topicId, queueId, batchSize, invisibleTime, fifo, attemptId, filter);

        String requestKey = groupId + "@" + topicId;
        String retryTopicV1 = KeyBuilder.buildPopRetryTopicV1(topicId, groupId);
        String retryTopicV2 = KeyBuilder.buildPopRetryTopicV2(topicId, groupId);

        // 使用请求计数实现负载均衡和重试策略
        long requestCount = Objects.requireNonNull(ConcurrentHashMapUtils.computeIfAbsent(
            requestCountTable, requestKey, k -> new AtomicLong(0L))).getAndIncrement();

        // 每5个请求中有1个优先从重试队列获取消息，平衡重试消息和普通消息
        boolean preferRetry = requestCount % 5L == 0L;

        CompletableFuture<PopConsumerContext> getMessageFuture =
            CompletableFuture.completedFuture(popConsumerContext);

        try {
            // 非顺序消息且优先重试的情况下，先从重试队列获取消息
            if (!fifo && preferRetry) {
                if (brokerConfig.isRetrieveMessageFromPopRetryTopicV1()) {
                    getMessageFuture = this.getMessageAsync(getMessageFuture, clientHost, groupId,
                        retryTopicV1, 0, batchSize, filter, PopConsumerRecord.RetryType.RETRY_TOPIC_V1);
                }
                if (brokerConfig.isEnableRetryTopicV2()) {
                    getMessageFuture = this.getMessageAsync(getMessageFuture, clientHost, groupId,
                        retryTopicV2, 0, batchSize, filter, PopConsumerRecord.RetryType.RETRY_TOPIC_V2);
                }
            }

            // 从普通队列获取消息
            if (queueId != -1) {
                // 获取指定队列的消息
                getMessageFuture = this.getMessageAsync(getMessageFuture, clientHost, groupId,
                    topicId, queueId, batchSize, filter, PopConsumerRecord.RetryType.NORMAL_TOPIC);
            } else {
                // 轮询所有队列获取消息，使用请求计数实现负载均衡
                for (int i = 0; i < topicConfig.getReadQueueNums(); i++) {
                    int current = (int) ((requestCount + i) % topicConfig.getReadQueueNums());
                    getMessageFuture = this.getMessageAsync(getMessageFuture, clientHost, groupId,
                        topicId, current, batchSize, filter, PopConsumerRecord.RetryType.NORMAL_TOPIC);
                }

                // 如果普通消息获取完毕且不是优先重试，再从重试队列获取
                if (!fifo && !preferRetry) {
                    if (brokerConfig.isRetrieveMessageFromPopRetryTopicV1()) {
                        getMessageFuture = this.getMessageAsync(getMessageFuture, clientHost, groupId,
                            retryTopicV1, 0, batchSize, filter, PopConsumerRecord.RetryType.RETRY_TOPIC_V1);
                    }
                    if (brokerConfig.isEnableRetryTopicV2()) {
                        getMessageFuture = this.getMessageAsync(getMessageFuture, clientHost, groupId,
                            retryTopicV2, 0, batchSize, filter, PopConsumerRecord.RetryType.RETRY_TOPIC_V2);
                    }
                }
            }

            return getMessageFuture.thenCompose(result -> {
                // 如果找到消息且不是顺序消息，需要持久化记录
                if (result.isFound() && !result.isFifo()) {
                    if (brokerConfig.isEnablePopBufferMerge() &&
                        popConsumerCache != null && !popConsumerCache.isCacheFull()) {
                        // 优先写入缓存，提高性能
                        this.popConsumerCache.writeRecords(result.getPopConsumerRecordList());
                    } else {
                        // 缓存满了或未启用缓存，直接写入持久化存储
                        this.popConsumerStore.writeRecords(result.getPopConsumerRecordList());
                    }

                    // 处理重试消息的重编码
                    for (int i = 0; i < result.getGetMessageResultList().size(); i++) {
                        GetMessageResult getMessageResult = result.getGetMessageResultList().get(i);
                        PopConsumerRecord popConsumerRecord = result.getPopConsumerRecordList().get(i);

                        // 如果缓冲区属于重试消息，消息需要重新编码。
                        // 当popResponseReturnActualRetryTopic为true或当前topic不是重试topic时，
                        // 缓冲区不应该被重新编码。
                        boolean recode = brokerConfig.isPopResponseReturnActualRetryTopic();
                        if (recode && popConsumerRecord.isRetry()) {
                            result.getGetMessageResultList().set(i, this.recodeRetryMessage(
                                getMessageResult, popConsumerRecord.getTopicId(),
                                popConsumerRecord.getQueueId(), result.getPopTime(), invisibleTime));
                        }
                    }
                }
                return CompletableFuture.completedFuture(result);
            }).whenComplete((result, throwable) -> {
                try {
                    if (throwable != null) {
                        log.error("PopConsumerService popAsync get message error",
                            throwable instanceof CompletionException ? throwable.getCause() : throwable);
                    }
                    if (result.getMessageCount() > 0) {
                        log.debug("PopConsumerService popAsync result, found={}, groupId={}, topicId={}, queueId={}, " +
                                "batchSize={}, invisibleTime={}, fifo={}, attemptId={}, filter={}", result.getMessageCount(),
                            groupId, topicId, queueId, batchSize, invisibleTime, fifo, attemptId, filter);
                    }
                } finally {
                    // 无论成功还是失败，都要释放锁
                    consumerLockService.unlock(groupId, topicId);
                }
            });
        } catch (Throwable t) {
            log.error("PopConsumerService popAsync error", t);
        }
        return getMessageFuture;
    }

    /**
     * 异步确认消息
     * <p>
     * 为什么需要ack机制：
     * 1. 标记消息已被成功消费，可以从存储中删除
     * 2. 避免消息重复投递
     * 3. 支持顺序消息的阻塞解除
     * 4. 触发长轮询请求的通知
     * <p>
     * 当接收到顺序确认时通知轮询请求
     */
    public CompletableFuture<Boolean> ackAsync(
        long popTime, long invisibleTime, String groupId, String topicId, int queueId, long offset) {

        if (brokerConfig.isPopConsumerKVServiceLog()) {
            log.info("PopConsumerService ack, time={}, invisible={}, groupId={}, topic={}, queueId={}, offset={}",
                popTime, invisibleTime, groupId, topicId, queueId, offset);
        }

        PopConsumerRecord record = new PopConsumerRecord(
            popTime, groupId, topicId, queueId, 0, invisibleTime, offset, null);

        // 优先从缓存中删除，如果缓存中没有再从持久化存储中删除
        if (brokerConfig.isEnablePopBufferMerge() && popConsumerCache != null) {
            if (popConsumerCache.deleteRecords(Collections.singletonList(record)).isEmpty()) {
                return CompletableFuture.completedFuture(true);
            }
        }
        this.popConsumerStore.deleteRecords(Collections.singletonList(record));
        return CompletableFuture.completedFuture(true);
    }

    /**
     * 修改消息不可见时间
     * <p>
     * 为什么需要这个功能：
     * 1. 允许消费者延长消息处理时间
     * 2. 支持动态调整消息的超时时间
     * 3. 在消息处理复杂的场景下避免消息超时重试
     * <p>
     * 参考 ChangeInvisibleTimeProcessor.appendCheckPointThenAckOrigin
     */
    public void changeInvisibilityDuration(long popTime, long invisibleTime,
        long changedPopTime, long changedInvisibleTime, String groupId, String topicId, int queueId, long offset) {

        if (brokerConfig.isPopConsumerKVServiceLog()) {
            log.info("PopConsumerService change, time={}, invisible={}, " +
                    "groupId={}, topic={}, queueId={}, offset={}, new time={}, new invisible={}",
                popTime, invisibleTime, groupId, topicId, queueId, offset, changedPopTime, changedInvisibleTime);
        }

        // 创建新的检查点记录（修改后的时间）
        PopConsumerRecord ckRecord = new PopConsumerRecord(
            changedPopTime, groupId, topicId, queueId, 0, changedInvisibleTime, offset, null);
        // 创建要删除的原始记录
        PopConsumerRecord ackRecord = new PopConsumerRecord(
            popTime, groupId, topicId, queueId, 0, invisibleTime, offset, null);

        // 先写入新记录，再删除旧记录，确保原子性
        this.popConsumerStore.writeRecords(Collections.singletonList(ckRecord));

        if (brokerConfig.isEnablePopBufferMerge() && popConsumerCache != null) {
            if (popConsumerCache.deleteRecords(Collections.singletonList(ackRecord)).isEmpty()) {
                return;
            }
        }
        this.popConsumerStore.deleteRecords(Collections.singletonList(ackRecord));
    }

    /**
     * 异步获取消息内容
     * <p>
     * 为什么使用escape bridge：
     * 1. 支持远程读取消息内容
     * 2. 统一消息读取接口
     * 3. 支持跨broker的消息访问
     * 4. 提供消息读取的抽象层
     */
    public CompletableFuture<Triple<MessageExt, String, Boolean>> getMessageAsync(PopConsumerRecord consumerRecord) {
        return this.brokerController.getEscapeBridge().getMessageAsync(consumerRecord.getTopicId(),
            consumerRecord.getOffset(), consumerRecord.getQueueId(), brokerConfig.getBrokerName(), false);
    }

    /**
     * 恢复单个消息记录
     * <p>
     * 为什么需要恢复机制：
     * 1. 处理超时未确认的消息
     * 2. 确保消息最终会被消费
     * 3. 避免消息丢失
     * 4. 支持消息重试机制
     */
    public CompletableFuture<Boolean> revive(PopConsumerRecord record) {
        return this.getMessageAsync(record)
            .thenCompose(result -> {
                if (result == null) {
                    log.error("PopConsumerService revive error, message may be lost, record={}", record);
                    return CompletableFuture.completedFuture(false);
                }

                // triple中的right为true表示获取消息需要重试
                if (result.getLeft() == null) {
                    log.info("PopConsumerService revive no need retry, record={}", record);
                    return CompletableFuture.completedFuture(!result.getRight());
                }

                return CompletableFuture.completedFuture(this.reviveRetry(record, result.getLeft()));
            });
    }

    /**
     * 清理缓存
     * <p>
     * 为什么需要锁机制：
     * 1. 防止清理过程中有新的写入操作
     * 2. 确保数据一致性
     * 3. 避免并发冲突
     */
    public void clearCache(String groupId, String topicId, int queueId) {
        // 循环获取锁，确保清理操作的原子性
        while (consumerLockService.tryLock(groupId, topicId)) {
        }
        try {
            if (popConsumerCache != null) {
                popConsumerCache.removeRecords(groupId, topicId, queueId);
            }
        } finally {
            consumerLockService.unlock(groupId, topicId);
        }
    }

    /**
     * 批量恢复过期消息记录
     * <p>
     * 这是Pop模式的核心恢复机制：
     * 1. 扫描过期的消息记录
     * 2. 异步处理恢复逻辑
     * 3. 处理失败重试
     * 4. 清理已处理的记录
     * 5. 更新扫描进度
     */
    public long revive(AtomicLong currentTime, int maxCount) {
        Stopwatch stopwatch = Stopwatch.createStarted();

        // 设置上限时间，避免处理太新的记录（可能还在处理中）
        long upperTime = System.currentTimeMillis() - 50L;

        // 扫描过期记录，从上次扫描位置开始
        List<PopConsumerRecord> consumerRecords = this.popConsumerStore.scanExpiredRecords(
            currentTime.get() - TimeUnit.SECONDS.toMillis(3), upperTime, maxCount);
        long scanCostTime = stopwatch.elapsed(TimeUnit.MILLISECONDS);

        Queue<PopConsumerRecord> failureList = new LinkedBlockingQueue<>();
        List<CompletableFuture<?>> futureList = new ArrayList<>(consumerRecords.size());

        // 这里可以合并读取操作来优化性能
        for (PopConsumerRecord record : consumerRecords) {
            futureList.add(this.revive(record).thenAccept(result -> {
                if (!result) {
                    // 恢复失败，根据重试次数决定处理策略
                    if (record.getAttemptTimes() < brokerConfig.getPopReviveMaxAttemptTimes()) {
                        // 使用指数退避算法计算下次重试时间
                        long backoffInterval = 1000L * REWRITE_INTERVALS_IN_SECONDS[
                            Math.min(REWRITE_INTERVALS_IN_SECONDS.length, record.getAttemptTimes())];
                        long nextInvisibleTime = record.getInvisibleTime() + backoffInterval;

                        PopConsumerRecord retryRecord = new PopConsumerRecord(System.currentTimeMillis(),
                            record.getGroupId(), record.getTopicId(), record.getQueueId(),
                            record.getRetryFlag(), nextInvisibleTime, record.getOffset(), record.getAttemptId());
                        retryRecord.setAttemptTimes(record.getAttemptTimes() + 1);
                        failureList.add(retryRecord);

                        log.warn("PopConsumerService revive backoff retry, record={}", retryRecord);
                    } else {
                        // 超过最大重试次数，记录错误（消息可能丢失）
                        log.error("PopConsumerService drop record, message may be lost, record={}", record);
                    }
                }
            }));
        }

        // 等待所有恢复操作完成
        CompletableFuture.allOf(futureList.toArray(new CompletableFuture[0])).join();

        // 将失败记录重新写入存储
        this.popConsumerStore.writeRecords(new ArrayList<>(failureList));
        // 删除已处理的记录
        this.popConsumerStore.deleteRecords(consumerRecords);

        // 更新扫描进度
        currentTime.set(consumerRecords.isEmpty() ?
            upperTime : consumerRecords.get(consumerRecords.size() - 1).getVisibilityTimeout());

        // 记录恢复统计信息
        if (brokerConfig.isEnablePopBufferMerge()) {
            log.info("PopConsumerService, key size={}, cache size={}, revive count={}, failure count={}, " +
                    "behindInMillis={}, scanInMillis={}, costInMillis={}",
                popConsumerCache.getCacheKeySize(), popConsumerCache.getCacheSize(),
                consumerRecords.size(), failureList.size(), upperTime - currentTime.get(),
                scanCostTime, stopwatch.elapsed(TimeUnit.MILLISECONDS));
        } else {
            log.info("PopConsumerService, revive count={}, failure count={}, " +
                    "behindInMillis={}, scanInMillis={}, costInMillis={}",
                consumerRecords.size(), failureList.size(), upperTime - currentTime.get(),
                scanCostTime, stopwatch.elapsed(TimeUnit.MILLISECONDS));
        }

        return consumerRecords.size();
    }

    /**
     * 创建重试topic（如果需要的话）
     * <p>
     * 为什么需要动态创建重试topic：
     * 1. 避免预先创建大量不必要的topic
     * 2. 支持按需创建，节省资源
     * 3. 自动化topic管理
     */
    public void createRetryTopicIfNeeded(String groupId, String topicId) {
        TopicConfig topicConfig = brokerController.getTopicConfigManager().selectTopicConfig(topicId);
        if (topicConfig != null) {
            return;
        }

        // 创建重试topic配置：1个读队列，1个写队列，支持读写权限
        topicConfig = new TopicConfig(topicId, 1, 1,
            PermName.PERM_READ | PermName.PERM_WRITE, 0);
        topicConfig.setTopicFilterType(TopicFilterType.SINGLE_TAG);
        brokerController.getTopicConfigManager().updateTopicConfig(topicConfig);

        // 初始化消费偏移量
        long offset = this.brokerController.getConsumerOffsetManager().queryOffset(groupId, topicId, 0);
        if (offset < 0) {
            this.brokerController.getConsumerOffsetManager().commitOffset(
                "InitPopOffset", groupId, topicId, 0, 0);
        }
    }

    /**
     * 恢复重试消息
     * <p>
     * 这个方法实现了消息的重试投递机制：
     * 1. 判断是否已经是重试消息
     * 2. 构建重试topic名称
     * 3. 复制消息内容和属性
     * 4. 增加重试次数
     * 5. 设置首次Pop时间
     * 6. 投递到重试队列
     *
     * @SuppressWarnings("DuplicatedCode") 参考 org.apache.rocketmq.broker.processor.PopReviveService#reviveRetry
     */
    public boolean reviveRetry(PopConsumerRecord record, MessageExt messageExt) {
        if (brokerConfig.isPopConsumerKVServiceLog()) {
            log.info("PopConsumerService revive, time={}, invisible={}, groupId={}, topic={}, queueId={}, offset={}",
                record.getPopTime(), record.getInvisibleTime(), record.getGroupId(), record.getTopicId(),
                record.getQueueId(), record.getOffset());
        }

        // 判断是否已经是重试消息
        boolean retry = StringUtils.startsWith(record.getTopicId(), MixAll.RETRY_GROUP_TOPIC_PREFIX);
        String retryTopic = retry ? record.getTopicId() : KeyBuilder.buildPopRetryTopic(
            record.getTopicId(), record.getGroupId(), brokerConfig.isEnableRetryTopicV2());

        // 确保重试topic存在
        this.createRetryTopicIfNeeded(record.getGroupId(), retryTopic);

        // 深度复制消息内容
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setTopic(retryTopic);
        msgInner.setBody(messageExt.getBody() != null ? messageExt.getBody() : new byte[] {});
        msgInner.setQueueId(0); // 重试消息都投递到队列0

        if (messageExt.getTags() != null) {
            msgInner.setTags(messageExt.getTags());
        } else {
            MessageAccessor.setProperties(msgInner, new HashMap<>());
        }

        msgInner.setBornTimestamp(messageExt.getBornTimestamp());
        msgInner.setFlag(messageExt.getFlag());
        msgInner.setSysFlag(messageExt.getSysFlag());
        msgInner.setBornHost(brokerController.getStoreHost());
        msgInner.setStoreHost(brokerController.getStoreHost());
        msgInner.setReconsumeTimes(messageExt.getReconsumeTimes() + 1); // 增加重试次数
        msgInner.getProperties().putAll(messageExt.getProperties());

        // 设置首次Pop时间，用于重试时长统计
        if (messageExt.getReconsumeTimes() == 0 ||
            msgInner.getProperties().get(MessageConst.PROPERTY_FIRST_POP_TIME) == null) {
            msgInner.getProperties().put(MessageConst.PROPERTY_FIRST_POP_TIME, String.valueOf(record.getPopTime()));
        }

        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));

        // 投递消息到重试队列
        PutMessageResult putMessageResult =
            brokerController.getEscapeBridge().putMessageToSpecificQueue(msgInner);

        if (putMessageResult.getAppendMessageResult() == null ||
            putMessageResult.getAppendMessageResult().getStatus() != AppendMessageStatus.PUT_OK) {
            log.error("PopConsumerService revive retry msg error, put status={}, ck={}, delay={}ms",
                putMessageResult, JSON.toJSONString(record), System.currentTimeMillis() - record.getVisibilityTimeout());
            return false;
        }

        // 更新统计信息
        if (this.brokerController.getBrokerStatsManager() != null) {
            this.brokerController.getBrokerStatsManager().incBrokerPutNums(msgInner.getTopic(), 1);
            this.brokerController.getBrokerStatsManager().incTopicPutNums(msgInner.getTopic());
            this.brokerController.getBrokerStatsManager().incTopicPutSize(
                msgInner.getTopic(), putMessageResult.getAppendMessageResult().getWroteBytes());
        }

        return true;
    }

    /**
     * 将KV存储记录导出到恢复topic
     * <p>
     * 为什么需要这个功能：
     * 1. 支持从KV存储迁移到文件存储
     * 2. 数据备份和恢复
     * 3. 存储格式升级
     * 4. 调试和排查问题
     *
     * @SuppressWarnings("ExtractMethodRecommender")
     */
    public synchronized void transferToFsStore() {
        Stopwatch stopwatch = Stopwatch.createStarted();

        while (true) {
            try {
                // 分批读取KV存储中的记录
                List<PopConsumerRecord> consumerRecords = this.popConsumerStore.scanExpiredRecords(
                    0, Long.MAX_VALUE, brokerConfig.getPopReviveMaxReturnSizePerRead());

                if (consumerRecords == null || consumerRecords.isEmpty()) {
                    break;
                }

                // 将每个记录转换为检查点消息并投递到恢复topic
                for (PopConsumerRecord record : consumerRecords) {
                    PopCheckPoint ck = new PopCheckPoint();
                    ck.setBitMap(0);
                    ck.setNum((byte) 1);
                    ck.setPopTime(record.getPopTime());
                    ck.setInvisibleTime(record.getInvisibleTime());
                    ck.setStartOffset(record.getOffset());
                    ck.setCId(record.getGroupId());
                    ck.setTopic(record.getTopicId());
                    ck.setQueueId(record.getQueueId());
                    ck.setBrokerName(brokerConfig.getBrokerName());
                    ck.addDiff(0);
                    ck.setRePutTimes(ck.getRePutTimes());

                    // 选择恢复队列ID
                    int reviveQueueId = (int) record.getOffset() % brokerConfig.getReviveQueueNum();
                    MessageExtBrokerInner ckMsg =
                        brokerController.getPopMessageProcessor().buildCkMsg(ck, reviveQueueId);
                    brokerController.getMessageStore().asyncPutMessage(ckMsg).join();
                }

                log.info("PopConsumerStore transfer from kvStore to fsStore, count={}", consumerRecords.size());
                this.popConsumerStore.deleteRecords(consumerRecords);
                this.waitForRunning(1); // 短暂休息，避免占用过多资源
            } catch (Throwable t) {
                log.error("PopConsumerStore transfer from kvStore to fsStore failure", t);
            }
        }

        log.info("PopConsumerStore transfer to fsStore finish, cost={}ms", stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }

    @Override
    public String getServiceName() {
        return PopConsumerService.class.getSimpleName();
    }

    /**
     * 获取Pop消费者存储（测试用）
     */
    @VisibleForTesting
    protected PopConsumerKVStore getPopConsumerStore() {
        return popConsumerStore;
    }

    /**
     * 获取消费者锁服务
     */
    public PopConsumerLockService getConsumerLockService() {
        return consumerLockService;
    }

    /**
     * 启动服务
     * <p>
     * 为什么需要特定的启动顺序：
     * 1. 存储必须先启动成功
     * 2. 缓存依赖于存储
     * 3. 服务线程最后启动
     */
    @Override
    public void start() {
        if (!this.popConsumerStore.start()) {
            throw new RuntimeException("PopConsumerStore init error");
        }

        if (this.popConsumerCache != null) {
            this.popConsumerCache.start();
        }

        super.start();
    }

    /**
     * 关闭服务
     * <p>
     * 为什么需要优雅关闭：
     * 1. 等待正在进行的写操作完成
     * 2. 避免数据丢失
     * 3. 正确释放资源
     * 4. 防止内存泄漏
     */
    @Override
    public void shutdown() {
        // 阻塞关闭线程直到写记录完成
        super.shutdown();

        // 等待消费者运行标志变为false
        do {
            this.waitForRunning(10);
        }
        while (consumerRunning.get());

        // 按顺序关闭各个组件
        if (this.popConsumerCache != null) {
            this.popConsumerCache.shutdown();
        }

        if (this.popConsumerStore != null) {
            this.popConsumerStore.shutdown();
        }
    }

    /**
     * 服务主循环
     * <p>
     * 这个方法实现了Pop消费服务的核心调度逻辑：
     * 1. 定期恢复过期消息
     * 2. 清理过期锁
     * 3. 根据恢复数量调整休眠时间
     * 4. 异常处理和恢复
     */
    @Override
    public void run() {
        this.consumerRunning.set(true);

        while (!isStopped()) {
            try {
                // 防止读写操作期间的并发问题
                long reviveCount = this.revive(this.currentTime,
                    brokerConfig.getPopReviveMaxReturnSizePerRead());

                long current = System.currentTimeMillis();
                // 每分钟清理一次过期锁，避免内存泄漏
                if (lastCleanupLockTime.get() + TimeUnit.MINUTES.toMillis(1) < current) {
                    this.consumerLockService.removeTimeout();
                    this.lastCleanupLockTime.set(current);
                }

                // 如果恢复的消息数量少于最大值，说明暂时没有更多过期消息，可以休眠
                if (reviveCount < brokerConfig.getPopReviveMaxReturnSizePerRead()) {
                    this.waitForRunning(500);
                }
            } catch (Exception e) {
                log.error("PopConsumerService revive error", e);
                this.waitForRunning(500); // 异常时也要休眠，避免无限循环
            }
        }

        this.consumerRunning.set(false);
    }
}
