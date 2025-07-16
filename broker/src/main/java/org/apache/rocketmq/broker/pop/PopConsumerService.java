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
 * Pop消费者服务 - RocketMQ Pop模式的核心服务类
 *
 * 【设计目标】
 * 这是RocketMQ Pop模式的核心服务，负责：
 * 1. 处理Pop消息请求的完整生命周期管理
 * 2. 管理消息的不可见时间（invisibility timeout）
 * 3. 处理消息的自动重试和恢复机制
 * 4. 维护Pop消息的状态存储（缓存+持久化）
 * 5. 支持顺序消息的阻塞机制
 *
 * 【核心特性】
 * - 提供比传统Pull模式更好的消费体验
 * - 避免消息重复消费（通过不可见机制）
 * - 确保消息最终一致性（通过重试机制）
 * - 支持高并发场景下的消费者负载均衡
 *
 * 【调用链路概述】
 * 1. BrokerController.start() -> PopConsumerService.start()
 * 2. PopMessageProcessor.processRequest() -> PopConsumerService.popAsync()
 * 3. AckMessageProcessor.processRequest() -> PopConsumerService.ackAsync()
 * 4. ServiceThread.run() -> PopConsumerService.revive() (定时恢复过期消息)
 */
public class PopConsumerService extends ServiceThread {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_POP_LOGGER_NAME);

    /**
     * 偏移量不存在的标识常量
     * 【使用场景】当查询缓存中的最小偏移量时，如果不存在则返回此值
     */
    private static final long OFFSET_NOT_EXIST = -1L;

    /**
     * RocksDB存储目录名
     * 【存储结构】${storePathRootDir}/kvStore/
     */
    private static final String ROCKSDB_DIRECTORY = "kvStore";

    /**
     * 重试间隔数组（单位：秒）- 使用指数退避策略避免消息堆积
     * 【策略说明】前期密集重试（适合临时故障），后期稀疏重试（避免资源浪费）
     * 【使用场景】在revive()方法中，根据消息的重试次数选择对应的退避间隔
     */
    private static final int[] REWRITE_INTERVALS_IN_SECONDS =
        new int[] {10, 30, 60, 120, 180, 240, 300, 360, 420, 480, 540, 600, 1200, 1800, 3600, 7200};

    // ========== 核心组件字段 ==========

    /**
     * 消费者运行状态标识
     * 【用途】用于优雅关闭时等待写操作完成，防止数据丢失
     * 【调用链路】shutdown() -> 等待consumerRunning变为false -> 关闭各组件
     */
    private final AtomicBoolean consumerRunning;

    /**
     * Broker配置对象
     * 【用途】获取各种配置参数，如重试次数、缓存大小等
     */
    private final BrokerConfig brokerConfig;

    /**
     * Broker控制器
     * 【用途】访问存储、偏移量管理、Topic配置等核心服务
     */
    private final BrokerController brokerController;

    /**
     * 当前扫描时间戳
     * 【用途】记录恢复任务的扫描进度，避免重复扫描
     * 【调用链路】run() -> revive(currentTime) -> 更新currentTime
     */
    private final AtomicLong currentTime;

    /**
     * 上次清理锁的时间
     * 【用途】控制锁清理频率，避免频繁清理过期锁消耗CPU
     * 【调用链路】run() -> 每分钟清理一次过期锁
     */
    private final AtomicLong lastCleanupLockTime;

    /**
     * Pop消费者缓存
     * 【用途】提供热点数据的快速访问，减少磁盘IO
     * 【存储内容】PopConsumerRecord记录
     * 【调用链路】popAsync() -> writeRecords() / deleteRecords()
     */
    private final PopConsumerCache popConsumerCache;

    /**
     * Pop消费者持久化存储
     * 【用途】确保数据不丢失，使用RocksDB实现
     * 【存储内容】PopConsumerRecord记录
     * 【调用链路】revive() -> scanExpiredRecords() / deleteRecords()
     */
    private final PopConsumerKVStore popConsumerStore;

    /**
     * 消费者锁服务
     * 【用途】防止同一消费组的并发操作冲突
     * 【锁粒度】groupId + topicId
     * 【调用链路】popAsync() -> tryLock() / unlock()
     */
    private final PopConsumerLockService consumerLockService;

    /**
     * 请求计数表
     * 【用途】实现负载均衡和重试策略
     * 【Key格式】groupId@topicId
     * 【调用链路】popAsync() -> 根据计数决定是否优先从重试队列获取消息
     */
    private final ConcurrentMap<String /* groupId@topicId*/, AtomicLong> requestCountTable;

    /**
     * 构造函数 - 初始化Pop消费者服务的所有组件
     *
     * 【调用链路】BrokerController.initialize() -> new PopConsumerService()
     * 【初始化顺序】配置 -> 锁服务 -> 存储 -> 缓存
     *
     * @param brokerController Broker控制器，提供核心服务访问
     */
    public PopConsumerService(BrokerController brokerController) {
        this.brokerController = brokerController;
        this.brokerConfig = brokerController.getBrokerConfig();

        // 初始化运行状态和计数器
        this.consumerRunning = new AtomicBoolean(false);
        this.requestCountTable = new ConcurrentHashMap<>();

        // 初始时间设置为3秒，避免启动时立即扫描大量过期数据
        // 【设计意图】系统启动时可能有大量历史数据，逐步扫描避免启动压力
        this.currentTime = new AtomicLong(TimeUnit.SECONDS.toMillis(3));
        this.lastCleanupLockTime = new AtomicLong(System.currentTimeMillis());

        // 锁超时时间设置为2分钟，平衡性能和资源占用
        // 【设计考虑】太短容易误删活跃锁，太长占用内存过多
        this.consumerLockService = new PopConsumerLockService(TimeUnit.MINUTES.toMillis(2));

        // 使用RocksDB作为持久化存储，支持高性能的KV操作
        // 【存储路径】${storePathRootDir}/kvStore/
        this.popConsumerStore = new PopConsumerRocksdbStore(Paths.get(
            brokerController.getMessageStoreConfig().getStorePathRootDir(), ROCKSDB_DIRECTORY).toString());

        // 只有启用缓冲区合并时才创建缓存，节省内存
        // 【条件判断】避免不必要的内存分配
        this.popConsumerCache = brokerConfig.isEnablePopBufferMerge() ? new PopConsumerCache(
            brokerController, this.popConsumerStore, this.consumerLockService, this::revive) : null;

        log.info("PopConsumerService init, buffer={}, rocksdb filePath={}",
            brokerConfig.isEnablePopBufferMerge(), this.popConsumerStore.getFilePath());
    }

    /**
     * 判断是否应该停止Pop操作 - 流控机制
     *
     * 【设计目的】
     * 1. 防止消费者消费能力不足导致的消息堆积
     * 2. 避免内存中accumulating过多未确认消息
     * 3. 提供流控保护，确保系统稳定性
     *
     * 【调用链路】
     * PopConsumerService.getMessageAsync() -> isPopShouldStop() -> 判断是否继续获取消息
     *
     * 【流控原理】
     * In-flight messages are those that have been received from a queue
     * by a consumer but have not yet been deleted. For standard queues,
     * there is a limit on the number of in-flight messages, depending on queue traffic and message backlog.
     *
     * @param group 消费组名称
     * @param topic Topic名称
     * @param queueId 队列ID
     * @return true表示应该停止Pop操作
     */
    public boolean isPopShouldStop(String group, String topic, int queueId) {
        return brokerConfig.isEnablePopMessageThreshold() && popConsumerCache != null &&
            popConsumerCache.getPopInFlightMessageCount(group, topic, queueId) >=
                brokerConfig.getPopInflightMessageThreshold();
    }

    /**
     * 获取待过滤的消息数量 - 用于长轮询和负载均衡
     *
     * 【业务用途】
     * 1. 用于长轮询服务判断是否还有消息可消费
     * 2. 提供给客户端用于负载均衡决策
     * 3. 监控消费滞后情况
     *
     * 【调用链路】
     * PopConsumerService.getMessageAsync() -> getPendingFilterCount() -> 计算剩余消息数
     *
     * 【计算公式】maxOffset - consumeOffset = 待消费消息数
     *
     * @param groupId 消费组ID
     * @param topicId Topic ID
     * @param queueId 队列ID
     * @return 待过滤的消息数量
     */
    public long getPendingFilterCount(String groupId, String topicId, int queueId) {
        try {
            // 获取队列的最大偏移量（最新消息位置）
            long maxOffset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topicId, queueId);
            // 获取当前消费偏移量
            long consumeOffset = this.brokerController.getConsumerOffsetManager().queryOffset(groupId, topicId, queueId);
            // 计算差值即为待消费消息数
            return maxOffset - consumeOffset;
        } catch (ConsumeQueueException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 重新编码重试消息 - 消息格式转换
     *
     * 【核心功能】
     * 1. 重试消息需要将topic名称从重试topic改回原始topic
     * 2. 需要添加Pop相关的检查点信息到消息属性中
     * 3. 清除存储大小以便重新计算编码后的大小
     * 4. 确保客户端接收到的消息格式正确
     *
     * 【调用链路】
     * PopConsumerService.popAsync() -> recodeRetryMessage() -> 处理重试消息格式
     *
     * 【编码步骤】
     * 1. 解码原始消息 -> 2. 修改Topic名称 -> 3. 添加检查点信息 -> 4. 重新编码
     *
     * @param getMessageResult 原始消息结果
     * @param topicId 目标Topic ID（原始Topic）
     * @param offset 消息偏移量
     * @param popTime Pop时间
     * @param invisibleTime 不可见时间
     * @return 重新编码后的消息结果
     */
    public GetMessageResult recodeRetryMessage(GetMessageResult getMessageResult,
        String topicId, long offset, long popTime, long invisibleTime) {

        // 边界检查：如果没有消息则直接返回
        if (getMessageResult.getMessageCount() == 0 ||
            getMessageResult.getMessageMapedList().isEmpty()) {
            return getMessageResult;
        }

        // 创建新的结果对象
        GetMessageResult result = new GetMessageResult(getMessageResult.getMessageCount());
        result.setStatus(GetMessageStatus.FOUND);
        String brokerName = brokerConfig.getBrokerName();

        // 遍历所有消息缓冲区
        for (SelectMappedBufferResult bufferResult : getMessageResult.getMessageMapedList()) {
            // 批量解码消息
            List<MessageExt> messageExtList = MessageDecoder.decodesBatch(
                bufferResult.getByteBuffer(), true, false, true);

            // 及时释放原始buffer避免内存泄漏
            bufferResult.release();

            // 处理每条消息
            for (MessageExt messageExt : messageExtList) {
                try {
                    // When override retry message topic to origin topic,
                    // need clear message store size to recode

                    // 构建检查点信息，包含Pop相关的元数据
                    String ckInfo = ExtraInfoUtil.buildExtraInfo(offset, popTime, invisibleTime, 0,
                        messageExt.getTopic(), brokerName, messageExt.getQueueId(), messageExt.getQueueOffset());

                    // 添加检查点信息到消息属性，用于消息确认和恢复
                    messageExt.getProperties().putIfAbsent(MessageConst.PROPERTY_POP_CK, ckInfo);

                    // 将重试topic改回原始topic，对客户端透明
                    messageExt.setTopic(topicId);

                    // 清除存储大小，强制重新编码（因为Topic名称变了）
                    messageExt.setStoreSize(0);

                    // 重新编码消息
                    byte[] encode = MessageDecoder.encode(messageExt, false);
                    ByteBuffer buffer = ByteBuffer.wrap(encode);

                    // 创建新的缓冲区结果
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
     * 处理获取消息结果 - 统一的结果处理逻辑
     *
     * 【核心职责】
     * 1. 统一处理顺序消息和普通消息的不同逻辑
     * 2. 管理消费偏移量的提交策略
     * 3. 处理缓存合并优化
     * 4. 构建响应头信息
     *
     * 【调用链路】
     * PopConsumerService.getMessageAsync() -> handleGetMessageResult() -> 处理消息结果
     *
     * 【偏移量提交策略】
     * - 顺序消息：严格按顺序提交，未找到消息时提交nextBeginOffset
     * - 普通消息：考虑缓存中的最小偏移量，避免缓存数据丢失
     *
     * @param context Pop消费上下文
     * @param result 获取消息的结果
     * @param topicId Topic ID
     * @param queueId 队列ID
     * @param retryType 重试类型
     * @param offset 起始偏移量
     * @return 更新后的消费上下文
     */
    public PopConsumerContext handleGetMessageResult(PopConsumerContext context, GetMessageResult result,
        String topicId, int queueId, PopConsumerRecord.RetryType retryType, long offset) {

        // 如果找到消息且有具体的队列偏移量
        if (GetMessageStatus.FOUND.equals(result.getStatus()) && !result.getMessageQueueOffset().isEmpty()) {

            if (context.isFifo()) {
                // 顺序消息需要设置阻塞状态，确保消息按顺序消费
                // 【设计原理】前一条消息未确认时，后续消息不能被消费
                // 【新增】添加 result，用于给 OrderlyConsumeManager 传递 pop 成功的数据
                this.setFifoBlocked(context, context.getGroupId(), topicId, queueId, result.getMessageQueueOffset(), result);
            }

            // 在这里构建响应头信息，包含偏移量、重试类型等元数据
            context.addGetMessageResult(result, topicId, queueId, retryType, offset);

            // 记录详细的Pop操作日志（如果启用）
            if (brokerConfig.isPopConsumerKVServiceLog()) {
                log.info("PopConsumerService pop, time={}, invisible={}, " +
                        "groupId={}, topic={}, queueId={}, offset={}, attemptId={}",
                    context.getPopTime(), context.getInvisibleTime(), context.getGroupId(),
                    topicId, queueId, result.getMessageQueueOffset(), context.getAttemptId());
            }
        }

        // 计算要提交的偏移量
        long commitOffset = offset;

        if (context.isFifo()) {
            // 顺序消息的偏移量提交策略
            if (!GetMessageStatus.FOUND.equals(result.getStatus())) {
                // 没找到消息时，提交下一个开始偏移量
                commitOffset = result.getNextBeginOffset();
            }
            // 找到消息时，保持原偏移量不变（等待消息确认后再提交）
        } else {
            // 普通消息的偏移量提交策略

            // 先提交Pull偏移量，用于与传统Pull模式兼容
            this.brokerController.getConsumerOffsetManager().commitPullOffset(
                context.getClientHost(), context.getGroupId(), topicId, queueId, result.getNextBeginOffset());

            // 如果启用缓冲区合并，使用缓存中的最小偏移量作为提交偏移量
            // 【设计目的】这样可以避免缓存中的消息丢失
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
     * 获取Pop偏移量 - 多重策略的偏移量获取
     *
     * 【复杂逻辑处理】
     * 1. 支持多种初始化模式（从最小偏移量或最大偏移量开始）
     * 2. 处理偏移量重置逻辑
     * 3. 兼容传统的Pull偏移量
     * 4. 在没有偏移量时提供合理的默认值
     *
     * 【调用链路】
     * PopConsumerService.getMessageAsync() -> getPopOffset() -> 获取消费起始位置
     *
     * 【偏移量优先级】
     * 1. 重置偏移量（最高优先级）
     * 2. Pull偏移量（兼容性考虑）
     * 3. 初始化偏移量（默认策略）
     *
     * @param groupId 消费组ID
     * @param topicId Topic ID
     * @param queueId 队列ID
     * @param initMode 初始化模式
     * @return 计算得出的Pop偏移量
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

        // 检查是否有重置偏移量的需求（管理员手动重置）
        Long resetOffset =
            this.brokerController.getConsumerOffsetManager().queryThenEraseResetOffset(topicId, groupId, queueId);

        if (resetOffset != null) {
            // 重置时需要清理相关缓存和状态，确保干净的重新开始
            this.clearCache(groupId, topicId, queueId);
            this.brokerController.getConsumerOrderInfoManager().clearBlock(topicId, groupId, queueId);
            this.brokerController.getConsumerOffsetManager()
                .commitOffset("ResetPopOffset", groupId, topicId, queueId, resetOffset);
        }

        // 返回最终确定的偏移量
        return resetOffset != null ? resetOffset : offset;
    }

    /**
     * 异步获取消息 - 存储层消息获取的异步封装
     *
     * 【异步设计原因】
     * 1. 避免阻塞Pop请求处理线程
     * 2. 支持更高的并发处理能力
     * 3. 可以更好地处理存储异常情况
     * 4. 与RocketMQ的整体异步架构保持一致
     *
     * 【调用链路】
     * PopConsumerService.getMessageAsync(recursive) -> getMessageAsync(storage) -> MessageStore.getMessageAsync()
     *
     * 【异常处理】
     * 处理偏移量不正确的情况，自动修正并重新获取
     *
     * @param clientHost 客户端主机
     * @param groupId 消费组ID
     * @param topicId Topic ID
     * @param queueId 队列ID
     * @param offset 起始偏移量
     * @param batchSize 批量大小
     * @param filter 消息过滤器
     * @return 异步获取消息的Future结果
     */
    public CompletableFuture<GetMessageResult> getMessageAsync(String clientHost,
        String groupId, String topicId, int queueId, long offset, int batchSize, MessageFilter filter) {

        log.debug("PopConsumerService getMessageAsync, groupId={}, topicId={}, queueId={}, offset={}, batchSize={}, filter={}",
            groupId, topicId, offset, queueId, batchSize, filter != null);

        // 调用存储层异步获取消息
        CompletableFuture<GetMessageResult> getMessageFuture =
            brokerController.getMessageStore().getMessageAsync(groupId, topicId, queueId, offset, batchSize, filter);

        // 参考 org.apache.rocketmq.broker.processor.PopMessageProcessor#popMsgFromQueue
        return getMessageFuture.thenCompose(result -> {
            if (result == null) {
                return CompletableFuture.completedFuture(null);
            }

            // 处理可能存在的存储偏移量不正确情况
            if (GetMessageStatus.OFFSET_TOO_SMALL.equals(result.getStatus()) ||
                GetMessageStatus.OFFSET_OVERFLOW_BADLY.equals(result.getStatus()) ||
                GetMessageStatus.OFFSET_FOUND_NULL.equals(result.getStatus())) {

                // 提交偏移量，因为偏移量不正确
                // 【重要说明】如果存储中的偏移量大于cq偏移量，会导致重复消息，
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
     * 设置顺序消息阻塞状态 - 顺序消费保证机制
     *
     * 【顺序消息阻塞原理】
     * 1. 确保消息按顺序消费，前一条消息未确认时后续消息不能被消费
     * 2. 避免乱序消费导致的业务逻辑错误
     * 3. 顺序消息在broker端不支持重试功能，需要特殊处理
     *
     * 【调用链路】
     * PopConsumerService.handleGetMessageResult() -> setFifoBlocked() -> ConsumerOrderInfoManager.update()
     *
     * 【注意】Fifo message does not have retry feature in broker
     *
     * @param context Pop消费上下文
     * @param groupId 消费组ID
     * @param topicId Topic ID
     * @param queueId 队列ID
     * @param queueOffsetList 消息队列偏移量列表
     */
    public void setFifoBlocked(PopConsumerContext context,
        String groupId, String topicId, int queueId, List<Long> queueOffsetList, GetMessageResult result) {
        brokerController.getConsumerOrderInfoManager().update(
            context.getAttemptId(), false, topicId, groupId, queueId,
            context.getPopTime(), context.getInvisibleTime(), queueOffsetList, context.getOrderCountInfoBuilder(), result);
    }

    /**
     * 检查顺序消息是否被阻塞
     *
     * 【调用链路】
     * PopConsumerService.getMessageAsync() -> isFifoBlocked() -> ConsumerOrderInfoManager.checkBlock()
     *
     * @param context Pop消费上下文
     * @param groupId 消费组ID
     * @param topicId Topic ID
     * @param queueId 队列ID
     * @return true表示被阻塞
     */
    public boolean isFifoBlocked(PopConsumerContext context, String groupId, String topicId, int queueId) {
        return brokerController.getConsumerOrderInfoManager().checkBlock(
            context.getAttemptId(), topicId, groupId, queueId, context.getInvisibleTime());
    }

    /**
     * 异步获取消息的递归实现 - 多队列、多类型消息的获取逻辑
     *
     * 【递归设计优势】
     * 1. 支持从多个队列和重试topic获取消息
     * 2. 在消息数量不足时继续获取
     * 3. 实现优雅的异步链式调用
     * 4. 避免阻塞和回调地狱
     *
     * 【调用链路】
     * PopConsumerService.popAsync() -> getMessageAsync(recursive) -> 链式调用直到获取足够消息
     *
     * 【终止条件】
     * 1. 流控限制（isPopShouldStop）
     * 2. 顺序消息阻塞（isFifoBlocked）
     * 3. 获取足够消息数量
     *
     * @param future 上一步的异步结果
     * @param clientHost 客户端主机
     * @param groupId 消费组ID
     * @param topicId Topic ID
     * @param queueId 队列ID
     * @param batchSize 批量大小
     * @param filter 消息过滤器
     * @param retryType 重试类型
     * @return 更新后的消费上下文Future
     */
    protected CompletableFuture<PopConsumerContext> getMessageAsync(CompletableFuture<PopConsumerContext> future,
        String clientHost, String groupId, String topicId, int queueId, int batchSize, MessageFilter filter,
        PopConsumerRecord.RetryType retryType) { // 新版 pop kv 获取消息，由 popAsync 调用，popAsync 由 pop processor 调用
        // 【顺序消息】判断是否阻塞的逻辑在这里
        // 从指定的 topic、queue 中获取消息，包括重试队列也可以。除此之外顺序消息也在这里

        return future.thenCompose(result -> {
            // 流控检查：pop请求过多，不应该在这里添加剩余计数
            if (isPopShouldStop(groupId, topicId, queueId)) {
                return CompletableFuture.completedFuture(result);
            }

            // 顺序消息阻塞检查
            // 【设计考虑】当前请求会计算等待过滤的消息总数，用于长轮询服务中的新消息到达通知，
            // 需要忽略顺序消费场景中的积压。如果剩余消息数包括被阻塞队列的积压，
            // 会导致长轮询请求频繁的不必要唤醒，造成不必要的CPU使用。
            // 当客户端确认消息时，长轮询请求会通过AckMessageProcessor.ackOrderly()得到通知，
            // 消息不会被延迟。
            if (result.isFifo() && isFifoBlocked(result, groupId, topicId, queueId)) {
                // 这里不应该添加积压（最大偏移量 - 消费者偏移量）
                return CompletableFuture.completedFuture(result);
            }

            // 计算还需要获取的消息数量
            int remain = batchSize - result.getMessageCount();

            if (remain <= 0) {
                // 已经获取足够的消息，添加剩余计数信息供长轮询使用
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
     * 异步Pop消息的主入口方法 - Pop消费流程的核心控制器
     * <p>
     * 【完整业务流程】
     * 1. 参数验证和锁获取
     * 2. 重试消息和普通消息的获取策略
     * 3. 数据持久化（缓存或存储）
     * 4. 消息重编码（针对重试消息）
     * 5. 资源清理和日志记录
     * <p>
     * 【调用链路】
     * PopMessageProcessor.processRequest() -> PopConsumerService.popAsync() -> 完整的Pop流程
     * <p>
     * 【获取策略】
     * - 每5个请求中有1个优先从重试队列获取（负载均衡）
     * - 支持指定队列和全队列轮询两种模式
     * - 重试消息和普通消息的混合获取
     *
     * @param clientHost 客户端主机
     * @param popTime Pop时间戳
     * @param invisibleTime 不可见时间（毫秒）
     * @param groupId 消费组ID
     * @param topicId Topic ID
     * @param queueId 队列ID（-1表示所有队列）
     * @param batchSize 批量大小
     * @param fifo 是否为顺序消息
     * @param attemptId 尝试ID（用于顺序消息）
     * @param initMode 初始化模式
     * @param filter 消息过滤器
     * @return Pop消费上下文的异步结果
     */
    public CompletableFuture<PopConsumerContext> popAsync(String clientHost, long popTime, long invisibleTime,
        String groupId, String topicId, int queueId, int batchSize, boolean fifo, String attemptId, int initMode,
        MessageFilter filter) { // 新版 kv pop 的主方法，由 processor 调用

        // 创建Pop消费上下文
        PopConsumerContext popConsumerContext =
            new PopConsumerContext(clientHost, popTime, invisibleTime, groupId, fifo, initMode, attemptId);

        // 前置检查：topic配置和获取锁
        TopicConfig topicConfig = brokerController.getTopicConfigManager().selectTopicConfig(topicId);
        if (topicConfig == null || !consumerLockService.tryLock(groupId, topicId)) { // 这里是粗化的锁，锁 groupId 和 topicId，原先是锁 groupId、topicId 以及 queueId
            // 无法获取锁或topic不存在，返回空结果
            return CompletableFuture.completedFuture(popConsumerContext);
        }

        log.debug("PopConsumerService popAsync, groupId={}, topicId={}, queueId={}, " +
                "batchSize={}, invisibleTime={}, fifo={}, attemptId={}, filter={}",
            groupId, topicId, queueId, batchSize, invisibleTime, fifo, attemptId, filter);

        // 构建相关Topic名称
        String requestKey = groupId + "@" + topicId;
        String retryTopicV1 = KeyBuilder.buildPopRetryTopicV1(topicId, groupId);
        String retryTopicV2 = KeyBuilder.buildPopRetryTopicV2(topicId, groupId);

        // 使用请求计数实现负载均衡和重试策略
        long requestCount = Objects.requireNonNull(ConcurrentHashMapUtils.computeIfAbsent(
            requestCountTable, requestKey, k -> new AtomicLong(0L))).getAndIncrement();

        // 每5个请求中有1个优先从重试队列获取消息，平衡重试消息和普通消息
        boolean preferRetry = requestCount % 5L == 0L;

        // 初始化异步链
        CompletableFuture<PopConsumerContext> getMessageFuture =
            CompletableFuture.completedFuture(popConsumerContext);

        try {
            // 阶段1：处理重试消息（如果优先重试且非顺序消息）
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

            // 阶段2：处理普通消息
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

                // 阶段3：如果普通消息获取完毕且不是优先重试，再从重试队列获取
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

            // 阶段4：后处理 - 持久化和编码
            return getMessageFuture.thenCompose(result -> {
                // 如果找到消息且不是顺序消息，需要持久化记录
                if (result.isFound() && !result.isFifo()) {

                    // 选择存储策略：优先使用缓存，缓存满则使用持久化存储
                    if (brokerConfig.isEnablePopBufferMerge() &&
                        popConsumerCache != null && !popConsumerCache.isCacheFull()) {
                        // 写入缓存，提高性能
                        // PopConsumerRecord 包括了 popTime、groupId、topicId、queueId、retryFlag、invisibleTime、offset、attemptId
                        // 以及 RetryType（枚举类型，包括 normal topic、retry topic v1 和 retry topic v2）
                        this.popConsumerCache.writeRecords(result.getPopConsumerRecordList());
                    } else {
                        // 未启用缓存时，直接写入持久化存储
                        this.popConsumerStore.writeRecords(result.getPopConsumerRecordList());
                    }

                    // 处理重试消息的重编码
                    for (int i = 0; i < result.getGetMessageResultList().size(); i++) {
                        GetMessageResult getMessageResult = result.getGetMessageResultList().get(i);
                        PopConsumerRecord popConsumerRecord = result.getPopConsumerRecordList().get(i);

                        // 如果缓冲区属于重试消息，消息需要重新编码。
                        // 当 popResponseReturnActualRetryTopic 为 true 或当前topic不是重试topic时，缓冲区不应该被重新编码。
                        // 默认不重新编码
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
                    // 异常处理和日志记录
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
     * 异步确认消息 - 消息消费完成的标记
     * <p>
     * 【Ack机制的重要性】
     * 1. 标记消息已被成功消费，可以从存储中删除
     * 2. 避免消息重复投递
     * 3. 支持顺序消息的阻塞解除
     * 4. 触发长轮询请求的通知
     *
     * 【调用链路】
     * AckMessageProcessor.processRequest() -> PopConsumerService.ackAsync() -> 删除消息记录
     *
     * 【删除策略】优先从缓存删除，缓存没有再从持久化存储删除
     *
     * 【说明】当接收到顺序确认时通知轮询请求
     *
     * @param popTime Pop时间戳
     * @param invisibleTime 不可见时间
     * @param groupId 消费组ID
     * @param topicId Topic ID
     * @param queueId 队列ID
     * @param offset 消息偏移量
     * @return 确认结果的异步Future
     */
    public CompletableFuture<Boolean> ackAsync(
        long popTime, long invisibleTime, String groupId, String topicId, int queueId, long offset) {

        if (brokerConfig.isPopConsumerKVServiceLog()) {
            log.info("PopConsumerService ack, time={}, invisible={}, groupId={}, topic={}, queueId={}, offset={}",
                popTime, invisibleTime, groupId, topicId, queueId, offset);
        }

        // 构建要删除的记录
        PopConsumerRecord record = new PopConsumerRecord(
            popTime, groupId, topicId, queueId, 0, invisibleTime, offset, null);

        // 优先从缓存中删除，如果缓存中没有再从持久化存储中删除
        if (brokerConfig.isEnablePopBufferMerge() && popConsumerCache != null) {
            if (popConsumerCache.deleteRecords(Collections.singletonList(record)).isEmpty()) {
                // 缓存中删除成功，无需再从持久化存储删除
                return CompletableFuture.completedFuture(true);
            }
        }

        // 从持久化存储中删除
        this.popConsumerStore.deleteRecords(Collections.singletonList(record));
        return CompletableFuture.completedFuture(true);
    }

    /**
     * 修改消息不可见时间 - 动态调整消息处理时间
     *
     * 【功能必要性】
     * 1. 允许消费者延长消息处理时间
     * 2. 支持动态调整消息的超时时间
     * 3. 在消息处理复杂的场景下避免消息超时重试
     *
     * 【调用链路】
     * ChangeInvisibleTimeProcessor.processRequest() -> PopConsumerService.changeInvisibilityDuration()
     *
     * 【操作原子性】先写入新记录，再删除旧记录，确保原子性
     *
     * 【参考】ChangeInvisibleTimeProcessor.appendCheckPointThenAckOrigin
     *
     * @param popTime 原始Pop时间
     * @param invisibleTime 原始不可见时间
     * @param changedPopTime 修改后的Pop时间
     * @param changedInvisibleTime 修改后的不可见时间
     * @param groupId 消费组ID
     * @param topicId Topic ID
     * @param queueId 队列ID
     * @param offset 消息偏移量
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
     * 异步获取消息内容 - 通过EscapeBridge读取消息
     *
     * 【EscapeBridge的作用】
     * 1. 支持远程读取消息内容
     * 2. 统一消息读取接口
     * 3. 支持跨broker的消息访问
     * 4. 提供消息读取的抽象层
     *
     * 【调用链路】
     * PopConsumerService.revive() -> getMessageAsync(record) -> EscapeBridge.getMessageAsync()
     *
     * @param consumerRecord Pop消费记录
     * @return Triple<MessageExt, info, needRetry>, check info and retry if and only if MessageExt is null
     */
    public CompletableFuture<Triple<MessageExt, String, Boolean>> getMessageAsync(PopConsumerRecord consumerRecord) {
        return this.brokerController.getEscapeBridge().getMessageAsync(consumerRecord.getTopicId(),
            consumerRecord.getOffset(), consumerRecord.getQueueId(), brokerConfig.getBrokerName(), false);
    }

    /**
     * 恢复单个消息记录 - 处理超时消息的核心逻辑
     *
     * 【恢复机制的必要性】
     * 1. 处理超时未确认的消息
     * 2. 确保消息最终会被消费
     * 3. 避免消息丢失
     * 4. 支持消息重试机制
     *
     * 【调用链路】
     * PopConsumerService.revive(batch) -> revive(single) -> reviveRetry() -> 投递到重试队列
     *
     * 【恢复策略】
     * - 获取消息内容成功 -> 投递到重试队列
     * - 获取消息失败但不需要重试 -> 标记为成功
     * - 获取消息失败且需要重试 -> 标记为失败
     *
     * @param record Pop消费记录
     * @return 恢复是否成功的异步Future
     */
    public CompletableFuture<Boolean> revive(PopConsumerRecord record) {
        return this.getMessageAsync(record)
            // 与 thenApply 对比，thenApply 连接一个同步方法，thenCompose 连接一个异步方法
            .thenCompose(result -> {
                if (result == null) {
                    log.error("PopConsumerService revive error, message may be lost, record={}", record);
                    return CompletableFuture.completedFuture(false);
                }

                // Triple<MessageExt, info, needRetry>, check info and retry if and only if MessageExt is null
                if (result.getLeft() == null) {
                    log.info("PopConsumerService revive no need retry, record={}", record);
                    return CompletableFuture.completedFuture(!result.getRight());
                }

                // 获取到消息内容，进行重试投递
                return CompletableFuture.completedFuture(this.reviveRetry(record, result.getLeft()));
            });
    }

    /**
     * 清理缓存 - 重置消费状态时的缓存清理
     *
     * 【锁机制的必要性】
     * 1. 防止清理过程中有新的写入操作
     * 2. 确保数据一致性
     * 3. 避免并发冲突
     *
     * 【调用链路】
     * PopConsumerService.getPopOffset() -> clearCache() -> 清理指定队列的缓存
     *
     * 【锁策略】循环获取锁，确保清理操作的原子性
     *
     * @param groupId 消费组ID
     * @param topicId Topic ID
     * @param queueId 队列ID
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
     * 批量恢复过期消息记录 - Pop模式的核心恢复机制
     *
     * 【恢复机制的核心流程】
     * 1. 扫描过期的消息记录
     * 2. 异步处理恢复逻辑
     * 3. 处理失败重试（指数退避）
     * 4. 清理已处理的记录
     * 5. 更新扫描进度
     *
     * 【调用链路】
     * ServiceThread.run() -> PopConsumerService.revive() -> 批量恢复过期消息
     *
     * 【指数退避策略】使用REWRITE_INTERVALS_IN_SECONDS数组实现智能重试
     * 【并发处理】使用CompletableFuture实现异步并发恢复
     *
     * @param currentTime 当前扫描时间（原子更新）
     * @param maxCount 最大处理数量
     * @return 实际恢复的消息数量
     */
    public long revive(AtomicLong currentTime, int maxCount) {
        Stopwatch stopwatch = Stopwatch.createStarted();

        // 设置上限时间，避免处理太新的记录（可能还在处理中）
        // 【设计原理】50ms的缓冲期避免处理正在进行中的消息
        long upperTime = System.currentTimeMillis() - 50L;

        // 扫描过期记录，从上次扫描位置开始
        // 【扫描范围】[currentTime - 3秒, upperTime]
        // Async_question: 为什么这里拿的是 PopConsumerRecord，而不是直接拿 Triple<MessageExt, String, Boolean>
        List<PopConsumerRecord> consumerRecords = this.popConsumerStore.scanExpiredRecords(
            currentTime.get() - TimeUnit.SECONDS.toMillis(3), upperTime, maxCount);
        long scanCostTime = stopwatch.elapsed(TimeUnit.MILLISECONDS);

        // 失败记录队列（用于重试）
        Queue<PopConsumerRecord> failureList = new LinkedBlockingQueue<>();
        List<CompletableFuture<?>> futureList = new ArrayList<>(consumerRecords.size());

        // 这里可以合并读取操作来优化性能
        // 【并发处理】为每个记录创建异步恢复任务
        for (PopConsumerRecord record : consumerRecords) {
            futureList.add(this.revive(record).thenAccept(result -> { // 返回 false 表示需要重试
                if (!result) { // 重试
                    // 恢复失败，根据重试次数决定处理策略
                    if (record.getAttemptTimes() < brokerConfig.getPopReviveMaxAttemptTimes()) {
                        // 使用指数退避算法计算下次重试时间
                        // 【退避策略】根据重试次数选择对应的间隔时间
                        long backoffInterval = 1000L * REWRITE_INTERVALS_IN_SECONDS[
                            Math.min(REWRITE_INTERVALS_IN_SECONDS.length - 1, record.getAttemptTimes())];
                        long nextInvisibleTime = record.getInvisibleTime() + backoffInterval;

                        // 创建重试记录
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

        // 将失败记录重新写入存储（用于下次重试）
        this.popConsumerStore.writeRecords(new ArrayList<>(failureList));

        // 删除已处理的记录
        this.popConsumerStore.deleteRecords(consumerRecords);

        // 更新扫描进度
        // 【进度更新】如果没有记录则使用upperTime，否则使用最后一条记录的可见时间
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
     * 创建重试topic（如果需要的话） - 动态Topic管理
     *
     * 【动态创建的优势】
     * 1. 避免预先创建大量不必要的topic
     * 2. 支持按需创建，节省资源
     * 3. 自动化topic管理
     *
     * 【调用链路】
     * PopConsumerService.reviveRetry() -> createRetryTopicIfNeeded() -> TopicConfigManager.updateTopicConfig()
     *
     * 【Topic配置】1个读队列，1个写队列，支持读写权限，单标签过滤
     *
     * @param groupId 消费组ID
     * @param topicId Topic ID（重试Topic名称）
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
     * 恢复重试消息 - 消息重试投递的核心实现
     *
     * 【重试投递流程】
     * 1. 判断是否已经是重试消息
     * 2. 构建重试topic名称
     * 3. 复制消息内容和属性
     * 4. 增加重试次数
     * 5. 设置首次Pop时间
     * 6. 投递到重试队列
     *
     * 【调用链路】
     * PopConsumerService.revive() -> reviveRetry() -> EscapeBridge.putMessageToSpecificQueue()
     *
     * 【重要属性】PROPERTY_FIRST_POP_TIME用于统计消息的总重试时长
     *
     * @SuppressWarnings("DuplicatedCode")
     * 参考 org.apache.rocketmq.broker.processor.PopReviveService#reviveRetry
     *
     * @param record Pop消费记录
     * @param messageExt 原始消息内容
     * @return 重试是否成功
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

        // 复制基础属性
        msgInner.setBornTimestamp(messageExt.getBornTimestamp());
        msgInner.setFlag(messageExt.getFlag());
        msgInner.setSysFlag(messageExt.getSysFlag());
        msgInner.setBornHost(brokerController.getStoreHost());
        msgInner.setStoreHost(brokerController.getStoreHost());
        msgInner.setReconsumeTimes(messageExt.getReconsumeTimes() + 1); // 增加重试次数
        msgInner.getProperties().putAll(messageExt.getProperties());

        // 设置首次Pop时间，用于重试时长统计
        // 【重要逻辑】只在第一次重试或属性不存在时设置
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
     * 将KV存储记录导出到恢复topic - 数据迁移和备份工具
     *
     * 【功能用途】
     * 1. 支持从KV存储迁移到文件存储
     * 2. 数据备份和恢复
     * 3. 存储格式升级
     * 4. 调试和排查问题
     *
     * 【调用场景】通常用于运维操作或存储升级
     *
     * 【转换过程】PopConsumerRecord -> PopCheckPoint -> MessageExtBrokerInner -> 恢复Topic
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
                    // 构建Pop检查点
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

                    // 选择恢复队列ID（负载均衡）
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
     * 启动服务 - 按依赖顺序启动各个组件
     *
     * 【启动顺序的重要性】
     * 1. 存储必须先启动成功（基础组件）
     * 2. 缓存依赖于存储（依赖关系）
     * 3. 服务线程最后启动（避免未初始化完成就开始工作）
     *
     * 【调用链路】
     * BrokerController.start() -> PopConsumerService.start() -> 各组件依次启动
     */
    @Override
    public void start() {
        if (!this.popConsumerStore.start()) {
            throw new RuntimeException("PopConsumerStore init error");
        }

        if (this.popConsumerCache != null) {
            this.popConsumerCache.start();
        }

        super.start(); // 启动ServiceThread
    }

    /**
     * 关闭服务 - 优雅关闭，确保数据完整性
     *
     * 【优雅关闭的必要性】
     * 1. 等待正在进行的写操作完成
     * 2. 避免数据丢失
     * 3. 正确释放资源
     * 4. 防止内存泄漏
     *
     * 【调用链路】
     * BrokerController.shutdown() -> PopConsumerService.shutdown() -> 各组件依次关闭
     *
     * 【关闭顺序】服务线程 -> 缓存 -> 存储
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
     * 服务主循环 - Pop消费服务的核心调度逻辑
     *
     * 【主循环职责】
     * 1. 定期恢复过期消息（核心功能）
     * 2. 清理过期锁（资源管理）
     * 3. 根据恢复数量调整休眠时间（性能优化）
     * 4. 异常处理和恢复（稳定性保证）
     *
     * 【调用链路】
     * ServiceThread.start() -> PopConsumerService.run() -> 持续运行直到停止
     *
     * 【调度策略】
     * - 有大量过期消息时：连续处理，不休眠
     * - 过期消息较少时：休眠500ms，降低CPU使用率
     * - 异常情况：休眠500ms后重试，避免无限循环
     */
    @Override
    public void run() {
         this.consumerRunning.set(true);

        while (!isStopped()) {
            try {
                // 防止读写操作期间的并发问题
                // 【核心功能】恢复过期消息
                long reviveCount = this.revive(this.currentTime,
                    brokerConfig.getPopReviveMaxReturnSizePerRead());

                long current = System.currentTimeMillis();

                // 每分钟清理一次过期锁，避免内存泄漏
                if (lastCleanupLockTime.get() + TimeUnit.MINUTES.toMillis(1) < current) {
                    this.consumerLockService.removeTimeout();
                    this.lastCleanupLockTime.set(current);
                }

                // 如果恢复的消息数量少于最大值，说明暂时没有更多过期消息，可以休眠
                // 【性能优化】根据工作负载动态调整休眠时间
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
