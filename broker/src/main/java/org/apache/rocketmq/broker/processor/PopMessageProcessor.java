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
package org.apache.rocketmq.broker.processor;

import com.alibaba.fastjson.JSON;
import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.FileRegion;
import io.opentelemetry.api.common.Attributes;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.filter.ConsumerFilterData;
import org.apache.rocketmq.broker.filter.ConsumerFilterManager;
import org.apache.rocketmq.broker.filter.ExpressionMessageFilter;
import org.apache.rocketmq.broker.longpolling.PollingHeader;
import org.apache.rocketmq.broker.longpolling.PollingResult;
import org.apache.rocketmq.broker.longpolling.PopLongPollingService;
import org.apache.rocketmq.broker.longpolling.PopRequest;
import org.apache.rocketmq.broker.metrics.BrokerMetricsManager;
import org.apache.rocketmq.broker.pagecache.ManyMessageTransfer;
import org.apache.rocketmq.broker.pop.PopConsumerContext;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.KeyBuilder;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.PopAckConstants;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.ConsumeInitMode;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.common.utils.ConcurrentHashMapUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.CommandCallback;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.metrics.RemotingMetricsManager;
import org.apache.rocketmq.remoting.netty.NettyRemotingAbstract;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.filter.FilterAPI;
import org.apache.rocketmq.remoting.protocol.header.ExtraInfoUtil;
import org.apache.rocketmq.remoting.protocol.header.PopMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.PopMessageResponseHeader;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.GetMessageStatus;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.exception.ConsumeQueueException;
import org.apache.rocketmq.store.pop.AckMsg;
import org.apache.rocketmq.store.pop.BatchAckMsg;
import org.apache.rocketmq.store.pop.PopCheckPoint;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_CONSUMER_GROUP;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_IS_RETRY;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_IS_SYSTEM;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_TOPIC;
import static org.apache.rocketmq.remoting.metrics.RemotingMetricsConstant.LABEL_REQUEST_CODE;
import static org.apache.rocketmq.remoting.metrics.RemotingMetricsConstant.LABEL_RESPONSE_CODE;
import static org.apache.rocketmq.remoting.metrics.RemotingMetricsConstant.LABEL_RESULT;

/**
 * Pop消息处理器
 * 这是RocketMQ新引入的消费模式处理器，相比于传统的Push/Pull模式，Pop模式具有以下特点：
 * 1. 消息被pop后会变为不可见状态（invisible），避免重复消费
 * 2. 支持消息的自动重试和长轮询机制
 * 3. 支持顺序消息和批量消息处理
 * 4. 提供更好的消费者负载均衡能力
 */
public class PopMessageProcessor implements NettyRequestProcessor {
    /** Pop消息专用日志器 */
    private static final Logger POP_LOGGER = LoggerFactory.getLogger(LoggerName.ROCKETMQ_POP_LOGGER_NAME);
    /** 消息诞生时间字段名 */
    private static final String BORN_TIME = "bornTime";

    /** Broker控制器，用于访问各种管理器和配置 */
    private final BrokerController brokerController;
    /** 随机数生成器，用于队列选择等随机化操作 */
    private final Random random = new Random(System.currentTimeMillis());
    /** 恢复topic名称，用于存储pop消息的检查点信息 */
    private final String reviveTopic;
    /** Pop长轮询服务，处理没有消息时的长轮询等待 */
    private final PopLongPollingService popLongPollingService;
    /** Pop缓冲区合并服务，用于管理pop消息的缓冲和消费进度 */
    private final PopBufferMergeService popBufferMergeService;
    /** 队列锁管理器，防止同一队列被多个消费者同时pop */
    private final QueueLockManager queueLockManager;
    /** 检查点消息编号，用于生成唯一的检查点ID */
    private final AtomicLong ckMessageNumber;

    /**
     * 构造函数
     * @param brokerController Broker控制器
     */
    public PopMessageProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
        // 构建集群恢复topic名称，用于存储pop消息的检查点
        this.reviveTopic = PopAckConstants.buildClusterReviveTopic(
            this.brokerController.getBrokerConfig().getBrokerClusterName());
        // 初始化长轮询服务
        this.popLongPollingService = new PopLongPollingService(brokerController, this, false);
        // 初始化队列锁管理器
        this.queueLockManager = new QueueLockManager();
        // 初始化缓冲区合并服务
        this.popBufferMergeService = new PopBufferMergeService(this.brokerController, this);
        // 初始化检查点消息编号计数器
        this.ckMessageNumber = new AtomicLong();
    }

    /**
     * 获取恢复topic名称
     * @return 恢复topic名称
     */
    protected String getReviveTopic() {
        return reviveTopic;
    }

    /**
     * 获取Pop长轮询服务
     * @return Pop长轮询服务实例
     */
    public PopLongPollingService getPopLongPollingService() {
        return popLongPollingService;
    }

    /**
     * 获取Pop缓冲区合并服务
     * @return Pop缓冲区合并服务实例
     */
    public PopBufferMergeService getPopBufferMergeService() {
        return this.popBufferMergeService;
    }

    /**
     * 获取队列锁管理器
     * @return 队列锁管理器实例
     */
    public QueueLockManager getQueueLockManager() {
        return queueLockManager;
    }

    /**
     * 生成ACK消息的唯一ID
     * @param ackMsg ACK消息对象
     * @return 唯一ID字符串
     */
    public static String genAckUniqueId(AckMsg ackMsg) {
        return ackMsg.getTopic()
            + PopAckConstants.SPLIT + ackMsg.getQueueId()
            + PopAckConstants.SPLIT + ackMsg.getAckOffset()
            + PopAckConstants.SPLIT + ackMsg.getConsumerGroup()
            + PopAckConstants.SPLIT + ackMsg.getPopTime()
            + PopAckConstants.SPLIT + ackMsg.getBrokerName()
            + PopAckConstants.SPLIT + PopAckConstants.ACK_TAG;
    }

    /**
     * 生成批量ACK消息的唯一ID
     * @param batchAckMsg 批量ACK消息对象
     * @return 唯一ID字符串
     */
    public static String genBatchAckUniqueId(BatchAckMsg batchAckMsg) {
        return batchAckMsg.getTopic()
            + PopAckConstants.SPLIT + batchAckMsg.getQueueId()
            + PopAckConstants.SPLIT + batchAckMsg.getAckOffsetList().toString()
            + PopAckConstants.SPLIT + batchAckMsg.getConsumerGroup()
            + PopAckConstants.SPLIT + batchAckMsg.getPopTime()
            + PopAckConstants.SPLIT + PopAckConstants.BATCH_ACK_TAG;
    }

    /**
     * 生成检查点的唯一ID
     * @param ck 检查点对象
     * @return 唯一ID字符串
     */
    public static String genCkUniqueId(PopCheckPoint ck) {
        return ck.getTopic()
            + PopAckConstants.SPLIT + ck.getQueueId()
            + PopAckConstants.SPLIT + ck.getStartOffset()
            + PopAckConstants.SPLIT + ck.getCId()
            + PopAckConstants.SPLIT + ck.getPopTime()
            + PopAckConstants.SPLIT + ck.getBrokerName()
            + PopAckConstants.SPLIT + PopAckConstants.CK_TAG;
    }

    /**
     * 是否拒绝请求
     * @return false，Pop处理器不拒绝任何请求
     */
    @Override
    public boolean rejectRequest() {
        return false;
    }

    /**
     * 获取长轮询映射表
     * @return 长轮询请求映射表
     */
    public ConcurrentLinkedHashMap<String, ConcurrentSkipListSet<PopRequest>> getPollingMap() {
        return popLongPollingService.getPollingMap();
    }

    /**
     * 如果需要的话，通知长轮询请求（简化版本）
     * @param topic 主题名称
     * @param group 消费组名称
     * @param queueId 队列ID
     * @throws ConsumeQueueException 消费队列异常
     */
    public void notifyLongPollingRequestIfNeed(String topic, String group, int queueId) throws ConsumeQueueException {
        this.notifyLongPollingRequestIfNeed(
            topic, group, queueId, null, 0L, null, null);
    }

    /**
     * 如果需要的话，通知长轮询请求（完整版本）
     * 当有新消息到达时，检查是否有等待的长轮询请求，如果有则唤醒它们
     *
     * @param topic 主题名称
     * @param group 消费组名称
     * @param queueId 队列ID
     * @param tagsCode 标签哈希码
     * @param msgStoreTime 消息存储时间
     * @param filterBitMap 过滤位图
     * @param properties 消息属性
     * @throws ConsumeQueueException 消费队列异常
     */
    public void notifyLongPollingRequestIfNeed(String topic, String group, int queueId,
        Long tagsCode, long msgStoreTime, byte[] filterBitMap,
        Map<String, String> properties) throws ConsumeQueueException {
        // 获取pop缓冲区的最新偏移量
        long popBufferOffset = this.brokerController.getPopMessageProcessor().getPopBufferMergeService().getLatestOffset(topic, group, queueId);
        // 获取消费者的消费偏移量
        long consumerOffset = this.brokerController.getConsumerOffsetManager().queryOffset(group, topic, queueId);
        // 获取队列的最大偏移量
        long maxOffset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId);
        // 取两个偏移量的最大值作为当前偏移量
        long offset = Math.max(popBufferOffset, consumerOffset);

        // 如果还有未消费的消息，则通知长轮询请求
        if (maxOffset > offset) {
            // 先尝试通知所有队列的长轮询请求（queueId = -1）
            boolean notifySuccess = popLongPollingService.notifyMessageArriving(
                topic, -1, group, tagsCode, msgStoreTime, filterBitMap, properties);
            if (!notifySuccess) {
                // 如果通知所有队列失败，则通知指定队列的长轮询请求
                notifySuccess = popLongPollingService.notifyMessageArriving(
                    topic, queueId, group, tagsCode, msgStoreTime, filterBitMap, properties);
            }
            // 通知其他处理器有消息到达
            this.brokerController.getNotificationProcessor().notifyMessageArriving(topic, queueId);
            // 记录通知日志
            if (this.brokerController.getBrokerConfig().isEnablePopLog()) {
                POP_LOGGER.info("notify long polling request. topic:{}, group:{}, queueId:{}, success:{}",
                    topic, group, queueId, notifySuccess);
            }
        }
    }

    /**
     * 通知消息到达（用于重试topic）
     * @param topic 主题名称
     * @param queueId 队列ID
     * @param offset 偏移量
     * @param tagsCode 标签哈希码
     * @param msgStoreTime 消息存储时间
     * @param filterBitMap 过滤位图
     * @param properties 消息属性
     */
    public void notifyMessageArriving(final String topic, final int queueId, long offset,
        Long tagsCode, long msgStoreTime, byte[] filterBitMap, Map<String, String> properties) {
        popLongPollingService.notifyMessageArrivingWithRetryTopic(
            topic, queueId, offset, tagsCode, msgStoreTime, filterBitMap, properties);
    }

    /**
     * 通知消息到达（简化版本）
     * @param topic 主题名称
     * @param queueId 队列ID
     * @param cid 客户端ID
     */
    public void notifyMessageArriving(final String topic, final int queueId, final String cid) {
        popLongPollingService.notifyMessageArriving(
            topic, queueId, cid, false, null, 0L, null, null);
    }

    /**
     * 处理Pop消息请求的主入口方法
     * @param ctx 网络通道上下文
     * @param request 远程调用请求
     * @return 响应命令
     * @throws RemotingCommandException 远程调用异常
     */
    @Override
    public RemotingCommand processRequest(final ChannelHandlerContext ctx, RemotingCommand request)
        throws RemotingCommandException {
        final long beginTimeMills = this.brokerController.getMessageStore().now();

        // 填充消息诞生时间到扩展字段（如果不存在的话）
        // 这里的注释问为什么需要这个？是为了跟踪请求的生命周期
        request.addExtFieldIfNotExist(BORN_TIME, String.valueOf(System.currentTimeMillis()));
        if (Objects.equals(request.getExtFields().get(BORN_TIME), "0")) {
            request.addExtField(BORN_TIME, String.valueOf(System.currentTimeMillis()));
        }

        Channel channel = ctx.channel();
        RemotingCommand response = RemotingCommand.createResponseCommand(PopMessageResponseHeader.class);
        response.setOpaque(request.getOpaque());

        // 解析请求头和响应头
        final PopMessageRequestHeader requestHeader =
            request.decodeCommandCustomHeader(PopMessageRequestHeader.class, true);
        final PopMessageResponseHeader responseHeader = (PopMessageResponseHeader) response.readCustomHeader();

        // Pop模式只支持集群负载均衡模式的消费
        brokerController.getConsumerManager().compensateBasicConsumerInfo(
            requestHeader.getConsumerGroup(), ConsumeType.CONSUME_POP, MessageModel.CLUSTERING);

        // 记录接收到的Pop请求日志
        if (brokerController.getBrokerConfig().isEnablePopLog()) {
            POP_LOGGER.info("receive PopMessage request command, {}", request);
        }

        // 检查请求是否超时过多
        if (requestHeader.isTimeoutTooMuch()) {
            response.setCode(ResponseCode.POLLING_TIMEOUT);
            response.setRemark(String.format("the broker[%s] pop message is timeout too much",
                this.brokerController.getBrokerConfig().getBrokerIP1()));
            return response;
        }

        // 检查broker是否有读权限
        if (!PermName.isReadable(this.brokerController.getBrokerConfig().getBrokerPermission())) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark(String.format("the broker[%s] pop message is forbidden",
                this.brokerController.getBrokerConfig().getBrokerIP1()));
            return response;
        }

        // 检查请求的消息数量是否超过限制（最大32条）
        if (requestHeader.getMaxMsgNums() > 32) {
            response.setCode(ResponseCode.INVALID_PARAMETER);
            response.setRemark(String.format("the broker[%s] pop message's num is greater than 32",
                this.brokerController.getBrokerConfig().getBrokerIP1()));
            return response;
        }

        // 检查定时轮是否启用（Pop模式需要定时轮支持）
        if (!brokerController.getMessageStore().getMessageStoreConfig().isTimerWheelEnable()) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(String.format("the broker[%s] pop message is forbidden because timerWheelEnable is false",
                this.brokerController.getBrokerConfig().getBrokerIP1()));
            return response;
        }

        // 检查topic是否存在
        TopicConfig topicConfig =
            this.brokerController.getTopicConfigManager().selectTopicConfig(requestHeader.getTopic());
        if (null == topicConfig) {
            POP_LOGGER.error("The topic {} not exist, consumer: {} ", requestHeader.getTopic(),
                RemotingHelper.parseChannelRemoteAddr(channel));
            response.setCode(ResponseCode.TOPIC_NOT_EXIST);
            response.setRemark(String.format("topic[%s] not exist, apply first please! %s", requestHeader.getTopic(),
                FAQUrl.suggestTodo(FAQUrl.APPLY_TOPIC_URL)));
            return response;
        }

        // 检查topic是否有读权限
        if (!PermName.isReadable(topicConfig.getPerm())) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark("the topic[" + requestHeader.getTopic() + "] peeking message is forbidden");
            return response;
        }

        // 检查队列ID是否合法
        if (requestHeader.getQueueId() >= topicConfig.getReadQueueNums()) {
            String errorInfo = String.format("queueId[%d] is illegal, topic:[%s] topicConfig.readQueueNums:[%d] " +
                    "consumer:[%s]",
                requestHeader.getQueueId(), requestHeader.getTopic(), topicConfig.getReadQueueNums(),
                channel.remoteAddress());
            POP_LOGGER.warn(errorInfo);
            response.setCode(ResponseCode.INVALID_PARAMETER);
            response.setRemark(errorInfo);
            return response;
        }

        // 检查订阅组是否存在
        SubscriptionGroupConfig subscriptionGroupConfig =
            this.brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(requestHeader.getConsumerGroup());
        if (null == subscriptionGroupConfig) {
            response.setCode(ResponseCode.SUBSCRIPTION_GROUP_NOT_EXIST);
            response.setRemark(String.format("subscription group [%s] does not exist, %s",
                requestHeader.getConsumerGroup(), FAQUrl.suggestTodo(FAQUrl.SUBSCRIPTION_GROUP_NOT_EXIST)));
            return response;
        }

        // 检查订阅组是否允许消费
        if (!subscriptionGroupConfig.isConsumeEnable()) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark("subscription group no permission, " + requestHeader.getConsumerGroup());
            return response;
        }

        BrokerConfig brokerConfig = brokerController.getBrokerConfig();
        SubscriptionData subscriptionData = null;
        ExpressionMessageFilter messageFilter = null;

        // 处理消息过滤表达式
        if (requestHeader.getExp() != null && !requestHeader.getExp().isEmpty()) {
            try {
                // 为原始topic构建订阅数据
                subscriptionData = FilterAPI.build(
                    requestHeader.getTopic(), requestHeader.getExp(), requestHeader.getExpType());
                brokerController.getConsumerManager().compensateSubscribeData(
                    requestHeader.getConsumerGroup(), requestHeader.getTopic(), subscriptionData);

                // 为重试topic构建订阅数据
                String retryTopic = KeyBuilder.buildPopRetryTopic(
                    requestHeader.getTopic(), requestHeader.getConsumerGroup(), brokerConfig.isEnableRetryTopicV2());
                SubscriptionData retrySubscriptionData = FilterAPI.build(
                    retryTopic, SubscriptionData.SUB_ALL, requestHeader.getExpType());
                brokerController.getConsumerManager().compensateSubscribeData(
                    requestHeader.getConsumerGroup(), retryTopic, retrySubscriptionData);

                // 构建消费者过滤数据（用于非标签类型的过滤）
                ConsumerFilterData consumerFilterData = null;
                if (!ExpressionType.isTagType(subscriptionData.getExpressionType())) {
                    consumerFilterData = ConsumerFilterManager.build(
                        requestHeader.getTopic(), requestHeader.getConsumerGroup(), requestHeader.getExp(),
                        requestHeader.getExpType(), System.currentTimeMillis());
                    if (consumerFilterData == null) {
                        POP_LOGGER.warn("Parse the consumer's subscription[{}] failed, group: {}",
                            requestHeader.getExp(), requestHeader.getConsumerGroup());
                        response.setCode(ResponseCode.SUBSCRIPTION_PARSE_FAILED);
                        response.setRemark("parse the consumer's subscription failed");
                        return response;
                    }
                }

                // 创建表达式消息过滤器
                messageFilter = new ExpressionMessageFilter(
                    subscriptionData, consumerFilterData, brokerController.getConsumerFilterManager());
            } catch (Exception e) {
                POP_LOGGER.warn("Parse the consumer's subscription[{}] error, group: {}", requestHeader.getExp(),
                    requestHeader.getConsumerGroup());
                response.setCode(ResponseCode.SUBSCRIPTION_PARSE_FAILED);
                response.setRemark("parse the consumer's subscription failed");
                return response;
            }
        } else {
            // 如果没有指定过滤表达式，使用默认的"*"（匹配所有）
            try {
                // 为原始topic构建默认订阅数据
                subscriptionData = FilterAPI.build(requestHeader.getTopic(), "*", ExpressionType.TAG);
                brokerController.getConsumerManager().compensateSubscribeData(
                    requestHeader.getConsumerGroup(), requestHeader.getTopic(), subscriptionData);

                // 为重试topic构建默认订阅数据
                String retryTopic = KeyBuilder.buildPopRetryTopic(
                    requestHeader.getTopic(), requestHeader.getConsumerGroup(), brokerConfig.isEnableRetryTopicV2());
                SubscriptionData retrySubscriptionData = FilterAPI.build(retryTopic, "*", ExpressionType.TAG);
                brokerController.getConsumerManager().compensateSubscribeData(
                    requestHeader.getConsumerGroup(), retryTopic, retrySubscriptionData);
            } catch (Exception e) {
                POP_LOGGER.warn("Build default subscription error, group: {}", requestHeader.getConsumerGroup());
            }
        }

        // 创建获取消息结果对象
        GetMessageResult getMessageResult = new GetMessageResult(requestHeader.getMaxMsgNums());
        ExpressionMessageFilter finalMessageFilter = messageFilter;
        SubscriptionData finalSubscriptionData = subscriptionData;

        // 如果启用了Pop消费者KV服务，使用异步方式处理
        if (brokerConfig.isPopConsumerKVServiceEnable()) {
            CompletableFuture<PopConsumerContext> popAsyncFuture = brokerController.getPopConsumerService().popAsync(
                RemotingHelper.parseChannelRemoteAddr(channel), beginTimeMills, requestHeader.getInvisibleTime(),
                requestHeader.getConsumerGroup(), requestHeader.getTopic(), requestHeader.getQueueId(),
                requestHeader.getMaxMsgNums(), requestHeader.isOrder(),
                requestHeader.getAttemptId(), requestHeader.getInitMode(), messageFilter);

            // 异步处理结果
            popAsyncFuture.thenApply(result -> {
                if (result.isFound()) {
                    // 找到消息
                    response.setCode(ResponseCode.SUCCESS);
                    getMessageResult.setStatus(GetMessageStatus.FOUND);
                    // 如果还有剩余消息，递归处理（通知长轮询）
                    if (result.getRestCount() > 0) {
                        popLongPollingService.notifyMessageArriving(
                            requestHeader.getTopic(), requestHeader.getQueueId(), requestHeader.getConsumerGroup(),
                            null, 0L, null, null);
                    }
                } else {
                    // 没有找到消息，尝试长轮询
                    POP_LOGGER.debug("Processor not found, polling request, popTime={}, restCount={}",
                        result.getPopTime(), result.getRestCount());
                    PollingResult pollingResult = popLongPollingService.polling(
                        ctx, request, new PollingHeader(requestHeader), finalSubscriptionData, finalMessageFilter);
                    if (PollingResult.POLLING_SUC == pollingResult) {
                        // 长轮询成功，递归处理
                        if (result.getRestCount() > 0) {
                            popLongPollingService.notifyMessageArriving(
                                requestHeader.getTopic(), requestHeader.getQueueId(), requestHeader.getConsumerGroup(),
                                null, 0L, null, null);
                        }
                        return null;
                    } else if (PollingResult.POLLING_FULL == pollingResult) {
                        response.setCode(ResponseCode.POLLING_FULL);
                    } else {
                        response.setCode(ResponseCode.POLLING_TIMEOUT);
                    }
                    getMessageResult.setStatus(GetMessageStatus.NO_MESSAGE_IN_QUEUE);
                }

                // 设置响应头信息
                responseHeader.setPopTime(result.getPopTime());
                responseHeader.setInvisibleTime(result.getInvisibleTime());
                responseHeader.setReviveQid(
                    requestHeader.isOrder() ? KeyBuilder.POP_ORDER_REVIVE_QUEUE : 0);
                responseHeader.setRestNum(result.getRestCount());
                responseHeader.setStartOffsetInfo(result.getStartOffsetInfo());
                responseHeader.setMsgOffsetInfo(result.getMsgOffsetInfo());
                if (requestHeader.isOrder() && !result.getOrderCountInfo().isEmpty()) {
                    responseHeader.setOrderCountInfo(result.getOrderCountInfo());
                }
                response.setRemark(getMessageResult.getStatus().name());

                if (response.getCode() != ResponseCode.SUCCESS) {
                    return response;
                }

                // 添加消息到结果中
                result.getGetMessageResultList().forEach(temp -> {
                    for (int i = 0; i < temp.getMessageMapedList().size(); i++) {
                        getMessageResult.addMessage(temp.getMessageMapedList().get(i));
                    }
                });

                // 根据配置选择消息传输方式
                if (this.brokerController.getBrokerConfig().isTransferMsgByHeap()) {
                    // 通过堆内存传输消息
                    final byte[] r = this.readGetMessageResult(getMessageResult,
                        requestHeader.getConsumerGroup(), requestHeader.getTopic(), requestHeader.getQueueId());
                    this.brokerController.getBrokerStatsManager().incGroupGetLatency(
                        requestHeader.getConsumerGroup(), requestHeader.getTopic(), requestHeader.getQueueId(),
                        (int) (this.brokerController.getMessageStore().now() - beginTimeMills));
                    response.setBody(r);
                } else {
                    // 通过文件传输消息（零拷贝）
                    final GetMessageResult tmpGetMessageResult = getMessageResult;
                    try {
                        FileRegion fileRegion = new ManyMessageTransfer(
                            response.encodeHeader(getMessageResult.getBufferTotalSize()), getMessageResult);
                        channel.writeAndFlush(fileRegion)
                            .addListener((ChannelFutureListener) future -> {
                                // 释放资源并记录指标
                                tmpGetMessageResult.release();
                                Attributes attributes = RemotingMetricsManager.newAttributesBuilder()
                                    .put(LABEL_REQUEST_CODE, RemotingHelper.getRequestCodeDesc(request.getCode()))
                                    .put(LABEL_RESPONSE_CODE, RemotingHelper.getResponseCodeDesc(response.getCode()))
                                    .put(LABEL_RESULT, RemotingMetricsManager.getWriteAndFlushResult(future))
                                    .build();
                                RemotingMetricsManager.rpcLatency.record(
                                    request.getProcessTimer().elapsed(TimeUnit.MILLISECONDS), attributes);
                                if (!future.isSuccess()) {
                                    POP_LOGGER.error("Fail to transfer messages from page cache to {}",
                                        channel.remoteAddress(), future.cause());
                                }
                            });
                    } catch (Throwable e) {
                        POP_LOGGER.error("Error occurred when transferring messages from page cache", e);
                        getMessageResult.release();
                    }
                    return null;
                }
                return response;
            }).thenAccept(result -> NettyRemotingAbstract.writeResponse(channel, request, result));
            return null;
        }

        // 传统的同步处理方式
        int randomQ = random.nextInt(100);
        int reviveQid;
        if (requestHeader.isOrder()) {
            // 顺序消息使用固定的恢复队列ID
            reviveQid = KeyBuilder.POP_ORDER_REVIVE_QUEUE;
        } else {
            // 普通消息使用轮转的方式分配恢复队列ID
            reviveQid = (int) Math.abs(ckMessageNumber.getAndIncrement() %
                this.brokerController.getBrokerConfig().getReviveQueueNum());
        }

        // 用于构建响应中的偏移量信息
        StringBuilder startOffsetInfo = new StringBuilder(64);
        StringBuilder msgOffsetInfo = new StringBuilder(64);
        StringBuilder orderCountInfo = requestHeader.isOrder() ? new StringBuilder(64) : null;

        // 由于startOffsetInfo、msgOffsetInfo和orderCountInfo字段的设计，
        // 单个POP请求只能对普通topic或重试topic的队列调用一次popMsgFromQueue方法。
        // 重试topic v1和v2被认为是同一类型，因为它们在前面的字段中共享相同的重试标志。
        // 因此，needRetryV1被设计为needRetry的子集，在单个请求中，
        // 只有一种类型的重试topic能够调用popMsgFromQueue。
        boolean needRetry = randomQ < brokerConfig.getPopFromRetryProbability();
        boolean needRetryV1 = false;
        if (brokerConfig.isEnableRetryTopicV2() && brokerConfig.isRetrieveMessageFromPopRetryTopicV1()) {
            needRetryV1 = randomQ % 2 == 0;
        }

        long popTime = System.currentTimeMillis();
        CompletableFuture<Long> getMessageFuture = CompletableFuture.completedFuture(0L);

        // 如果需要重试且不是顺序消息，先从重试topic获取消息
        if (needRetry && !requestHeader.isOrder()) {
            if (needRetryV1) {
                String retryTopic = KeyBuilder.buildPopRetryTopicV1(requestHeader.getTopic(), requestHeader.getConsumerGroup());
                getMessageFuture = popMsgFromTopic(retryTopic, true, getMessageResult, requestHeader, reviveQid, channel,
                    popTime, finalMessageFilter, startOffsetInfo, msgOffsetInfo, orderCountInfo, randomQ, getMessageFuture);
            } else {
                String retryTopic = KeyBuilder.buildPopRetryTopic(requestHeader.getTopic(), requestHeader.getConsumerGroup(), brokerConfig.isEnableRetryTopicV2());
                getMessageFuture = popMsgFromTopic(retryTopic, true, getMessageResult, requestHeader, reviveQid, channel,
                    popTime, finalMessageFilter, startOffsetInfo, msgOffsetInfo, orderCountInfo, randomQ, getMessageFuture);
            }
        }

        // 从原始topic获取消息
        if (requestHeader.getQueueId() < 0) {
            // 读取所有队列
            getMessageFuture = popMsgFromTopic(topicConfig, false, getMessageResult, requestHeader, reviveQid, channel,
                popTime, finalMessageFilter, startOffsetInfo, msgOffsetInfo, orderCountInfo, randomQ, getMessageFuture);
        } else {
            // 读取指定队列
            int queueId = requestHeader.getQueueId();
            getMessageFuture = getMessageFuture.thenCompose(restNum ->
                popMsgFromQueue(topicConfig.getTopicName(), requestHeader.getAttemptId(), false,
                    getMessageResult, requestHeader, queueId, restNum, reviveQid, channel, popTime, finalMessageFilter,
                    startOffsetInfo, msgOffsetInfo, orderCountInfo));
        }

        // 如果消息数量不足且之前没有尝试重试，再次尝试从重试topic获取消息
        if (!needRetry && getMessageResult.getMessageMapedList().size() < requestHeader.getMaxMsgNums() && !requestHeader.isOrder()) {
            if (needRetryV1) {
                String retryTopicV1 = KeyBuilder.buildPopRetryTopicV1(requestHeader.getTopic(), requestHeader.getConsumerGroup());
                getMessageFuture = popMsgFromTopic(retryTopicV1, true, getMessageResult, requestHeader, reviveQid, channel,
                    popTime, finalMessageFilter, startOffsetInfo, msgOffsetInfo, orderCountInfo, randomQ, getMessageFuture);
            } else {
                String retryTopic = KeyBuilder.buildPopRetryTopic(requestHeader.getTopic(), requestHeader.getConsumerGroup(), brokerConfig.isEnableRetryTopicV2());
                getMessageFuture = popMsgFromTopic(retryTopic, true, getMessageResult, requestHeader, reviveQid, channel,
                    popTime, finalMessageFilter, startOffsetInfo, msgOffsetInfo, orderCountInfo, randomQ, getMessageFuture);
            }
        }

        final RemotingCommand finalResponse = response;
        // 处理异步获取消息的结果
        getMessageFuture.thenApply(restNum -> {
            try {
                // 执行回调函数
                if (request.getCallbackList() != null) {
                    request.getCallbackList().forEach(CommandCallback::accept);
                    request.getCallbackList().clear();
                }
            } catch (Throwable t) {
                POP_LOGGER.error("PopProcessor execute callback error", t);
            }

            if (!getMessageResult.getMessageBufferList().isEmpty()) {
                // 找到消息
                finalResponse.setCode(ResponseCode.SUCCESS);
                getMessageResult.setStatus(GetMessageStatus.FOUND);
                if (restNum > 0) {
                    // 全队列pop不能通知指定队列pop，反之亦然
                    popLongPollingService.notifyMessageArriving(
                        requestHeader.getTopic(), requestHeader.getQueueId(), requestHeader.getConsumerGroup(),
                        null, 0L, null, null);
                }
            } else {
                // 没有找到消息，尝试长轮询
                PollingResult pollingResult = popLongPollingService.polling(
                    ctx, request, new PollingHeader(requestHeader), finalSubscriptionData, finalMessageFilter);
                if (PollingResult.POLLING_SUC == pollingResult) {
                    if (restNum > 0) {
                        popLongPollingService.notifyMessageArriving(
                            requestHeader.getTopic(), requestHeader.getQueueId(), requestHeader.getConsumerGroup(),
                            null, 0L, null, null);
                    }
                    return null;
                } else if (PollingResult.POLLING_FULL == pollingResult) {
                    finalResponse.setCode(ResponseCode.POLLING_FULL);
                } else {
                    finalResponse.setCode(ResponseCode.POLLING_TIMEOUT);
                }
                getMessageResult.setStatus(GetMessageStatus.NO_MESSAGE_IN_QUEUE);
            }

            // 设置响应头信息
            responseHeader.setInvisibleTime(requestHeader.getInvisibleTime());
            responseHeader.setPopTime(popTime);
            responseHeader.setReviveQid(reviveQid);
            responseHeader.setRestNum(restNum);
            responseHeader.setStartOffsetInfo(startOffsetInfo.toString());
            responseHeader.setMsgOffsetInfo(msgOffsetInfo.toString());
            if (requestHeader.isOrder() && orderCountInfo != null) {
                responseHeader.setOrderCountInfo(orderCountInfo.toString());
            }
            finalResponse.setRemark(getMessageResult.getStatus().name());

            switch (finalResponse.getCode()) {
                case ResponseCode.SUCCESS:
                    // 根据配置选择消息传输方式
                    if (this.brokerController.getBrokerConfig().isTransferMsgByHeap()) {
                        // 通过堆内存传输
                        final byte[] r = this.readGetMessageResult(getMessageResult, requestHeader.getConsumerGroup(),
                            requestHeader.getTopic(), requestHeader.getQueueId());
                        this.brokerController.getBrokerStatsManager().incGroupGetLatency(requestHeader.getConsumerGroup(),
                            requestHeader.getTopic(), requestHeader.getQueueId(),
                            (int) (this.brokerController.getMessageStore().now() - beginTimeMills));
                        finalResponse.setBody(r);
                    } else {
                        // 通过文件传输（零拷贝）
                        final GetMessageResult tmpGetMessageResult = getMessageResult;
                        try {
                            FileRegion fileRegion =
                                new ManyMessageTransfer(finalResponse.encodeHeader(getMessageResult.getBufferTotalSize()),
                                    getMessageResult);
                            channel.writeAndFlush(fileRegion)
                                .addListener((ChannelFutureListener) future -> {
                                    // 释放资源并记录指标
                                    tmpGetMessageResult.release();
                                    Attributes attributes = RemotingMetricsManager.newAttributesBuilder()
                                        .put(LABEL_REQUEST_CODE, RemotingHelper.getRequestCodeDesc(request.getCode()))
                                        .put(LABEL_RESPONSE_CODE, RemotingHelper.getResponseCodeDesc(finalResponse.getCode()))
                                        .put(LABEL_RESULT, RemotingMetricsManager.getWriteAndFlushResult(future))
                                        .build();
                                    RemotingMetricsManager.rpcLatency.record(request.getProcessTimer().elapsed(TimeUnit.MILLISECONDS), attributes);
                                    if (!future.isSuccess()) {
                                        POP_LOGGER.error("Fail to transfer messages from page cache to {}",
                                            channel.remoteAddress(), future.cause());
                                    }
                                });
                        } catch (Throwable e) {
                            POP_LOGGER.error("Error occurred when transferring messages from page cache", e);
                            getMessageResult.release();
                        }
                        return null;
                    }
                    break;
                default:
                    return finalResponse;
            }
            return finalResponse;
        }).thenAccept(result -> NettyRemotingAbstract.writeResponse(channel, request, result));
        return null;
    }

    /**
     * 从指定topic的所有队列中pop消息
     * @param topicConfig topic配置
     * @param isRetry 是否是重试topic
     * @param getMessageResult 获取消息结果对象
     * @param requestHeader 请求头
     * @param reviveQid 恢复队列ID
     * @param channel 网络通道
     * @param popTime pop时间
     * @param messageFilter 消息过滤器
     * @param startOffsetInfo 起始偏移量信息
     * @param msgOffsetInfo 消息偏移量信息
     * @param orderCountInfo 顺序消息计数信息
     * @param randomQ 随机数（用于队列选择）
     * @param getMessageFuture 获取消息的Future
     * @return 返回剩余消息数量的Future
     */
    private CompletableFuture<Long> popMsgFromTopic(TopicConfig topicConfig, boolean isRetry, GetMessageResult getMessageResult,
        PopMessageRequestHeader requestHeader, int reviveQid, Channel channel, long popTime,
        ExpressionMessageFilter messageFilter, StringBuilder startOffsetInfo,
        StringBuilder msgOffsetInfo, StringBuilder orderCountInfo, int randomQ, CompletableFuture<Long> getMessageFuture) {
        if (topicConfig != null) {
            // 遍历所有读队列
            for (int i = 0; i < topicConfig.getReadQueueNums(); i++) {
                // 使用随机数确定队列选择的起始位置，避免总是从0开始
                int queueId = (randomQ + i) % topicConfig.getReadQueueNums();
                getMessageFuture = getMessageFuture.thenCompose(restNum ->
                    popMsgFromQueue(topicConfig.getTopicName(), requestHeader.getAttemptId(), isRetry,
                        getMessageResult, requestHeader, queueId, restNum, reviveQid, channel, popTime, messageFilter,
                        startOffsetInfo, msgOffsetInfo, orderCountInfo));
            }
        }
        return getMessageFuture;
    }

    /**
     * 从指定topic的所有队列中pop消息（通过topic名称）
     * @param topic topic名称
     * @param isRetry 是否是重试topic
     * @param getMessageResult 获取消息结果对象
     * @param requestHeader 请求头
     * @param reviveQid 恢复队列ID
     * @param channel 网络通道
     * @param popTime pop时间
     * @param messageFilter 消息过滤器
     * @param startOffsetInfo 起始偏移量信息
     * @param msgOffsetInfo 消息偏移量信息
     * @param orderCountInfo 顺序消息计数信息
     * @param randomQ 随机数（用于队列选择）
     * @param getMessageFuture 获取消息的Future
     * @return 返回剩余消息数量的Future
     */
    private CompletableFuture<Long> popMsgFromTopic(String topic, boolean isRetry, GetMessageResult getMessageResult,
        PopMessageRequestHeader requestHeader, int reviveQid, Channel channel, long popTime,
        ExpressionMessageFilter messageFilter, StringBuilder startOffsetInfo,
        StringBuilder msgOffsetInfo, StringBuilder orderCountInfo, int randomQ, CompletableFuture<Long> getMessageFuture) {
        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(topic);
        return popMsgFromTopic(topicConfig, isRetry, getMessageResult, requestHeader, reviveQid, channel, popTime,
            messageFilter, startOffsetInfo, msgOffsetInfo, orderCountInfo, randomQ, getMessageFuture);
    }

    /**
     * 从指定队列中pop消息的核心方法
     * @param topic topic名称
     * @param attemptId 尝试ID（用于顺序消息）
     * @param isRetry 是否是重试topic
     * @param getMessageResult 获取消息结果对象
     * @param requestHeader 请求头
     * @param queueId 队列ID
     * @param restNum 剩余消息数量
     * @param reviveQid 恢复队列ID
     * @param channel 网络通道
     * @param popTime pop时间
     * @param messageFilter 消息过滤器
     * @param startOffsetInfo 起始偏移量信息
     * @param msgOffsetInfo 消息偏移量信息
     * @param orderCountInfo 顺序消息计数信息
     * @return 返回剩余消息数量的Future
     */
    private CompletableFuture<Long> popMsgFromQueue(String topic, String attemptId, boolean isRetry,
        GetMessageResult getMessageResult,
        PopMessageRequestHeader requestHeader, int queueId, long restNum, int reviveQid,
        Channel channel, long popTime, ExpressionMessageFilter messageFilter, StringBuilder startOffsetInfo,
        StringBuilder msgOffsetInfo, StringBuilder orderCountInfo) {

        // 构建队列锁的键
        String lockKey =
            topic + PopAckConstants.SPLIT + requestHeader.getConsumerGroup() + PopAckConstants.SPLIT + queueId;
        boolean isOrder = requestHeader.isOrder();
        long offset;

        // 获取pop偏移量
        try {
            offset = getPopOffset(topic, requestHeader.getConsumerGroup(), queueId, requestHeader.getInitMode(),
                false, lockKey, false);
        } catch (ConsumeQueueException e) {
            CompletableFuture<Long> failure = new CompletableFuture<>();
            failure.completeExceptionally(e);
            return failure;
        }

        CompletableFuture<Long> future = new CompletableFuture<>();

        // 尝试获取队列锁
        if (!queueLockManager.tryLock(lockKey)) {
            // 获取锁失败，直接返回剩余消息数量
            try {
                if (!requestHeader.isOrder()) {
                    restNum = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId) - offset + restNum;
                }
                future.complete(restNum);
            } catch (ConsumeQueueException e) {
                future.completeExceptionally(e);
            }
            return future;
        }

        // 确保在Future完成时释放锁
        future.whenComplete((result, throwable) -> queueLockManager.unLock(lockKey));

        // 检查是否应该停止pop（因为未确认消息过多）
        if (isPopShouldStop(topic, requestHeader.getConsumerGroup(), queueId)) {
            POP_LOGGER.warn("Too much msgs unacked, then stop popping. topic={}, group={}, queueId={}",
                topic, requestHeader.getConsumerGroup(), queueId);
            try {
                restNum = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId) - offset + restNum;
                future.complete(restNum);
            } catch (ConsumeQueueException e) {
                future.completeExceptionally(e);
            }
            return future;
        }

        try {
            // 重新获取pop偏移量（可能包括重置检查）
            offset = getPopOffset(topic, requestHeader.getConsumerGroup(), queueId, requestHeader.getInitMode(),
                true, lockKey, true);

            // 当前请求会计算等待过滤的消息总数，用于长轮询服务中的新消息到达通知，
            // 需要忽略顺序消费场景中的积压。如果剩余消息数包括被阻塞队列的积压，
            // 会导致长轮询请求频繁的不必要唤醒，造成不必要的CPU使用。
            // 当客户端确认消息时，长轮询请求会通过AckMessageProcessor.ackOrderly()得到通知，
            // 消息不会被延迟。
            if (isOrder) {
                if (brokerController.getConsumerOrderInfoManager().checkBlock(
                    attemptId, topic, requestHeader.getConsumerGroup(), queueId, requestHeader.getInvisibleTime())) {
                    // 这里不应该添加积压（最大偏移量 - 消费者偏移量）
                    future.complete(restNum);
                    return future;
                }
                // 清除飞行中消息数量
                this.brokerController.getPopInflightMessageCounter().clearInFlightMessageNum(
                    topic, requestHeader.getConsumerGroup(), queueId);
            }

            // 如果已经获取到足够的消息，直接返回
            if (getMessageResult.getMessageMapedList().size() >= requestHeader.getMaxMsgNums()) {
                restNum = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId) - offset + restNum;
                future.complete(restNum);
                return future;
            }
        } catch (Exception e) {
            POP_LOGGER.error("Exception in popMsgFromQueue", e);
            future.complete(restNum);
            return future;
        }

        // 使用原子类处理并发访问
        AtomicLong atomicRestNum = new AtomicLong(restNum);
        AtomicLong atomicOffset = new AtomicLong(offset);
        long finalOffset = offset;

        // 异步从存储中获取消息
        return this.brokerController.getMessageStore()
            .getMessageAsync(requestHeader.getConsumerGroup(), topic, queueId, offset,
                requestHeader.getMaxMsgNums() - getMessageResult.getMessageMapedList().size(), messageFilter)
            .thenCompose(result -> {
                if (result == null) {
                    return CompletableFuture.completedFuture(null);
                }

                // 处理偏移量不正确的情况
                if (GetMessageStatus.OFFSET_TOO_SMALL.equals(result.getStatus())
                    || GetMessageStatus.OFFSET_OVERFLOW_BADLY.equals(result.getStatus())
                    || GetMessageStatus.OFFSET_FOUND_NULL.equals(result.getStatus())) {
                    // 提交偏移量，因为偏移量不正确
                    // 如果存储中的偏移量大于cq偏移量，会导致重复消息，
                    // 因为PopBuffer中的偏移量没有被提交。
                    POP_LOGGER.warn("Pop initial offset, because store is no correct, {}, {}->{}",
                        lockKey, atomicOffset.get(), result.getNextBeginOffset());
                    this.brokerController.getConsumerOffsetManager().commitOffset(channel.remoteAddress().toString(), requestHeader.getConsumerGroup(), topic,
                        queueId, result.getNextBeginOffset());
                    atomicOffset.set(result.getNextBeginOffset());
                    // 使用正确的偏移量重新获取消息
                    return this.brokerController.getMessageStore().getMessageAsync(requestHeader.getConsumerGroup(), topic, queueId, atomicOffset.get(),
                        requestHeader.getMaxMsgNums() - getMessageResult.getMessageMapedList().size(), messageFilter);
                }
                return CompletableFuture.completedFuture(result);
            }).thenApply(result -> {
                if (result == null) {
                    // 计算剩余消息数量
                    try {
                        atomicRestNum.set(brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId) - atomicOffset.get() + atomicRestNum.get());
                    } catch (ConsumeQueueException e) {
                        POP_LOGGER.error("Failed to get max offset in queue", e);
                    }
                    return atomicRestNum.get();
                }

                if (!result.getMessageMapedList().isEmpty()) {
                    // 更新统计信息
                    this.brokerController.getBrokerStatsManager().incBrokerGetNums(requestHeader.getTopic(), result.getMessageCount());
                    this.brokerController.getBrokerStatsManager().incGroupGetNums(requestHeader.getConsumerGroup(), topic,
                        result.getMessageCount());
                    this.brokerController.getBrokerStatsManager().incGroupGetSize(requestHeader.getConsumerGroup(), topic,
                        result.getBufferTotalSize());

                    // 更新指标
                    Attributes attributes = BrokerMetricsManager.newAttributesBuilder()
                        .put(LABEL_TOPIC, requestHeader.getTopic())
                        .put(LABEL_CONSUMER_GROUP, requestHeader.getConsumerGroup())
                        .put(LABEL_IS_SYSTEM, TopicValidator.isSystemTopic(requestHeader.getTopic()) || MixAll.isSysConsumerGroup(requestHeader.getConsumerGroup()))
                        .put(LABEL_IS_RETRY, isRetry)
                        .build();
                    BrokerMetricsManager.messagesOutTotal.add(result.getMessageCount(), attributes);
                    BrokerMetricsManager.throughputOutTotal.add(result.getBufferTotalSize(), attributes);

                    if (isOrder) {
                        // 顺序消息的特殊处理
                        this.brokerController.getConsumerOrderInfoManager().update(requestHeader.getAttemptId(), isRetry, topic,
                            requestHeader.getConsumerGroup(),
                            queueId, popTime, requestHeader.getInvisibleTime(), result.getMessageQueueOffset(),
                            orderCountInfo);
                        this.brokerController.getConsumerOffsetManager().commitOffset(channel.remoteAddress().toString(),
                            requestHeader.getConsumerGroup(), topic, queueId, finalOffset);
                    } else {
                        // 普通消息添加检查点
                        if (!appendCheckPoint(requestHeader, topic, reviveQid, queueId, finalOffset, result, popTime, this.brokerController.getBrokerConfig().getBrokerName())) {
                            return atomicRestNum.get() + result.getMessageCount();
                        }
                    }

                    // 构建偏移量信息
                    ExtraInfoUtil.buildStartOffsetInfo(startOffsetInfo, topic, queueId, finalOffset);
                    ExtraInfoUtil.buildMsgOffsetInfo(msgOffsetInfo, topic, queueId,
                        result.getMessageQueueOffset());
                } else if ((GetMessageStatus.NO_MATCHED_MESSAGE.equals(result.getStatus())
                    || GetMessageStatus.OFFSET_FOUND_NULL.equals(result.getStatus())
                    || GetMessageStatus.MESSAGE_WAS_REMOVING.equals(result.getStatus())
                    || GetMessageStatus.NO_MATCHED_LOGIC_QUEUE.equals(result.getStatus()))
                    && result.getNextBeginOffset() > -1) {
                    // 处理没有匹配消息的情况
                    if (isOrder) {
                        this.brokerController.getConsumerOffsetManager().commitOffset(channel.remoteAddress().toString(), requestHeader.getConsumerGroup(), topic,
                            queueId, result.getNextBeginOffset());
                    } else {
                        // 添加模拟检查点
                        popBufferMergeService.addCkMock(requestHeader.getConsumerGroup(), topic, queueId, finalOffset,
                            requestHeader.getInvisibleTime(), popTime, reviveQid, result.getNextBeginOffset(), brokerController.getBrokerConfig().getBrokerName());
                    }
                }

                // 计算剩余消息数量
                atomicRestNum.set(result.getMaxOffset() - result.getNextBeginOffset() + atomicRestNum.get());
                String brokerName = brokerController.getBrokerConfig().getBrokerName();

                // 处理获取到的消息
                for (SelectMappedBufferResult mapedBuffer : result.getMessageMapedList()) {
                    // 当popResponseReturnActualRetryTopic为true或topic不是重试topic时，不应该重新编码缓冲区
                    if (brokerController.getBrokerConfig().isPopResponseReturnActualRetryTopic() || !isRetry) {
                        getMessageResult.addMessage(mapedBuffer);
                    } else {
                        // 重新编码重试消息，将topic改回原始topic
                        List<MessageExt> messageExtList = MessageDecoder.decodesBatch(mapedBuffer.getByteBuffer(),
                            true, false, true);
                        mapedBuffer.release();
                        for (MessageExt messageExt : messageExtList) {
                            try {
                                // 构建检查点信息
                                String ckInfo = ExtraInfoUtil.buildExtraInfo(finalOffset, popTime, requestHeader.getInvisibleTime(),
                                    reviveQid, messageExt.getTopic(), brokerName, messageExt.getQueueId(), messageExt.getQueueOffset());
                                messageExt.getProperties().putIfAbsent(MessageConst.PROPERTY_POP_CK, ckInfo);
                                // 将重试消息的topic设置为原始topic并清除消息存储大小以便重新编码
                                messageExt.setTopic(requestHeader.getTopic());
                                messageExt.setStoreSize(0);
                                byte[] encode = MessageDecoder.encode(messageExt, false);
                                ByteBuffer buffer = ByteBuffer.wrap(encode);
                                SelectMappedBufferResult tmpResult =
                                    new SelectMappedBufferResult(mapedBuffer.getStartOffset(), buffer, encode.length, null);
                                getMessageResult.addMessage(tmpResult);
                            } catch (Exception e) {
                                POP_LOGGER.error("Exception in recode retry message buffer, topic={}", topic, e);
                            }
                        }
                    }
                }

                // 增加飞行中消息计数
                this.brokerController.getPopInflightMessageCounter().incrementInFlightMessageNum(
                    topic,
                    requestHeader.getConsumerGroup(),
                    queueId,
                    result.getMessageCount()
                );
                return atomicRestNum.get();
            }).whenComplete((result, throwable) -> {
                if (throwable != null) {
                    POP_LOGGER.error("Pop message error, {}", lockKey, throwable);
                }
                // 确保释放队列锁
                queueLockManager.unLock(lockKey);
            });
    }

    /**
     * 判断是否应该停止pop操作
     * 当未确认的消息数量超过阈值时，停止pop新消息
     * @param topic topic名称
     * @param group 消费组名称
     * @param queueId 队列ID
     * @return true表示应该停止pop
     */
    private boolean isPopShouldStop(String topic, String group, int queueId) {
        return brokerController.getBrokerConfig().isEnablePopMessageThreshold() &&
            brokerController.getPopInflightMessageCounter().getGroupPopInFlightMessageNum(topic, group, queueId) > brokerController.getBrokerConfig().getPopInflightMessageThreshold();
    }

    /**
     * 获取pop偏移量
     * @param topic topic名称
     * @param group 消费组名称
     * @param queueId 队列ID
     * @param initMode 初始化模式
     * @param init 是否初始化
     * @param lockKey 锁键
     * @param checkResetOffset 是否检查重置偏移量
     * @return pop偏移量
     * @throws ConsumeQueueException 消费队列异常
     */
    private long getPopOffset(String topic, String group, int queueId, int initMode, boolean init, String lockKey,
        boolean checkResetOffset) throws ConsumeQueueException {
        // 先查询消费者的偏移量
        long offset = this.brokerController.getConsumerOffsetManager().queryOffset(group, topic, queueId);
        if (offset < 0) {
            // 如果没有找到，使用初始偏移量
            offset = this.getInitOffset(topic, group, queueId, initMode, init);
        }

        // 检查是否有重置偏移量
        if (checkResetOffset) {
            Long resetOffset = resetPopOffset(topic, group, queueId);
            if (resetOffset != null) {
                return resetOffset;
            }
        }

        // 获取缓冲区中的最新偏移量
        long bufferOffset = this.popBufferMergeService.getLatestOffset(lockKey);
        if (bufferOffset < 0) {
            return offset;
        } else {
            // 返回两者中的较大值
            return Math.max(bufferOffset, offset);
        }
    }

    /**
     * 获取初始偏移量
     * @param topic topic名称
     * @param group 消费组名称
     * @param queueId 队列ID
     * @param initMode 初始化模式
     * @param init 是否初始化（是否提交偏移量）
     * @return 初始偏移量
     * @throws ConsumeQueueException 消费队列异常
     */
    public long getInitOffset(String topic, String group, int queueId, int initMode, boolean init)
        throws ConsumeQueueException {
        long offset;
        if (ConsumeInitMode.MIN == initMode || topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
            // 从最小偏移量开始消费
            offset = this.brokerController.getMessageStore().getMinOffsetInQueue(topic, queueId);
        } else {
            // 检查是否可以通过内存中的消息初始化pop偏移量
            if (this.brokerController.getBrokerConfig().isInitPopOffsetByCheckMsgInMem() &&
                this.brokerController.getMessageStore().getMinOffsetInQueue(topic, queueId) <= 0 &&
                this.brokerController.getMessageStore().checkInMemByConsumeOffset(topic, queueId, 0, 1)) {
                offset = 0;
            } else {
                // pop最后一条消息，然后提交偏移量
                offset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId) - 1;
                // 最大偏移量且没有消费者偏移量
                if (offset < 0) {
                    offset = 0;
                }
            }
        }

        if (init) { // 无论哪种初始化模式
            // 提交初始偏移量
            this.brokerController.getConsumerOffsetManager().commitOffset(
                "getPopOffset", group, topic, queueId, offset);
        }
        return offset;
    }

    /**
     * 构建检查点消息
     * @param ck 检查点对象
     * @param reviveQid 恢复队列ID
     * @return 检查点消息
     */
    public MessageExtBrokerInner buildCkMsg(final PopCheckPoint ck, final int reviveQid) {
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setTopic(reviveTopic);
        msgInner.setBody(JSON.toJSONString(ck).getBytes(StandardCharsets.UTF_8));
        msgInner.setQueueId(reviveQid);
        msgInner.setTags(PopAckConstants.CK_TAG);
        msgInner.setBornTimestamp(System.currentTimeMillis());
        msgInner.setBornHost(this.brokerController.getStoreHost());
        msgInner.setStoreHost(this.brokerController.getStoreHost());
        // 设置延迟投递时间
        msgInner.setDeliverTimeMs(ck.getReviveTime() - PopAckConstants.ackTimeInterval);
        msgInner.getProperties().put(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, genCkUniqueId(ck));
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
        return msgInner;
    }

    /**
     * 添加检查点到恢复日志
     * @param requestHeader 请求头
     * @param topic topic名称
     * @param reviveQid 恢复队列ID
     * @param queueId 队列ID
     * @param offset 偏移量
     * @param getMessageTmpResult 获取消息的临时结果
     * @param popTime pop时间
     * @param brokerName broker名称
     * @return 是否添加成功
     */
    private boolean appendCheckPoint(final PopMessageRequestHeader requestHeader,
        final String topic, final int reviveQid, final int queueId, final long offset,
        final GetMessageResult getMessageTmpResult, final long popTime, final String brokerName) {
        // 将检查点消息添加到恢复日志
        final PopCheckPoint ck = new PopCheckPoint();
        ck.setBitMap(0);
        ck.setNum((byte) getMessageTmpResult.getMessageMapedList().size());
        ck.setPopTime(popTime);
        ck.setInvisibleTime(requestHeader.getInvisibleTime());
        ck.setStartOffset(offset);
        ck.setCId(requestHeader.getConsumerGroup());
        ck.setTopic(topic);
        ck.setQueueId(queueId);
        ck.setBrokerName(brokerName);

        // 添加每条消息的偏移量差值
        for (Long msgQueueOffset : getMessageTmpResult.getMessageQueueOffset()) {
            ck.addDiff((int) (msgQueueOffset - offset));
        }

        // 更新统计信息
        this.brokerController.getBrokerStatsManager().incBrokerCkNums(1);
        this.brokerController.getBrokerStatsManager().incGroupCkNums(requestHeader.getConsumerGroup(), requestHeader.getTopic(), 1);

        // 尝试添加检查点到缓冲区
        final boolean addBufferSuc = this.popBufferMergeService.addCk(
            ck, reviveQid, -1, getMessageTmpResult.getNextBeginOffset()
        );
        if (addBufferSuc) {
            return true;
        }

        // 如果添加失败，创建一条定时消息，进入磁盘
        return this.popBufferMergeService.addCkJustOffset(
            ck, reviveQid, -1, getMessageTmpResult.getNextBeginOffset()
        );
    }

    /**
     * 重置pop偏移量
     * @param topic topic名称
     * @param group 消费组名称
     * @param queueId 队列ID
     * @return 重置的偏移量，如果没有重置则返回null
     */
    private Long resetPopOffset(String topic, String group, int queueId) {
        String lockKey = topic + PopAckConstants.SPLIT + group + PopAckConstants.SPLIT + queueId;
        // 查询并删除重置偏移量
        Long resetOffset =
            this.brokerController.getConsumerOffsetManager().queryThenEraseResetOffset(topic, group, queueId);
        if (resetOffset != null) {
            // 清除顺序消息的阻塞状态
            this.brokerController.getConsumerOrderInfoManager().clearBlock(topic, group, queueId);
            // 清除偏移量队列
            this.getPopBufferMergeService().clearOffsetQueue(lockKey);
            // 提交重置的偏移量
            this.brokerController.getConsumerOffsetManager()
                .commitOffset("ResetPopOffset", group, topic, queueId, resetOffset);
        }
        return resetOffset;
    }

    /**
     * 读取获取消息结果到字节数组
     * @param getMessageResult 获取消息结果
     * @param group 消费组名称
     * @param topic topic名称
     * @param queueId 队列ID
     * @return 消息内容的字节数组
     */
    private byte[] readGetMessageResult(final GetMessageResult getMessageResult, final String group, final String topic,
        final int queueId) {
        final ByteBuffer byteBuffer = ByteBuffer.allocate(getMessageResult.getBufferTotalSize());
        long storeTimestamp = 0;
        try {
            List<ByteBuffer> messageBufferList = getMessageResult.getMessageBufferList();
            for (ByteBuffer bb : messageBufferList) {
                byteBuffer.put(bb);
                // 获取消息存储时间戳
                storeTimestamp = bb.getLong(MessageDecoder.MESSAGE_STORE_TIMESTAMP_POSITION);
            }
        } finally {
            // 释放资源
            getMessageResult.release();
        }

        // 记录磁盘落后时间
        this.brokerController.getBrokerStatsManager().recordDiskFallBehindTime(group, topic, queueId,
            this.brokerController.getMessageStore().now() - storeTimestamp);
        return byteBuffer.array();
    }

    /**
     * 定时锁类
     * 支持超时的锁机制，用于防止死锁
     */
    static class TimedLock {
        /** 锁状态，false表示未锁定 */
        private final AtomicBoolean lock;
        /** 锁定时间 */
        private volatile long lockTime;

        public TimedLock() {
            // 初始化锁状态，false表示未锁定
            this.lock = new AtomicBoolean(false);
            this.lockTime = System.currentTimeMillis();
        }

        /**
         * 尝试获取锁
         * @return true表示获取成功
         */
        public boolean tryLock() {
            boolean ret = lock.compareAndSet(false, true);
            if (ret) {
                this.lockTime = System.currentTimeMillis();
                return true;
            } else {
                return false;
            }
        }

        /**
         * 释放锁
         */
        public void unLock() {
            lock.set(false);
        }

        /**
         * 检查是否被锁定
         * @return true表示被锁定
         */
        public boolean isLock() {
            return lock.get();
        }

        /**
         * 获取锁定时间
         * @return 锁定时间戳
         */
        public long getLockTime() {
            return lockTime;
        }
    }

    /**
     * 队列锁管理器
     * 管理每个队列的锁，防止同一队列被多个消费者同时访问
     * 继承ServiceThread，具有后台清理过期锁的能力
     */
    public class QueueLockManager extends ServiceThread {
        /** 过期本地缓存，存储队列锁 */
        private final ConcurrentHashMap<String, TimedLock> expiredLocalCache = new ConcurrentHashMap<>(100000);

        /**
         * 构建锁键
         * @param topic topic名称
         * @param consumerGroup 消费组名称
         * @param queueId 队列ID
         * @return 锁键字符串
         */
        public String buildLockKey(String topic, String consumerGroup, int queueId) {
            return topic + PopAckConstants.SPLIT + consumerGroup + PopAckConstants.SPLIT + queueId;
        }

        /**
         * 尝试获取锁
         * @param topic topic名称
         * @param consumerGroup 消费组名称
         * @param queueId 队列ID
         * @return true表示获取成功
         */
        public boolean tryLock(String topic, String consumerGroup, int queueId) {
            return tryLock(buildLockKey(topic, consumerGroup, queueId));
        }

        /**
         * 尝试获取锁
         * @param key 锁键
         * @return true表示获取成功
         */
        public boolean tryLock(String key) {
            TimedLock timedLock = ConcurrentHashMapUtils.computeIfAbsent(expiredLocalCache, key, k -> new TimedLock());
            return timedLock.tryLock();
        }

        /**
         * 清理未使用的锁
         * 注意：这个方法不是线程安全的，可能导致重复锁
         *
         * @param usedExpireMillis 过期时间（毫秒）
         * @return TimedLock的总数
         */
        public int cleanUnusedLock(final long usedExpireMillis) {
            Iterator<Entry<String, TimedLock>> iterator = expiredLocalCache.entrySet().iterator();
            int total = 0;
            while (iterator.hasNext()) {
                Entry<String, TimedLock> entry = iterator.next();
                // 如果锁超过过期时间，则删除
                if (System.currentTimeMillis() - entry.getValue().getLockTime() > usedExpireMillis) {
                    iterator.remove();
                    POP_LOGGER.info("Remove unused queue lock: {}, {}, {}", entry.getKey(),
                        entry.getValue().getLockTime(),
                        entry.getValue().isLock());
                }
                total++;
            }
            return total;
        }

        /**
         * 释放锁
         * @param topic topic名称
         * @param consumerGroup 消费组名称
         * @param queueId 队列ID
         */
        public void unLock(String topic, String consumerGroup, int queueId) {
            unLock(buildLockKey(topic, consumerGroup, queueId));
        }

        /**
         * 释放锁
         * @param key 锁键
         */
        public void unLock(String key) {
            TimedLock timedLock = expiredLocalCache.get(key);
            if (timedLock != null) {
                timedLock.unLock();
            }
        }

        /**
         * 获取服务名称
         * @return 服务名称
         */
        @Override
        public String getServiceName() {
            if (PopMessageProcessor.this.brokerController.getBrokerConfig().isInBrokerContainer()) {
                return PopMessageProcessor.this.brokerController.getBrokerIdentity().getIdentifier() + QueueLockManager.class.getSimpleName();
            }
            return QueueLockManager.class.getSimpleName();
        }

        /**
         * 后台运行方法
         * 定期清理过期的锁
         */
        @Override
        public void run() {
            while (!isStopped()) {
                try {
                    // 等待60秒
                    this.waitForRunning(60000);
                    // 清理60秒前的未使用锁
                    int count = cleanUnusedLock(60000);
                    POP_LOGGER.info("QueueLockSize={}", count);
                } catch (Exception e) {
                    PopMessageProcessor.POP_LOGGER.error("QueueLockManager run error", e);
                }
            }
        }
    }
}
