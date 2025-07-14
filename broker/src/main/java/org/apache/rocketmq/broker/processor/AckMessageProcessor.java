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
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import java.util.BitSet;
import java.nio.charset.StandardCharsets;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.metrics.PopMetricsManager;
import org.apache.rocketmq.broker.offset.ConsumerOffsetManager;
import org.apache.rocketmq.broker.offset.order.ConsumerOrderInfoManager;
import org.apache.rocketmq.broker.pop.PopConsumerLockService;
import org.apache.rocketmq.common.KeyBuilder;
import org.apache.rocketmq.common.PopAckConstants;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.body.BatchAck;
import org.apache.rocketmq.remoting.protocol.body.BatchAckMessageRequestBody;
import org.apache.rocketmq.remoting.protocol.header.AckMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ExtraInfoUtil;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.exception.ConsumeQueueException;
import org.apache.rocketmq.store.pop.AckMsg;
import org.apache.rocketmq.store.pop.BatchAckMsg;

/**
 * Ack消息处理器
 *
 * 主要功能：
 * 1. 处理Pop消费模式下的消息确认请求（单个ACK和批量ACK）
 * 2. 管理Pop消息的复活机制（PopReviveService）
 * 3. 处理顺序消息的确认
 * 4. 将ACK记录写入复活主题，用于后续的重新投递处理
 *
 * 核心概念：
 * - Pop消费：客户端拉取消息后，消息在一定时间内不可见，需要客户端主动确认
 * - 复活机制：未确认的消息在不可见时间到期后会被重新投递
 * - 复活主题：存储ACK记录的特殊主题，用于跟踪消息确认状态
 *
 * 调用链路：
 * Client ACK Request -> NettyRemotingServer -> AckMessageProcessor.processRequest()
 */
public class AckMessageProcessor implements NettyRequestProcessor {
    private static final Logger POP_LOGGER = LoggerFactory.getLogger(LoggerName.ROCKETMQ_POP_LOGGER_NAME);

    // Broker控制器，提供各种服务的访问入口
    private final BrokerController brokerController;

    // 复活主题名称，用于存储ACK记录
    // 格式：%RETRY%${clusterName}_REVIVE_LOG
    private final String reviveTopic;

    // Pop复活服务数组，每个队列对应一个服务
    // 负责扫描复活主题中的ACK记录，处理消息的重新投递
    private final PopReviveService[] popReviveServices;

    /**
     * 构造函数：初始化ACK消息处理器
     *
     * 意图：
     * 1. 初始化复活主题名称
     * 2. 创建复活服务数组，每个队列一个服务
     * 3. 只有主Broker（brokerId=0）才运行复活服务
     */
    public AckMessageProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;

        // 构建集群级别的复活主题名称
        this.reviveTopic = PopAckConstants.buildClusterReviveTopic(
            this.brokerController.getBrokerConfig().getBrokerClusterName());

        // 创建复活服务数组，数量由配置决定
        this.popReviveServices = new PopReviveService[this.brokerController.getBrokerConfig().getReviveQueueNum()];
        for (int i = 0; i < this.brokerController.getBrokerConfig().getReviveQueueNum(); i++) {
            this.popReviveServices[i] = new PopReviveService(brokerController, reviveTopic, i);
            // 只有主Broker才运行复活服务，避免重复处理
            this.popReviveServices[i].setShouldRunPopRevive(brokerController.getBrokerConfig().getBrokerId() == 0);
        }
    }

    // 获取复活服务数组的方法
    public PopReviveService[] getPopReviveServices() {
        return popReviveServices;
    }

    /**
     * 启动复活服务
     * 意图：在Broker启动时启动所有复活服务线程
     *
     * 调用链路：BrokerController.start() -> startPopReviveService()
     */
    public void startPopReviveService() {
        for (PopReviveService popReviveService : popReviveServices) {
            popReviveService.start();
        }
    }

    /**
     * 关闭复活服务
     * 意图：在Broker关闭时优雅关闭所有复活服务线程
     *
     * 调用链路：BrokerController.shutdown() -> shutdownPopReviveService()
     */
    public void shutdownPopReviveService() {
        for (PopReviveService popReviveService : popReviveServices) {
            popReviveService.shutdown();
        }
    }

    /**
     * 设置复活服务的运行状态
     * 意图：支持主从切换，当Broker角色变化时动态调整复活服务状态
     *
     * 调用链路：主从切换逻辑 -> setPopReviveServiceStatus()
     */
    public void setPopReviveServiceStatus(boolean shouldStart) {
        for (PopReviveService popReviveService : popReviveServices) {
            popReviveService.setShouldRunPopRevive(shouldStart);
        }
    }

    /**
     * 检查是否有复活服务在运行
     * 意图：用于状态检查和监控
     */
    public boolean isPopReviveServiceRunning() {
        for (PopReviveService popReviveService : popReviveServices) {
            if (popReviveService.isShouldRunPopRevive()) {
                return true;
            }
        }
        return false;
    }

    /**
     * 网络请求处理入口
     * 意图：Netty框架调用的统一入口，转发到具体的处理方法
     *
     * 调用链路：NettyRemotingServer -> processRequest(ChannelHandlerContext, RemotingCommand)
     */
    @Override
    public RemotingCommand processRequest(final ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        return this.processRequest(ctx.channel(), request, true);
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    /**
     * ACK请求的核心处理方法
     *
     * 意图：
     * 1. 解析请求类型（单个ACK或批量ACK）
     * 2. 验证请求参数的合法性
     * 3. 根据配置选择新旧两种处理方式
     * 4. 调用相应的ACK处理方法
     *
     * 调用链路：
     * processRequest(ChannelHandlerContext) -> processRequest(Channel, RemotingCommand, boolean)
     */
    private RemotingCommand processRequest(final Channel channel, RemotingCommand request,
        boolean brokerAllowSuspend) throws RemotingCommandException {

        AckMessageRequestHeader requestHeader;
        BatchAckMessageRequestBody reqBody = null;
        final RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        response.setOpaque(request.getOpaque());

        if (request.getCode() == RequestCode.ACK_MESSAGE) {
            // 处理单个消息ACK请求
            requestHeader = (AckMessageRequestHeader) request.decodeCommandCustomHeader(AckMessageRequestHeader.class);

            // 验证主题是否存在
            TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(requestHeader.getTopic());
            if (null == topicConfig) {
                POP_LOGGER.error("The topic {} not exist, consumer: {} ", requestHeader.getTopic(), RemotingHelper.parseChannelRemoteAddr(channel));
                response.setCode(ResponseCode.TOPIC_NOT_EXIST);
                response.setRemark(String.format("topic[%s] not exist, apply first please! %s", requestHeader.getTopic(), FAQUrl.suggestTodo(FAQUrl.APPLY_TOPIC_URL)));
                return response;
            }

            // 验证队列ID是否合法
            if (requestHeader.getQueueId() >= topicConfig.getReadQueueNums() || requestHeader.getQueueId() < 0) {
                String errorInfo = String.format("queueId[%d] is illegal, topic:[%s] topicConfig.readQueueNums:[%d] consumer:[%s]",
                    requestHeader.getQueueId(), requestHeader.getTopic(), topicConfig.getReadQueueNums(), channel.remoteAddress());
                POP_LOGGER.warn(errorInfo);
                response.setCode(ResponseCode.MESSAGE_ILLEGAL);
                response.setRemark(errorInfo);
                return response;
            }

            // 验证偏移量是否在合法范围内
            long minOffset = this.brokerController.getMessageStore().getMinOffsetInQueue(requestHeader.getTopic(), requestHeader.getQueueId());
            long maxOffset;
            try {
                maxOffset = this.brokerController.getMessageStore().getMaxOffsetInQueue(requestHeader.getTopic(), requestHeader.getQueueId());
            } catch (ConsumeQueueException e) {
                throw new RemotingCommandException("Failed to get max offset", e);
            }

            if (requestHeader.getOffset() < minOffset || requestHeader.getOffset() > maxOffset) {
                String errorInfo = String.format("offset is illegal, key:%s@%d, commit:%d, store:%d~%d",
                    requestHeader.getTopic(), requestHeader.getQueueId(), requestHeader.getOffset(), minOffset, maxOffset);
                POP_LOGGER.warn(errorInfo);
                response.setCode(ResponseCode.NO_MESSAGE);
                response.setRemark(errorInfo);
                return response;
            }

            // 根据配置选择新旧ACK处理方式
            if (brokerController.getBrokerConfig().isPopConsumerKVServiceEnable()) {
                // 新版本：使用KV存储服务处理ACK
                appendAckNew(requestHeader, null, response, channel, null);
            } else {
                // 旧版本：使用复活主题处理ACK
                appendAck(requestHeader, null, response, channel, null);
            }

        } else if (request.getCode() == RequestCode.BATCH_ACK_MESSAGE) {
            // 处理批量消息ACK请求
            if (request.getBody() != null) {
                reqBody = BatchAckMessageRequestBody.decode(request.getBody(), BatchAckMessageRequestBody.class);
            }

            if (reqBody == null || reqBody.getAcks() == null || reqBody.getAcks().isEmpty()) {
                response.setCode(ResponseCode.NO_MESSAGE);
                return response;
            }

            // 遍历批量ACK中的每一个ACK记录
            for (BatchAck bAck : reqBody.getAcks()) {
                if (brokerController.getBrokerConfig().isPopConsumerKVServiceEnable()) {
                    appendAckNew(null, bAck, response, channel, reqBody.getBrokerName());
                } else {
                    appendAck(null, bAck, response, channel, reqBody.getBrokerName());
                }
            }
        } else {
            // 不支持的请求类型
            POP_LOGGER.error("AckMessageProcessor failed to process RequestCode: {}, consumer: {} ", request.getCode(), RemotingHelper.parseChannelRemoteAddr(channel));
            response.setCode(ResponseCode.MESSAGE_ILLEGAL);
            response.setRemark(String.format("AckMessageProcessor failed to process RequestCode: %d", request.getCode()));
            return response;
        }
        return response;
    }

    /**
     * 旧版本的ACK处理方法
     *
     * 意图：
     * 1. 解析ACK请求中的额外信息
     * 2. 构建ACK消息对象
     * 3. 将ACK消息写入复活主题，用于后续的重新投递处理
     * 4. 更新统计指标
     *
     * 核心流程：
     * 1. 提取请求参数（消费组、主题、队列ID、偏移量等）
     * 2. 特殊处理顺序消息的ACK
     * 3. 尝试加入到缓冲区进行批量处理
     * 4. 如果缓冲区满，则直接写入复活主题
     *
     * 调用链路：processRequest() -> appendAck()
     */
    private void appendAck(final AckMessageRequestHeader requestHeader, final BatchAck batchAck,
        final RemotingCommand response, final Channel channel, String brokerName) throws RemotingCommandException {

        String[] extraInfo;
        String consumeGroup, topic;
        int qId, rqId;  // qId: 队列ID, rqId: 复活队列ID
        long startOffset, ackOffset;
        long popTime, invisibleTime;
        AckMsg ackMsg;
        int ackCount = 0;

        if (batchAck == null) {
            // 处理单个ACK
            extraInfo = ExtraInfoUtil.split(requestHeader.getExtraInfo());
            brokerName = ExtraInfoUtil.getBrokerName(extraInfo);
            consumeGroup = requestHeader.getConsumerGroup();
            topic = requestHeader.getTopic();
            qId = requestHeader.getQueueId();
            rqId = ExtraInfoUtil.getReviveQid(extraInfo);  // 复活队列ID
            startOffset = ExtraInfoUtil.getCkQueueOffset(extraInfo);
            ackOffset = requestHeader.getOffset();
            popTime = ExtraInfoUtil.getPopTime(extraInfo);
            invisibleTime = ExtraInfoUtil.getInvisibleTime(extraInfo);

            // 特殊处理顺序消息的ACK
            if (rqId == KeyBuilder.POP_ORDER_REVIVE_QUEUE) {
                ackOrderly(topic, consumeGroup, qId, ackOffset, popTime, invisibleTime, channel, response);
                return;
            }

            ackMsg = new AckMsg();
            ackCount = 1;
        } else {
            // 处理批量ACK
            consumeGroup = batchAck.getConsumerGroup();
            topic = ExtraInfoUtil.getRealTopic(batchAck.getTopic(), batchAck.getConsumerGroup(), batchAck.getRetry());
            qId = batchAck.getQueueId();
            rqId = batchAck.getReviveQueueId();
            startOffset = batchAck.getStartOffset();
            ackOffset = -1;  // 批量ACK没有单一偏移量
            popTime = batchAck.getPopTime();
            invisibleTime = batchAck.getInvisibleTime();

            // 验证队列的偏移量范围
            long minOffset = this.brokerController.getMessageStore().getMinOffsetInQueue(topic, qId);
            long maxOffset;
            try {
                maxOffset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, qId);
            } catch (ConsumeQueueException e) {
                throw new RemotingCommandException("Failed to get max offset in queue", e);
            }

            if (minOffset == -1 || maxOffset == -1) {
                POP_LOGGER.error("Illegal topic or queue found when batch ack {}", batchAck);
                return;
            }

            // 处理批量ACK中的每个偏移量
            BatchAckMsg batchAckMsg = new BatchAckMsg();
            BitSet bitSet = batchAck.getBitSet();  // 使用位图表示要ACK的偏移量

            for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1)) {
                if (i == Integer.MAX_VALUE) {
                    break;
                }
                long offset = startOffset + i;

                // 检查偏移量是否在合法范围内
                if (offset < minOffset || offset > maxOffset) {
                    continue;
                }

                if (rqId == KeyBuilder.POP_ORDER_REVIVE_QUEUE) {
                    // 顺序消息特殊处理
                    ackOrderly(topic, consumeGroup, qId, offset, popTime, invisibleTime, channel, response);
                } else {
                    batchAckMsg.getAckOffsetList().add(offset);
                }
            }

            if (rqId == KeyBuilder.POP_ORDER_REVIVE_QUEUE || batchAckMsg.getAckOffsetList().isEmpty()) {
                return;
            }

            ackMsg = batchAckMsg;
            ackCount = batchAckMsg.getAckOffsetList().size();
        }

        // 更新统计指标
        this.brokerController.getBrokerStatsManager().incBrokerAckNums(ackCount);
        this.brokerController.getBrokerStatsManager().incGroupAckNums(consumeGroup, topic, ackCount);

        // 设置ACK消息的基本信息
        ackMsg.setConsumerGroup(consumeGroup);
        ackMsg.setTopic(topic);
        ackMsg.setQueueId(qId);
        ackMsg.setStartOffset(startOffset);
        ackMsg.setAckOffset(ackOffset);
        ackMsg.setPopTime(popTime);
        ackMsg.setBrokerName(brokerName);

        // 尝试加入到缓冲区进行批量处理（性能优化）
        if (this.brokerController.getPopMessageProcessor().getPopBufferMergeService().addAk(rqId, ackMsg)) {
            // 成功加入缓冲区，减少飞行中消息计数
            brokerController.getPopInflightMessageCounter().decrementInFlightMessageNum(topic, consumeGroup, popTime, qId, ackCount);
            return;
        }

        // 缓冲区满或添加失败，直接写入复活主题
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setTopic(reviveTopic);  // 写入复活主题
        msgInner.setBody(JSON.toJSONString(ackMsg).getBytes(StandardCharsets.UTF_8));
        msgInner.setQueueId(rqId);  // 使用复活队列ID

        // 设置消息标签和唯一ID
        if (ackMsg instanceof BatchAckMsg) {
            msgInner.setTags(PopAckConstants.BATCH_ACK_TAG);
            msgInner.getProperties().put(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
                PopMessageProcessor.genBatchAckUniqueId((BatchAckMsg) ackMsg));
        } else {
            msgInner.setTags(PopAckConstants.ACK_TAG);
            msgInner.getProperties().put(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
                PopMessageProcessor.genAckUniqueId(ackMsg));
        }

        // 设置消息的时间戳和主机信息
        msgInner.setBornTimestamp(System.currentTimeMillis());
        msgInner.setBornHost(this.brokerController.getStoreHost());
        msgInner.setStoreHost(this.brokerController.getStoreHost());

        // 设置投递时间（Pop时间 + 不可见时间）
        msgInner.setDeliverTimeMs(popTime + invisibleTime);
        msgInner.getProperties().put(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
            PopMessageProcessor.genAckUniqueId(ackMsg));
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));

        // 根据配置选择同步或异步写入
        if (brokerController.getBrokerConfig().isAppendAckAsync()) {
            // 异步写入ACK消息
            int finalAckCount = ackCount;
            this.brokerController.getEscapeBridge().asyncPutMessageToSpecificQueue(msgInner).thenAccept(putMessageResult -> {
                handlePutMessageResult(putMessageResult, ackMsg, topic, consumeGroup, popTime, qId, finalAckCount);
            }).exceptionally(throwable -> {
                handlePutMessageResult(new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, null, false),
                    ackMsg, topic, consumeGroup, popTime, qId, finalAckCount);
                POP_LOGGER.error("put ack msg error ", throwable);
                return null;
            });
        } else {
            // 同步写入ACK消息
            PutMessageResult putMessageResult = this.brokerController.getEscapeBridge().putMessageToSpecificQueue(msgInner);
            handlePutMessageResult(putMessageResult, ackMsg, topic, consumeGroup, popTime, qId, ackCount);
        }
    }

    /**
     * 新版本的ACK处理方法（使用KV服务）
     *
     * 意图：
     * 1. 使用KV存储服务替代复活主题机制
     * 2. 提供更高效的ACK处理性能
     * 3. 简化ACK记录的管理
     *
     * 与旧版本的区别：
     * 1. 不再写入复活主题，而是调用KV服务
     * 2. 异步处理，提高性能
     * 3. 简化了消息构建过程
     *
     * 调用链路：processRequest() -> appendAckNew()
     */
    private void appendAckNew(final AckMessageRequestHeader requestHeader, final BatchAck batchAck,
        final RemotingCommand response, final Channel channel, String brokerName) throws RemotingCommandException {

        if (requestHeader != null && batchAck == null) {
            // 处理单个ACK
            String[] extraInfo = ExtraInfoUtil.split(requestHeader.getExtraInfo());
            String groupId = requestHeader.getConsumerGroup();
            String topicId = requestHeader.getTopic();
            int queueId = requestHeader.getQueueId();
            long ackOffset = requestHeader.getOffset();
            long popTime = ExtraInfoUtil.getPopTime(extraInfo);
            long invisibleTime = ExtraInfoUtil.getInvisibleTime(extraInfo);
            int reviveQueueId = ExtraInfoUtil.getReviveQid(extraInfo);

            if (reviveQueueId == KeyBuilder.POP_ORDER_REVIVE_QUEUE) {
                // 顺序消息特殊处理
                ackOrderlyNew(topicId, groupId, queueId, ackOffset, popTime, invisibleTime, channel, response);
            } else {
                // 调用KV服务异步处理ACK
                this.brokerController.getPopConsumerService().ackAsync(
                    popTime, invisibleTime, groupId, topicId, queueId, ackOffset);
            }

            // 更新统计指标
            this.brokerController.getBrokerStatsManager().incBrokerAckNums(1);
            this.brokerController.getBrokerStatsManager().incGroupAckNums(groupId, topicId, 1);
        } else {
            // 处理批量ACK
            String groupId = batchAck.getConsumerGroup();
            String topicId = ExtraInfoUtil.getRealTopic(
                batchAck.getTopic(), batchAck.getConsumerGroup(), batchAck.getRetry());
            int queueId = batchAck.getQueueId();
            int reviveQueueId = batchAck.getReviveQueueId();
            long startOffset = batchAck.getStartOffset();
            long popTime = batchAck.getPopTime();
            long invisibleTime = batchAck.getInvisibleTime();

            try {
                // 验证偏移量范围
                long minOffset = this.brokerController.getMessageStore().getMinOffsetInQueue(topicId, queueId);
                long maxOffset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topicId, queueId);

                if (minOffset == -1 || maxOffset == -1) {
                    POP_LOGGER.error("Illegal topic or queue found when batch ack {}", batchAck);
                    return;
                }

                int ackCount = 0;
                // 处理批量ACK中的每个偏移量
                BitSet bitSet = batchAck.getBitSet();
                for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1)) {
                    if (i == Integer.MAX_VALUE) {
                        break;
                    }
                    long offset = startOffset + i;

                    if (offset < minOffset || offset > maxOffset) {
                        continue;
                    }

                    if (reviveQueueId == KeyBuilder.POP_ORDER_REVIVE_QUEUE) {
                        ackOrderlyNew(topicId, groupId, queueId, offset, popTime, invisibleTime, channel, response);
                    } else {
                        // 调用KV服务异步处理ACK
                        this.brokerController.getPopConsumerService().ackAsync(
                            popTime, invisibleTime, groupId, topicId, queueId, offset);
                    }
                    ackCount++;
                }

                // 更新统计指标
                this.brokerController.getBrokerStatsManager().incBrokerAckNums(ackCount);
                this.brokerController.getBrokerStatsManager().incGroupAckNums(groupId, topicId, ackCount);
            } catch (ConsumeQueueException e) {
                throw new RemotingCommandException("Failed to ack message", e);
            }
        }
    }

    /**
     * 处理ACK消息写入结果
     *
     * 意图：
     * 1. 检查ACK消息是否成功写入存储
     * 2. 更新监控指标
     * 3. 减少飞行中消息计数
     *
     * 调用链路：appendAck() -> handlePutMessageResult()
     */
    private void handlePutMessageResult(PutMessageResult putMessageResult, AckMsg ackMsg, String topic,
        String consumeGroup, long popTime, int qId, int ackCount) {

        // 检查写入状态，只有严重错误才记录日志
        if (putMessageResult.getPutMessageStatus() != PutMessageStatus.PUT_OK
            && putMessageResult.getPutMessageStatus() != PutMessageStatus.FLUSH_DISK_TIMEOUT
            && putMessageResult.getPutMessageStatus() != PutMessageStatus.FLUSH_SLAVE_TIMEOUT
            && putMessageResult.getPutMessageStatus() != PutMessageStatus.SLAVE_NOT_AVAILABLE) {
            POP_LOGGER.error("put ack msg error:" + putMessageResult);
        }

        // 更新监控指标
        PopMetricsManager.incPopReviveAckPutCount(ackMsg, putMessageResult.getPutMessageStatus());

        // 减少飞行中消息计数
        brokerController.getPopInflightMessageCounter().decrementInFlightMessageNum(topic, consumeGroup, popTime, qId, ackCount);
    }

    /**
     * 旧版本的顺序消息ACK处理
     *
     * 意图：
     * 1. 顺序消息需要严格按顺序确认
     * 2. 使用队列锁保证并发安全
     * 3. 更新消费进度，触发后续消息的投递
     *
     * 核心流程：
     * 1. 获取队列锁，防止并发操作
     * 2. 检查ACK偏移量是否合法
     * 3. 提交消费进度并获取下一个偏移量
     * 4. 触发消息到达通知
     *
     * 调用链路：appendAck() -> ackOrderly()
     */
    protected void ackOrderly(String topic, String consumeGroup, int qId, long ackOffset, long popTime,
        long invisibleTime, Channel channel, RemotingCommand response) {

        // 构建锁的Key
        String lockKey = topic + PopAckConstants.SPLIT + consumeGroup + PopAckConstants.SPLIT + qId;

        // 预检查：如果ACK偏移量小于已提交偏移量，直接返回
        long oldOffset = this.brokerController.getConsumerOffsetManager().queryOffset(consumeGroup, topic, qId);
        if (ackOffset < oldOffset) {
            return;
        }

        // 获取队列锁，保证顺序性
        while (!this.brokerController.getPopMessageProcessor().getQueueLockManager().tryLock(lockKey)) {
            // 忙等待获取锁
        }

        try {
            // 双重检查：再次验证偏移量
            oldOffset = this.brokerController.getConsumerOffsetManager().queryOffset(consumeGroup, topic, qId);
            if (ackOffset < oldOffset) {
                return;
            }

            // 提交当前偏移量并获取下一个可消费偏移量
            long nextOffset = brokerController.getConsumerOrderInfoManager().commitAndNext(
                topic, consumeGroup, qId, ackOffset, popTime);

            if (nextOffset > -1) {
                // 成功获取下一个偏移量，更新消费进度
                if (!this.brokerController.getConsumerOffsetManager().hasOffsetReset(topic, consumeGroup, qId)) {
                    this.brokerController.getConsumerOffsetManager().commitOffset(
                        channel.remoteAddress().toString(), consumeGroup, topic, qId, nextOffset);
                }

                // 检查是否有阻塞，如果没有则触发消息到达通知
                if (!this.brokerController.getConsumerOrderInfoManager().checkBlock(null, topic, consumeGroup, qId, invisibleTime)) {
                    this.brokerController.getPopMessageProcessor().notifyMessageArriving(topic, qId, consumeGroup);
                }
            } else if (nextOffset == -1) {
                // 偏移量非法，返回错误
                String errorInfo = String.format("offset is illegal, key:%s, old:%d, commit:%d, next:%d, %s",
                    lockKey, oldOffset, ackOffset, nextOffset, channel.remoteAddress());
                POP_LOGGER.warn(errorInfo);
                response.setCode(ResponseCode.MESSAGE_ILLEGAL);
                response.setRemark(errorInfo);
                return;
            }
        } finally {
            // 释放锁
            this.brokerController.getPopMessageProcessor().getQueueLockManager().unLock(lockKey);
        }

        // 减少飞行中消息计数
        brokerController.getPopInflightMessageCounter().decrementInFlightMessageNum(topic, consumeGroup, popTime, qId, 1);
    }

    /**
     * 新版本的顺序消息ACK处理
     *
     * 意图：
     * 1. 使用新的锁服务替代旧的队列锁管理器
     * 2. 提供更好的性能和可维护性
     * 3. 增加详细的日志记录
     *
     * 与旧版本的区别：
     * 1. 使用PopConsumerLockService替代QueueLockManager
     * 2. 增加了配置开关控制日志输出
     * 3. 优化了错误处理逻辑
     *
     * 调用链路：appendAckNew() -> ackOrderlyNew()
     */
    protected void ackOrderlyNew(String topic, String consumeGroup, int qId, long ackOffset, long popTime,
        long invisibleTime, Channel channel, RemotingCommand response) {

        // 获取相关服务实例
        ConsumerOffsetManager consumerOffsetManager = this.brokerController.getConsumerOffsetManager();
        ConsumerOrderInfoManager consumerOrderInfoManager = brokerController.getConsumerOrderInfoManager();
        PopConsumerLockService consumerLockService = this.brokerController.getPopConsumerService().getConsumerLockService();

        // 预检查偏移量
        long oldOffset = consumerOffsetManager.queryOffset(consumeGroup, topic, qId);
        if (ackOffset < oldOffset) {
            return;
        }

        // 获取消费者锁
        while (!consumerLockService.tryLock(consumeGroup, topic)) {
            // 忙等待获取锁
        }

        try {
            // 双重检查
            oldOffset = consumerOffsetManager.queryOffset(consumeGroup, topic, qId);
            if (ackOffset < oldOffset) {
                return;
            }

            // 提交并获取下一个偏移量
            long nextOffset = consumerOrderInfoManager.commitAndNext(topic, consumeGroup, qId, ackOffset, popTime);

            // 记录详细日志（如果启用）
            if (brokerController.getBrokerConfig().isPopConsumerKVServiceLog()) {
                POP_LOGGER.info("PopConsumerService ack orderly, time={}, topicId={}, groupId={}, queueId={}, " +
                    "offset={}, next={}", popTime, topic, consumeGroup, qId, ackOffset, nextOffset);
            }

            if (nextOffset > -1L) {
                // 成功处理，更新消费进度
                if (!consumerOffsetManager.hasOffsetReset(topic, consumeGroup, qId)) {
                    String remoteAddress = RemotingHelper.parseSocketAddressAddr(channel.remoteAddress());
                    consumerOffsetManager.commitOffset(remoteAddress, consumeGroup, topic, qId, nextOffset);
                }

                // 检查并触发消息到达通知
                if (!consumerOrderInfoManager.checkBlock(null, topic, consumeGroup, qId, invisibleTime)) {
                    this.brokerController.getPopMessageProcessor().notifyMessageArriving(topic, qId, consumeGroup);
                }
                return;
            }

            if (nextOffset == -1) {
                // 处理错误情况
                String errorInfo = String.format("offset is illegal, key:%s %s %s, old:%d, commit:%d, next:%d, %s",
                    consumeGroup, topic, qId, oldOffset, ackOffset, nextOffset, channel.remoteAddress());
                POP_LOGGER.warn(errorInfo);
                response.setCode(ResponseCode.MESSAGE_ILLEGAL);
                response.setRemark(errorInfo);
            }
        } finally {
            // 释放锁
            consumerLockService.unlock(consumeGroup, topic);
        }
    }
}
