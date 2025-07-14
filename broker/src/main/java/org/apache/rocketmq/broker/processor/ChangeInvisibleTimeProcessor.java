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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.nio.charset.StandardCharsets;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.metrics.PopMetricsManager;
import org.apache.rocketmq.broker.offset.ConsumerOffsetManager;
import org.apache.rocketmq.broker.offset.ConsumerOrderInfoManager;
import org.apache.rocketmq.broker.pop.PopConsumerLockService;
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
import org.apache.rocketmq.remoting.netty.NettyRemotingAbstract;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.ChangeInvisibleTimeRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ChangeInvisibleTimeResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.ExtraInfoUtil;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.exception.ConsumeQueueException;
import org.apache.rocketmq.store.pop.AckMsg;
import org.apache.rocketmq.store.pop.PopCheckPoint;

/**
 * 修改消息不可见时间处理器
 *
 * 核心功能：
 * 1. 处理客户端修改消息不可见时间的请求
 * 2. 支持普通消息和顺序消息的不可见时间修改
 * 3. 通过CheckPoint机制实现消息的延时重新消费
 * 4. 维护消费顺序信息，确保顺序消息的正确处理
 *
 * 业务场景：
 * 当消费者需要更长时间处理消息时，可以请求延长消息的不可见时间，
 * 防止消息被其他消费者重复消费。这在处理耗时任务时非常有用。
 */
public class ChangeInvisibleTimeProcessor implements NettyRequestProcessor {
    // POP消费专用日志记录器
    private static final Logger POP_LOGGER = LoggerFactory.getLogger(LoggerName.ROCKETMQ_POP_LOGGER_NAME);

    // Broker控制器引用
    private final BrokerController brokerController;

    // 复活主题名称，用于存储CheckPoint和ACK消息
    private final String reviveTopic;

    /**
     * 构造函数
     * @param brokerController Broker���制器
     */
    public ChangeInvisibleTimeProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
        // 构建集群级别的复活主题名称，格式：%RETRY%{clusterName}
        this.reviveTopic = PopAckConstants.buildClusterReviveTopic(this.brokerController.getBrokerConfig().getBrokerClusterName());
    }

    /**
     * 处理请求的入口方法（实现NettyRequestProcessor接口）
     *
     * @param ctx Netty通道处理器上下文
     * @param request 远程请求命令
     * @return 响应命令
     *
     * 调用链路：
     * 1. 客户端发起ChangeInvisibleTime请求 ->
     * 2. Netty接收并路由到此处理器 ->
     * 3. ChangeInvisibleTimeProcessor.processRequest()
     */
    @Override
    public RemotingCommand processRequest(final ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        return this.processRequest(ctx.channel(), request, true);
    }

    /**
     * 是否拒绝请求
     * @return false表示不拒绝请求
     */
    @Override
    public boolean rejectRequest() {
        return false;
    }

    /**
     * 处理请求的核心方法
     * 根据配置决定使用同步还是异步方式处理
     *
     * @param channel 网络通道
     * @param request 请求命令
     * @param brokerAllowSuspend 是否允许挂起
     * @return 响应命令
     */
    private RemotingCommand processRequest(final Channel channel, RemotingCommand request,
        boolean brokerAllowSuspend) throws RemotingCommandException {

        // 异步处理请求
        CompletableFuture<RemotingCommand> responseFuture = processRequestAsync(channel, request, brokerAllowSuspend);

        // 根据配置决定处理方式
        if (brokerController.getBrokerConfig().isAppendCkAsync() && brokerController.getBrokerConfig().isAppendAckAsync()) {
            // 异步模式：直接返回null，响应通过回调处理
            responseFuture.thenAccept(response ->
                doResponse(channel, request, response)
            ).exceptionally(throwable -> {
                // 异常处理：创建系统错误响应
                RemotingCommand response = RemotingCommand.createResponseCommand(ChangeInvisibleTimeResponseHeader.class);
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setOpaque(request.getOpaque());
                doResponse(channel, request, response);
                POP_LOGGER.error("append checkpoint or ack origin failed", throwable);
                return null;
            });
        } else {
            // 同步模式：等待异步结果完成
            RemotingCommand response;
            try {
                // 等待最多3秒获取结果
                response = responseFuture.get(3000, TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                // 超时或异常时返回系统错误
                response = RemotingCommand.createResponseCommand(ChangeInvisibleTimeResponseHeader.class);
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setOpaque(request.getOpaque());
                POP_LOGGER.error("append checkpoint or ack origin failed", e);
            }
            return response;
        }
        return null;
    }

    /**
     * 异步处理请求的核心逻辑
     *
     * @param channel 网络通道
     * @param request 请求命令
     * @param brokerAllowSuspend 是否允许挂起
     * @return 异步响应结果
     */
    public CompletableFuture<RemotingCommand> processRequestAsync(final Channel channel, RemotingCommand request,
        boolean brokerAllowSuspend) throws RemotingCommandException {

        // 解析请求头
        final ChangeInvisibleTimeRequestHeader requestHeader =
            (ChangeInvisibleTimeRequestHeader) request.decodeCommandCustomHeader(ChangeInvisibleTimeRequestHeader.class);

        // 创建响应命令
        RemotingCommand response = RemotingCommand.createResponseCommand(ChangeInvisibleTimeResponseHeader.class);
        response.setCode(ResponseCode.SUCCESS);
        response.setOpaque(request.getOpaque());
        final ChangeInvisibleTimeResponseHeader responseHeader =
            (ChangeInvisibleTimeResponseHeader) response.readCustomHeader();

        // 1. 验证主题配置
        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(requestHeader.getTopic());
        if (null == topicConfig) {
            POP_LOGGER.error("The topic {} not exist, consumer: {} ",
                requestHeader.getTopic(), RemotingHelper.parseChannelRemoteAddr(channel));
            response.setCode(ResponseCode.TOPIC_NOT_EXIST);
            response.setRemark(String.format("topic[%s] not exist, apply first please! %s",
                requestHeader.getTopic(), FAQUrl.suggestTodo(FAQUrl.APPLY_TOPIC_URL)));
            return CompletableFuture.completedFuture(response);
        }

        // 2. 验证队列ID合法性
        if (requestHeader.getQueueId() >= topicConfig.getReadQueueNums() || requestHeader.getQueueId() < 0) {
            String errorInfo = String.format("queueId[%d] is illegal, topic:[%s] topicConfig.readQueueNums:[%d] consumer:[%s]",
                requestHeader.getQueueId(), requestHeader.getTopic(), topicConfig.getReadQueueNums(), channel.remoteAddress());
            POP_LOGGER.warn(errorInfo);
            response.setCode(ResponseCode.MESSAGE_ILLEGAL);
            response.setRemark(errorInfo);
            return CompletableFuture.completedFuture(response);
        }

        // 3. 验证消息偏移量范围
        long minOffset = this.brokerController.getMessageStore().getMinOffsetInQueue(requestHeader.getTopic(), requestHeader.getQueueId());
        long maxOffset;
        try {
            maxOffset = this.brokerController.getMessageStore().getMaxOffsetInQueue(requestHeader.getTopic(), requestHeader.getQueueId());
        } catch (ConsumeQueueException e) {
            throw new RemotingCommandException("Failed to get max consume offset", e);
        }

        if (requestHeader.getOffset() < minOffset || requestHeader.getOffset() >= maxOffset) {
            response.setCode(ResponseCode.NO_MESSAGE);
            return CompletableFuture.completedFuture(response);
        }

        // 4. 解析额外信息
        String[] extraInfo = ExtraInfoUtil.split(requestHeader.getExtraInfo());

        // 5. 根据是否启用PopConsumerKVService选择处理方式
        if (brokerController.getBrokerConfig().isPopConsumerKVServiceEnable()) {
            if (ExtraInfoUtil.isOrder(extraInfo)) {
                // 新版本的顺序消息处理
                return this.processChangeInvisibleTimeForOrderNew(
                    requestHeader, extraInfo, response, responseHeader);
            }

            // 新版本的普通消息处理
            try {
                long current = System.currentTimeMillis();
                brokerController.getPopConsumerService().changeInvisibilityDuration(
                    ExtraInfoUtil.getPopTime(extraInfo),           // 原始POP时间
                    ExtraInfoUtil.getInvisibleTime(extraInfo),     // 原始不可见时间
                    current,                                       // 当前时间
                    requestHeader.getInvisibleTime(),              // 新的不可见时间
                    requestHeader.getConsumerGroup(),              // 消费者组
                    requestHeader.getTopic(),                      // 主题
                    requestHeader.getQueueId(),                    // 队列ID
                    requestHeader.getOffset()                      // 消息偏移量
                );

                // 设置响应头信息
                responseHeader.setInvisibleTime(requestHeader.getInvisibleTime());
                responseHeader.setPopTime(current);
                responseHeader.setReviveQid(ExtraInfoUtil.getReviveQid(extraInfo));
            } catch (Exception e) {
                response.setCode(ResponseCode.SYSTEM_ERROR);
            }
            return CompletableFuture.completedFuture(response);
        }

        // 6. 旧版本处理逻辑
        if (ExtraInfoUtil.isOrder(extraInfo)) {
            // 旧版本的顺序消息处理
            return CompletableFuture.completedFuture(
                processChangeInvisibleTimeForOrder(requestHeader, extraInfo, response, responseHeader));
        }

        // 7. 旧版本的普通消息处理：通过CheckPoint机制
        long now = System.currentTimeMillis();
        CompletableFuture<Boolean> futureResult = appendCheckPointThenAckOrigin(requestHeader,
            ExtraInfoUtil.getReviveQid(extraInfo), // 复活队列ID
            requestHeader.getQueueId(),             // 原始队列ID
            requestHeader.getOffset(),              // 消息偏移量
            now,                                    // 当前时间
            extraInfo                               // 额外信息
        );

        // 根据CheckPoint添加结果设置响应
        return futureResult.thenCompose(result -> {
            if (result) {
                responseHeader.setInvisibleTime(requestHeader.getInvisibleTime());
                responseHeader.setPopTime(now);
                responseHeader.setReviveQid(ExtraInfoUtil.getReviveQid(extraInfo));
            } else {
                response.setCode(ResponseCode.SYSTEM_ERROR);
            }
            return CompletableFuture.completedFuture(response);
        });
    }

    /**
     * 新版本顺序消息的不可见时间修改处理
     * 使用PopConsumerService和锁机制确保顺序性
     *
     * @param requestHeader 请求头
     * @param extraInfo 额外信息
     * @param response 响应对象
     * @param responseHeader 响应头
     * @return 异步响应结果
     */
    @SuppressWarnings({"StatementWithEmptyBody", "DuplicatedCode"})
    public CompletableFuture<RemotingCommand> processChangeInvisibleTimeForOrderNew(
        ChangeInvisibleTimeRequestHeader requestHeader, String[] extraInfo,
        RemotingCommand response, ChangeInvisibleTimeResponseHeader responseHeader) {

        String groupId = requestHeader.getConsumerGroup();
        String topicId = requestHeader.getTopic();
        Integer queueId = requestHeader.getQueueId();
        long popTime = ExtraInfoUtil.getPopTime(extraInfo);

        // 获取必要的服务组件
        PopConsumerLockService consumerLockService =
            this.brokerController.getPopConsumerService().getConsumerLockService();
        ConsumerOffsetManager consumerOffsetManager = this.brokerController.getConsumerOffsetManager();
        ConsumerOrderInfoManager consumerOrderInfoManager = brokerController.getConsumerOrderInfoManager();

        // 检查消息偏移量是否已被消费
        long oldOffset = consumerOffsetManager.queryOffset(groupId, topicId, queueId);
        if (requestHeader.getOffset() < oldOffset) {
            return CompletableFuture.completedFuture(response);
        }

        // 获取消费者锁，确保顺序处理
        while (!consumerLockService.tryLock(groupId, topicId)) {
            // 自旋等待获取锁
        }

        try {
            // 双重检查：再次验证偏移量
            oldOffset = consumerOffsetManager.queryOffset(groupId, topicId, queueId);
            if (requestHeader.getOffset() < oldOffset) {
                return CompletableFuture.completedFuture(response);
            }

            // 计算新的可见时间
            long visibilityTimeout = System.currentTimeMillis() + requestHeader.getInvisibleTime();

            // 更新顺序信息管理器中的下次可见时间
            consumerOrderInfoManager.updateNextVisibleTime(
                topicId, groupId, queueId, requestHeader.getOffset(), popTime, visibilityTimeout);

            // 设置响应头信息
            responseHeader.setInvisibleTime(visibilityTimeout - popTime);
            responseHeader.setPopTime(popTime);
            responseHeader.setReviveQid(ExtraInfoUtil.getReviveQid(extraInfo));
        } finally {
            // 释放锁
            consumerLockService.unlock(groupId, topicId);
        }

        return CompletableFuture.completedFuture(response);
    }

    /**
     * 旧版本顺序消息的不可见时间修改处理
     * 使用队列锁管理器确保顺序性
     *
     * @param requestHeader 请求头
     * @param extraInfo 额外信息
     * @param response 响应对象
     * @param responseHeader 响应头
     * @return 响应命令
     */
    protected RemotingCommand processChangeInvisibleTimeForOrder(ChangeInvisibleTimeRequestHeader requestHeader,
        String[] extraInfo, RemotingCommand response, ChangeInvisibleTimeResponseHeader responseHeader) {

        long popTime = ExtraInfoUtil.getPopTime(extraInfo);

        // 检查消息偏移量是否已被消费
        long oldOffset = this.brokerController.getConsumerOffsetManager().queryOffset(
            requestHeader.getConsumerGroup(), requestHeader.getTopic(), requestHeader.getQueueId());
        if (requestHeader.getOffset() < oldOffset) {
            return response;
        }

        // 获取队列锁
        while (!this.brokerController.getPopMessageProcessor().getQueueLockManager().tryLock(
            requestHeader.getTopic(), requestHeader.getConsumerGroup(), requestHeader.getQueueId())) {
            // 自旋等待获取锁
        }

        try {
            // 双重检查偏移量
            oldOffset = this.brokerController.getConsumerOffsetManager().queryOffset(
                requestHeader.getConsumerGroup(), requestHeader.getTopic(), requestHeader.getQueueId());
            if (requestHeader.getOffset() < oldOffset) {
                return response;
            }

            // 计算下次可见时间
            long nextVisibleTime = System.currentTimeMillis() + requestHeader.getInvisibleTime();

            // 更新顺序信息管理器
            this.brokerController.getConsumerOrderInfoManager().updateNextVisibleTime(
                requestHeader.getTopic(), requestHeader.getConsumerGroup(),
                requestHeader.getQueueId(), requestHeader.getOffset(), popTime, nextVisibleTime);

            // 设置响应头
            responseHeader.setInvisibleTime(nextVisibleTime - popTime);
            responseHeader.setPopTime(popTime);
            responseHeader.setReviveQid(ExtraInfoUtil.getReviveQid(extraInfo));
        } finally {
            // 释放队列锁
            this.brokerController.getPopMessageProcessor().getQueueLockManager().unLock(
                requestHeader.getTopic(), requestHeader.getConsumerGroup(), requestHeader.getQueueId());
        }

        return response;
    }

    /**
     * 确认原始消息
     * 将ACK消息写入复活主题，用于后续的消息确认处理
     *
     * @param requestHeader 请求头
     * @param extraInfo 额外信息
     * @return 异步操作结果
     */
    private CompletableFuture<Boolean> ackOrigin(final ChangeInvisibleTimeRequestHeader requestHeader,
        String[] extraInfo) {

        // 创建内部消息对象
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();

        // 创建ACK消息对象
        AckMsg ackMsg = new AckMsg();
        ackMsg.setAckOffset(requestHeader.getOffset());                    // 确认的偏移量
        ackMsg.setStartOffset(ExtraInfoUtil.getCkQueueOffset(extraInfo)); // CheckPoint起始偏移量
        ackMsg.setConsumerGroup(requestHeader.getConsumerGroup());        // 消费者组
        ackMsg.setTopic(requestHeader.getTopic());                        // 主题
        ackMsg.setQueueId(requestHeader.getQueueId());                    // 队列ID
        ackMsg.setPopTime(ExtraInfoUtil.getPopTime(extraInfo));           // POP时间
        ackMsg.setBrokerName(ExtraInfoUtil.getBrokerName(extraInfo));     // Broker名称

        int rqId = ExtraInfoUtil.getReviveQid(extraInfo);

        // 更新统计信息
        this.brokerController.getBrokerStatsManager().incBrokerAckNums(1);
        this.brokerController.getBrokerStatsManager().incGroupAckNums(
            requestHeader.getConsumerGroup(), requestHeader.getTopic(), 1);

        // 尝试使用buffer合并服务优化性能
        if (brokerController.getPopMessageProcessor().getPopBufferMergeService().addAk(rqId, ackMsg)) {
            return CompletableFuture.completedFuture(true);
        }

        // 构建ACK消息并写入复活主题
        msgInner.setTopic(reviveTopic);
        msgInner.setBody(JSON.toJSONString(ackMsg).getBytes(StandardCharsets.UTF_8));
        msgInner.setQueueId(rqId);
        msgInner.setTags(PopAckConstants.ACK_TAG);
        msgInner.setBornTimestamp(System.currentTimeMillis());
        msgInner.setBornHost(this.brokerController.getStoreHost());
        msgInner.setStoreHost(this.brokerController.getStoreHost());

        // 设置投递时间：原始不可见时间到期时
        msgInner.setDeliverTimeMs(ExtraInfoUtil.getPopTime(extraInfo) + ExtraInfoUtil.getInvisibleTime(extraInfo));

        // 设置唯一ID
        msgInner.getProperties().put(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
            PopMessageProcessor.genAckUniqueId(ackMsg));
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));

        // 异步写入消息
        return this.brokerController.getEscapeBridge().asyncPutMessageToSpecificQueue(msgInner)
            .thenCompose(putMessageResult -> {
                // 检查写入结果
                if (putMessageResult.getPutMessageStatus() != PutMessageStatus.PUT_OK
                    && putMessageResult.getPutMessageStatus() != PutMessageStatus.FLUSH_DISK_TIMEOUT
                    && putMessageResult.getPutMessageStatus() != PutMessageStatus.FLUSH_SLAVE_TIMEOUT
                    && putMessageResult.getPutMessageStatus() != PutMessageStatus.SLAVE_NOT_AVAILABLE) {
                    POP_LOGGER.error("change Invisible, put ack msg fail: {}, {}", ackMsg, putMessageResult);
                }

                // 更新指标统计
                PopMetricsManager.incPopReviveAckPutCount(ackMsg, putMessageResult.getPutMessageStatus());
                return CompletableFuture.completedFuture(true);
            })
            .exceptionally(e -> {
                POP_LOGGER.error("change Invisible, put ack msg error: {}, {}", requestHeader.getExtraInfo(), e.getMessage());
                return false;
            });
    }

    /**
     * 添加CheckPoint然后确认原始消息
     * 这是普通消息修改不可见时间的核心逻辑
     *
     * @param requestHeader 请求头
     * @param reviveQid 复活队列ID
     * @param queueId 原始队列ID
     * @param offset 消息偏移量
     * @param popTime POP时间
     * @param extraInfo 额外信息
     * @return 异步操作结果
     */
    private CompletableFuture<Boolean> appendCheckPointThenAckOrigin(
        final ChangeInvisibleTimeRequestHeader requestHeader,
        int reviveQid, int queueId, long offset, long popTime, String[] extraInfo) {

        // 创建CheckPoint消息
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setTopic(reviveTopic);

        // 创建PopCheckPoint对象
        PopCheckPoint ck = new PopCheckPoint();
        ck.setBitMap(0);                                        // 位图，标记哪些消息已确认
        ck.setNum((byte) 1);                                    // 消息数量
        ck.setPopTime(popTime);                                 // POP时间
        ck.setInvisibleTime(requestHeader.getInvisibleTime());  // 新的不可见时间
        ck.setStartOffset(offset);                              // 起始偏移量
        ck.setCId(requestHeader.getConsumerGroup());            // 消费者组ID
        ck.setTopic(requestHeader.getTopic());                  // 主题
        ck.setQueueId(queueId);                                 // 队列ID
        ck.addDiff(0);                                          // 添加偏移量差值
        ck.setBrokerName(ExtraInfoUtil.getBrokerName(extraInfo)); // Broker名称

        // 设置消息属性
        msgInner.setBody(JSON.toJSONString(ck).getBytes(StandardCharsets.UTF_8));
        msgInner.setQueueId(reviveQid);
        msgInner.setTags(PopAckConstants.CK_TAG);
        msgInner.setBornTimestamp(System.currentTimeMillis());
        msgInner.setBornHost(this.brokerController.getStoreHost());
        msgInner.setStoreHost(this.brokerController.getStoreHost());

        // 设置投递时间：复活时间减去ACK时间间隔
        msgInner.setDeliverTimeMs(ck.getReviveTime() - PopAckConstants.ackTimeInterval);

        // 设置唯一ID
        msgInner.getProperties().put(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
            PopMessageProcessor.genCkUniqueId(ck));
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));

        // 异步写入CheckPoint消息
        return this.brokerController.getEscapeBridge().asyncPutMessageToSpecificQueue(msgInner)
            .thenCompose(putMessageResult -> {
                // 记录日志
                if (brokerController.getBrokerConfig().isEnablePopLog()) {
                    POP_LOGGER.info("change Invisible, appendCheckPoint, topic {}, queueId {},reviveId {}, cid {}, startOffset {}, rt {}, result {}",
                        requestHeader.getTopic(), queueId, reviveQid, requestHeader.getConsumerGroup(),
                        offset, ck.getReviveTime(), putMessageResult);
                }

                // 更新统计信息
                if (putMessageResult != null) {
                    PopMetricsManager.incPopReviveCkPutCount(ck, putMessageResult.getPutMessageStatus());
                    if (putMessageResult.isOk()) {
                        this.brokerController.getBrokerStatsManager().incBrokerCkNums(1);
                        this.brokerController.getBrokerStatsManager().incGroupCkNums(
                            requestHeader.getConsumerGroup(), requestHeader.getTopic(), 1);
                    }
                }

                // 检查CheckPoint写入结果
                if (putMessageResult.getPutMessageStatus() != PutMessageStatus.PUT_OK
                    && putMessageResult.getPutMessageStatus() != PutMessageStatus.FLUSH_DISK_TIMEOUT
                    && putMessageResult.getPutMessageStatus() != PutMessageStatus.FLUSH_SLAVE_TIMEOUT
                    && putMessageResult.getPutMessageStatus() != PutMessageStatus.SLAVE_NOT_AVAILABLE) {
                    POP_LOGGER.error("change invisible, put new ck error: {}", putMessageResult);
                    return CompletableFuture.completedFuture(false);
                } else {
                    // CheckPoint写入成功，继续确认原始消息
                    return ackOrigin(requestHeader, extraInfo);
                }
            })
            .exceptionally(throwable -> {
                POP_LOGGER.error("change invisible, put new ck error", throwable);
                return null;
            });
    }

    /**
     * 发送响应到客户端
     *
     * @param channel 网络通道
     * @param request 原始请求
     * @param response 响应命令
     */
    protected void doResponse(Channel channel, RemotingCommand request, final RemotingCommand response) {
        NettyRemotingAbstract.writeResponse(channel, request, response);
    }
}
