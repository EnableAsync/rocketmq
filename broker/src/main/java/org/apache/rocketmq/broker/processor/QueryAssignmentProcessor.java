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

import io.netty.channel.ChannelHandlerContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.client.ConsumerGroupInfo;
import org.apache.rocketmq.broker.loadbalance.MessageRequestModeManager;
import org.apache.rocketmq.broker.topic.TopicRouteInfoManager;
import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragelyByCircle;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.message.MessageQueueAssignment;
import org.apache.rocketmq.common.message.MessageRequestMode;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.body.QueryAssignmentRequestBody;
import org.apache.rocketmq.remoting.protocol.body.QueryAssignmentResponseBody;
import org.apache.rocketmq.remoting.protocol.body.SetMessageRequestModeRequestBody;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;

/**
 * 查询队列分配处理器
 * 主要功能：
 * 1. 处理消费者队列分配请求，实现客户端负载均衡
 * 2. 设置和管理消息请求模式（PULL/POP模式）
 * 3. 支持多种队列分配策略
 */
public class QueryAssignmentProcessor implements NettyRequestProcessor {
    /** 日志对象 */
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    /** Broker控制器，用于访问各种管理器和配置 */
    private final BrokerController brokerController;

    /** 负载均衡策略名称到策略实现的映射表 */
    private final ConcurrentHashMap<String, AllocateMessageQueueStrategy> name2LoadStrategy = new ConcurrentHashMap<>();

    /** 消息请求模式管理器，管理topic和消费组的请求模式 */
    private MessageRequestModeManager messageRequestModeManager;

    /**
     * 构造函数
     * @param brokerController Broker控制器
     */
    public QueryAssignmentProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;

        // 注册负载均衡策略
        // 注意：这里使用broker的日志而不是客户端日志进行初始化

        // 注册平均分配策略
        AllocateMessageQueueAveragely allocateMessageQueueAveragely = new AllocateMessageQueueAveragely();
        name2LoadStrategy.put(allocateMessageQueueAveragely.getName(), allocateMessageQueueAveragely);

        // 注册环形平均分配策略
        AllocateMessageQueueAveragelyByCircle allocateMessageQueueAveragelyByCircle = new AllocateMessageQueueAveragelyByCircle();
        name2LoadStrategy.put(allocateMessageQueueAveragelyByCircle.getName(), allocateMessageQueueAveragelyByCircle);

        // 初始化消息请求模式管理器并加载配置
        this.messageRequestModeManager = new MessageRequestModeManager(brokerController);
        this.messageRequestModeManager.load();
    }

    /**
     * 处理网络请求的入口方法
     * @param ctx 网络通道上下文
     * @param request 远程调用请求
     * @return 响应命令
     * @throws RemotingCommandException 远程调用异常
     */
    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        // 根据请求码分发到不同的处理方法
        switch (request.getCode()) {
            case RequestCode.QUERY_ASSIGNMENT:
                // 处理查询队列分配请求
                return this.queryAssignment(ctx, request);
            case RequestCode.SET_MESSAGE_REQUEST_MODE:
                // 处理设置消息请求模式请求
                return this.setMessageRequestMode(ctx, request);
            default:
                break;
        }
        return null;
    }

    /**
     * 是否拒绝请求
     * @return false表示不拒绝任何请求
     */
    @Override
    public boolean rejectRequest() {
        return false;
    }

    /**
     * 处理队列分配查询请求
     * @param ctx 网络通道上下文
     * @param request 远程调用请求
     * @return 包含队列分配结果的响应
     * @throws RemotingCommandException 远程调用异常
     */
    private RemotingCommand queryAssignment(ChannelHandlerContext ctx, RemotingCommand request)
        throws RemotingCommandException {
        // 解析请求体
        final QueryAssignmentRequestBody requestBody = QueryAssignmentRequestBody.decode(request.getBody(), QueryAssignmentRequestBody.class);
        final String topic = requestBody.getTopic();
        final String consumerGroup = requestBody.getConsumerGroup();
        final String clientId = requestBody.getClientId();
        final MessageModel messageModel = requestBody.getMessageModel();
        final String strategyName = requestBody.getStrategyName();

        // 创建响应对象
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final QueryAssignmentResponseBody responseBody = new QueryAssignmentResponseBody();

        // 获取或设置消息请求模式
        SetMessageRequestModeRequestBody setMessageRequestModeRequestBody = this.messageRequestModeManager.getMessageRequestMode(topic, consumerGroup);
        if (setMessageRequestModeRequestBody == null) {
            // 如果没有设置过模式，使用默认配置
            setMessageRequestModeRequestBody = new SetMessageRequestModeRequestBody();
            setMessageRequestModeRequestBody.setTopic(topic);
            setMessageRequestModeRequestBody.setConsumerGroup(consumerGroup);

            if (topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                // 重试topic必须使用PULL模式
                setMessageRequestModeRequestBody.setMode(MessageRequestMode.PULL);
            } else {
                // 使用默认的消息请求模式
                setMessageRequestModeRequestBody.setMode(brokerController.getBrokerConfig().getDefaultMessageRequestMode());
            }

            // 如果是POP模式，设置共享队列数量
            if (setMessageRequestModeRequestBody.getMode() == MessageRequestMode.POP) {
                setMessageRequestModeRequestBody.setPopShareQueueNum(brokerController.getBrokerConfig().getDefaultPopShareQueueNum());
            }
        }

        // 执行负载均衡，获取分配给该客户端的队列
        Set<MessageQueue> messageQueues = doLoadBalance(topic, consumerGroup, clientId, messageModel, strategyName, setMessageRequestModeRequestBody, ctx);

        // 构建队列分配结果
        Set<MessageQueueAssignment> assignments = null;
        if (messageQueues != null) {
            assignments = new HashSet<>();
            for (MessageQueue messageQueue : messageQueues) {
                MessageQueueAssignment messageQueueAssignment = new MessageQueueAssignment();
                messageQueueAssignment.setMessageQueue(messageQueue);
                if (setMessageRequestModeRequestBody != null) {
                    messageQueueAssignment.setMode(setMessageRequestModeRequestBody.getMode());
                }
                assignments.add(messageQueueAssignment);
            }
        }

        // 设置响应内容
        responseBody.setMessageQueueAssignments(assignments);
        response.setBody(responseBody.encode());
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    /**
     * 执行负载均衡，为客户端分配MessageQueue
     *
     * @param topic 主题名称
     * @param consumerGroup 消费组名称
     * @param clientId 客户端ID
     * @param messageModel 消息模型（广播/集群）
     * @param strategyName 分配策略名称
     * @param setMessageRequestModeRequestBody 消息请求模式配置
     * @param ctx 网络通道上下文
     * @return 分配给客户端的MessageQueue集合
     *         返回空集合表示客户端应该清除之前的分配
     *         返回null表示结果无效，客户端应该跳过更新逻辑
     */
    private Set<MessageQueue> doLoadBalance(final String topic, final String consumerGroup, final String clientId,
        final MessageModel messageModel, final String strategyName,
        SetMessageRequestModeRequestBody setMessageRequestModeRequestBody, final ChannelHandlerContext ctx) {
        Set<MessageQueue> assignedQueueSet = null;
        final TopicRouteInfoManager topicRouteInfoManager = this.brokerController.getTopicRouteInfoManager();

        switch (messageModel) {
            case BROADCASTING: {
                // 广播模式：每个消费者都消费所有队列
                assignedQueueSet = topicRouteInfoManager.getTopicSubscribeInfo(topic);
                if (assignedQueueSet == null) {
                    log.warn("QueryLoad: no assignment for group[{}], the topic[{}] does not exist.", consumerGroup, topic);
                }
                break;
            }
            case CLUSTERING: {
                // 集群模式：队列在消费者之间分配
                Set<MessageQueue> mqSet;

                if (MixAll.isLmq(topic)) {
                    // 轻量级消息队列（LMQ）的特殊处理
                    mqSet = new HashSet<>();
                    mqSet.add(new MessageQueue(
                        topic, brokerController.getBrokerConfig().getBrokerName(), (int)MixAll.LMQ_QUEUE_ID));
                } else {
                    // 普通topic，获取所有可订阅的队列
                    mqSet = topicRouteInfoManager.getTopicSubscribeInfo(topic);
                }

                if (null == mqSet) {
                    if (!topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                        log.warn("QueryLoad: no assignment for group[{}], the topic[{}] does not exist.", consumerGroup, topic);
                    }
                    return null;
                }

                // 如果禁用了服务端负载均衡，直接返回所有队列
                if (!brokerController.getBrokerConfig().isServerLoadBalancerEnable()) {
                    return mqSet;
                }

                // 获取消费组中所有客户端ID列表
                List<String> cidAll = null;
                ConsumerGroupInfo consumerGroupInfo = this.brokerController.getConsumerManager().getConsumerGroupInfo(consumerGroup);
                if (consumerGroupInfo != null) {
                    cidAll = consumerGroupInfo.getAllClientId();
                }

                if (null == cidAll) {
                    log.warn("QueryLoad: no assignment for group[{}] topic[{}], get consumer id list failed", consumerGroup, topic);
                    return null;
                }

                // 准备分配算法的输入参数
                List<MessageQueue> mqAll = new ArrayList<>();
                mqAll.addAll(mqSet);
                Collections.sort(mqAll);  // 排序保证分配结果的一致性
                Collections.sort(cidAll); // 排序保证分配结果的一致性

                List<MessageQueue> allocateResult = null;
                try {
                    // 根据策略名称获取分配策略
                    AllocateMessageQueueStrategy allocateMessageQueueStrategy = name2LoadStrategy.get(strategyName);
                    if (null == allocateMessageQueueStrategy) {
                        log.warn("QueryLoad: unsupported strategy [{}],  {}", strategyName, RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
                        return null;
                    }

                    // 根据消息请求模式选择不同的分配方法
                    if (setMessageRequestModeRequestBody != null && setMessageRequestModeRequestBody.getMode() == MessageRequestMode.POP) {
                        // POP模式的特殊分配逻辑，支持队列共享
                        allocateResult = allocate4Pop(allocateMessageQueueStrategy, consumerGroup, clientId, mqAll,
                            cidAll, setMessageRequestModeRequestBody.getPopShareQueueNum());
                    } else {
                        // 标准的分配逻辑
                        allocateResult = allocateMessageQueueStrategy.allocate(consumerGroup, clientId, mqAll, cidAll);
                    }
                } catch (Throwable e) {
                    log.error("QueryLoad: no assignment for group[{}] topic[{}], allocate message queue exception. strategy name: {}, ex: {}", consumerGroup, topic, strategyName, e);
                    return null;
                }

                // 将分配结果转换为Set
                assignedQueueSet = new HashSet<>();
                if (allocateResult != null) {
                    assignedQueueSet.addAll(allocateResult);
                }
                break;
            }
            default:
                break;
        }
        return assignedQueueSet;
    }

    /**
     * POP模式的队列分配算法
     * POP模式支持队列共享，允许多个消费者从同一个队列pop消息
     *
     * @param allocateMessageQueueStrategy 基础分配策略
     * @param consumerGroup 消费组名称
     * @param clientId 客户端ID
     * @param mqAll 所有可用的MessageQueue列表
     * @param cidAll 所有客户端ID列表
     * @param popShareQueueNum 共享队列数量
     * @return 分配给该客户端的MessageQueue列表
     */
    public List<MessageQueue> allocate4Pop(AllocateMessageQueueStrategy allocateMessageQueueStrategy,
        final String consumerGroup, final String clientId, List<MessageQueue> mqAll, List<String> cidAll,
        int popShareQueueNum) {
        List<MessageQueue> allocateResult;

        if (popShareQueueNum <= 0 || popShareQueueNum >= cidAll.size() - 1) {
            // 情况1：每个客户端都可以pop所有队列
            // 当共享数量配置无效时，允许所有客户端访问所有队列
            allocateResult = new ArrayList<>(mqAll.size());
            for (MessageQueue mq : mqAll) {
                // 必须创建新的MessageQueue实例，避免修改AssignmentManager中的缓存
                // queueId设置为-1，表示可以pop该topic下的所有队列
                MessageQueue newMq = new MessageQueue(mq.getTopic(), mq.getBrokerName(), -1);
                allocateResult.add(newMq);
            }
        } else {
            if (cidAll.size() <= mqAll.size()) {
                // 情况2：消费者数量少于或等于队列数量
                // 在POP模式下，消费者可以共享分配给它后面N个消费者的队列

                // 首先按正常策略分配属于自己的队列
                allocateResult = allocateMessageQueueStrategy.allocate(consumerGroup, clientId, mqAll, cidAll);
                int index = cidAll.indexOf(clientId);

                if (index >= 0) {
                    // 然后添加共享的队列（分配给后面N个消费者的队列）
                    for (int i = 1; i <= popShareQueueNum; i++) {
                        index++;
                        index = index % cidAll.size(); // 环形处理
                        List<MessageQueue> tmp = allocateMessageQueueStrategy.allocate(consumerGroup, cidAll.get(index), mqAll, cidAll);
                        allocateResult.addAll(tmp);
                    }
                }
            } else {
                // 情况3：消费者数量多于队列数量
                // 确保每个消费者都有队列分配
                allocateResult = allocate(consumerGroup, clientId, mqAll, cidAll);
            }
        }
        return allocateResult;
    }

    /**
     * 当消费者数量多于队列数量时的分配算法
     * 确保每个消费者至少分配到一个队列
     *
     * @param consumerGroup 消费组名称
     * @param currentCID 当前客户端ID
     * @param mqAll 所有MessageQueue列表
     * @param cidAll 所有客户端ID列表
     * @return 分配给当前客户端的MessageQueue列表
     */
    private List<MessageQueue> allocate(String consumerGroup, String currentCID, List<MessageQueue> mqAll,
        List<String> cidAll) {
        // 参数验证
        if (StringUtils.isBlank(currentCID)) {
            throw new IllegalArgumentException("currentCID is empty");
        }
        if (CollectionUtils.isEmpty(mqAll)) {
            throw new IllegalArgumentException("mqAll is null or mqAll empty");
        }
        if (CollectionUtils.isEmpty(cidAll)) {
            throw new IllegalArgumentException("cidAll is null or cidAll empty");
        }

        List<MessageQueue> result = new ArrayList<>();
        if (!cidAll.contains(currentCID)) {
            // 如果当前客户端不在客户端列表中，记录警告并返回空结果
            log.info("[BUG] ConsumerGroup: {} The consumerId: {} not in cidAll: {}",
                consumerGroup,
                currentCID,
                cidAll);
            return result;
        }

        // 根据客户端在列表中的位置分配队列
        // 使用取模运算确保即使消费者多于队列也能分配到队列
        int index = cidAll.indexOf(currentCID);
        result.add(mqAll.get(index % mqAll.size()));
        return result;
    }

    /**
     * 处理设置消息请求模式的请求
     *
     * @param ctx 网络通道上下文
     * @param request 远程调用请求
     * @return 响应命令
     * @throws RemotingCommandException 远程调用异常
     */
    private RemotingCommand setMessageRequestMode(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final SetMessageRequestModeRequestBody requestBody = SetMessageRequestModeRequestBody.decode(request.getBody(), SetMessageRequestModeRequestBody.class);
        final String topic = requestBody.getTopic();

        // 重试topic不允许设置模式（必须使用PULL模式）
        if (topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark("retry topic is not allowed to set mode");
            return response;
        }

        // 验证topic是否存在
        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(topic);
        if (null == topicConfig) {
            response.setCode(ResponseCode.TOPIC_NOT_EXIST);
            response.setRemark("topic[" + topic + "] not exist");
            return response;
        }

        // 验证消费组是否存在
        final String consumerGroup = requestBody.getConsumerGroup();
        SubscriptionGroupConfig groupConfig = this.brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(consumerGroup);
        if (null == groupConfig) {
            response.setCode(ResponseCode.SUBSCRIPTION_GROUP_NOT_EXIST);
            response.setRemark("subscription group does not exist");
            return response;
        }

        // 设置消息请求模式并持久化
        this.messageRequestModeManager.setMessageRequestMode(topic, consumerGroup, requestBody);
        this.messageRequestModeManager.persist();

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    /**
     * 获取消息请求模式管理器
     * @return 消息请求模式管理器实例
     */
    public MessageRequestModeManager getMessageRequestModeManager() {
        return messageRequestModeManager;
    }
}
