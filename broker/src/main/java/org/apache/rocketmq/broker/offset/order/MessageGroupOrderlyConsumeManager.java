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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.header.ExtraInfoUtil;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.GetMessageStatus;
import org.apache.rocketmq.store.SelectMappedBufferResult;

/**
 * 消息组级别的顺序消费控制器
 * 基于消息组（sharding key）实现队列内的并发消费，显著提升并发度
 * <p>
 * 核心特点：
 * 1. 同一队列内不同消息组可以并行消费
 * 2. 只有相同消息组内的消息需要保持严格顺序
 * 3. 大幅提升并发度，特别适用于消息组分布均匀的场景
 * <p>
 * 数据结构：
 * topic@group -> queueId -> messageGroup -> OrderInfo
 */
public class MessageGroupOrderlyConsumeManager implements OrderlyConsumeManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    public static final String CONTROLLER_TYPE = "MESSAGE_GROUP_LEVEL";

    // Topic和Group的分隔符
    private static final String TOPIC_GROUP_SEPARATOR = "@";

    // 默认消息组，用于没有sharding key的消息
    private static final String DEFAULT_MESSAGE_GROUP = "_DEFAULT_GROUP_";

    /**
     * 核心数据结构：四层嵌套Map
     * 第一层：topic@group -> 第二层Map
     * 第二层：queueId -> 第三层Map
     * 第三层：messageGroup -> OrderInfo
     * <p>
     * 这种结构支持队列内不同消息组的并发消费
     */
    private final ConcurrentHashMap<String/* topic@group*/,
        ConcurrentHashMap<Integer/*queueId*/,
            ConcurrentHashMap<String/*messageGroup*/, ConsumerOrderInfoManager.OrderInfo>>> table =
        new ConcurrentHashMap<>(128);

    // 锁管理器，用于管理消费者的锁状态
    private ConsumerOrderInfoLockManager consumerOrderInfoLockManager;

    // Broker控制器引用
    private BrokerController brokerController;

    /**
     * 构造函数
     *
     * @param brokerController             Broker控制器
     * @param consumerOrderInfoLockManager 锁管理器
     */
    public MessageGroupOrderlyConsumeManager(BrokerController brokerController,
        ConsumerOrderInfoLockManager consumerOrderInfoLockManager) {
        this.brokerController = brokerController;
        this.consumerOrderInfoLockManager = consumerOrderInfoLockManager;
    }

    @Override
    public void update(String attemptId, boolean isRetry, String topic, String group, int queueId,
        long popTime, long invisibleTime, List<Long> msgQueueOffsetList,
        StringBuilder orderInfoBuilder, GetMessageResult getMessageResult) {

        if (msgQueueOffsetList == null || msgQueueOffsetList.isEmpty()) {
            return;
        }

        // 从消息偏移量列表中读取消息，分析消息组
        // MessageGroupAnalysis analysis = analyzeMessageGroups(topic, group, queueId, msgQueueOffsetList);

        String key = buildKey(topic, group);

        // 获取或创建队列映射
        ConcurrentHashMap<Integer, ConcurrentHashMap<String, ConsumerOrderInfoManager.OrderInfo>> queueMap =
            table.computeIfAbsent(key, k -> new ConcurrentHashMap<>(16));
//
//        // 获取或创建消息组映射
//        ConcurrentHashMap<String, ConsumerOrderInfoManager.OrderInfo> messageGroupMap =
//            queueMap.computeIfAbsent(queueId, k -> new ConcurrentHashMap<>(16));
//
//        // 为每个消息组创建或更新 OrderInfo
//        for (Map.Entry<String, List<Long>> entry : analysis.getMessageGroupOffsets().entrySet()) {
//            String messageGroup = entry.getKey();
//            List<Long> groupOffsets = entry.getValue();
//
//            ConsumerOrderInfoManager.OrderInfo existingOrderInfo = messageGroupMap.get(messageGroup);
//            ConsumerOrderInfoManager.OrderInfo newOrderInfo;
//            if (existingOrderInfo != null) {
//                // 合并现有信息
//                newOrderInfo = new ConsumerOrderInfoManager.OrderInfo(
//                    attemptId, popTime, invisibleTime, groupOffsets, System.currentTimeMillis(), 0);
//                newOrderInfo.mergeOffsetConsumedCount(existingOrderInfo.getAttemptId(),
//                    existingOrderInfo.getOffsetList(), existingOrderInfo.getOffsetConsumedCount());
//            } else {
//                // 创建新的 OrderInfo
//                newOrderInfo = new ConsumerOrderInfoManager.OrderInfo(
//                    attemptId, popTime, invisibleTime, groupOffsets, System.currentTimeMillis(), 0);
//            }
//
//            messageGroupMap.put(messageGroup, newOrderInfo);
//
//            // 为每个消息组构建消费次数信息
//            buildOrderCountInfo(orderInfoBuilder, topic, queueId, messageGroup, newOrderInfo);
//        }
//
//        // 更新锁释放时间戳
//        updateLockFreeTimestamp(topic, group, queueId, analysis);

        if (getMessageResult != null) {
            List<String> messageGroups = new ArrayList<>();
            // 待确认是不是一定会有消息组
            for (ByteBuffer buffer : getMessageResult.getMessageBufferList()) {
                Map<String, String> properties = MessageDecoder.decodeProperties(buffer);
                if (properties == null || properties.get(MessageConst.PROPERTY_SHARDING_KEY) == null) {
                    messageGroups.add(DEFAULT_MESSAGE_GROUP);
                } else {
                    messageGroups.add(properties.get(MessageConst.PROPERTY_SHARDING_KEY));
                }
            }
        } else {
            log.warn("[MessageGroupOrderlyConsumeManager] getMessageResult is null");
        }
    }

    /**
     * 分析消息偏移量列表，提取消息组信息
     */
    private MessageGroupAnalysis analyzeMessageGroups(String topic, String group, int queueId,
        List<Long> msgQueueOffsetList) {
        MessageGroupAnalysis analysis = new MessageGroupAnalysis();

        try {
            // 计算批量读取的起始和结束偏移量
            long minOffset = msgQueueOffsetList.stream().min(Long::compareTo).orElse(0L);
            long maxOffset = msgQueueOffsetList.stream().max(Long::compareTo).orElse(0L);
            int batchSize = (int) (maxOffset - minOffset + 1);

            // 异步获取消息
            CompletableFuture<GetMessageResult> future = brokerController.getMessageStore()
                .getMessageAsync(group, topic, queueId, minOffset, batchSize, null);

            GetMessageResult result = future.get(); // 同步等待结果

            if (result != null && result.getStatus() == GetMessageStatus.FOUND) {
                // 解析消息，提取消息组信息
                Map<String, List<Long>> messageGroupOffsets = new HashMap<>();

                for (SelectMappedBufferResult bufferResult : result.getMessageMapedList()) {
                    try {
                        List<MessageExt> messageExtList = MessageDecoder.decodesBatch(
                            bufferResult.getByteBuffer(), true, false, true);

                        for (MessageExt messageExt : messageExtList) {
                            long queueOffset = messageExt.getQueueOffset();

                            // 只处理我们关心的偏移量
                            if (msgQueueOffsetList.contains(queueOffset)) {
                                String messageGroup = extractMessageGroup(messageExt);
                                messageGroupOffsets.computeIfAbsent(messageGroup, k -> new ArrayList<>())
                                    .add(queueOffset);
                            }
                        }
                    } finally {
                        bufferResult.release();
                    }
                }

                analysis.setMessageGroupOffsets(messageGroupOffsets);
            } else {
                // 如果无法读取消息，使用默认消息组
                Map<String, List<Long>> defaultGroupOffsets = new HashMap<>();
                defaultGroupOffsets.put(DEFAULT_MESSAGE_GROUP, new ArrayList<>(msgQueueOffsetList));
                analysis.setMessageGroupOffsets(defaultGroupOffsets);
            }
        } catch (Exception e) {
            log.error("Failed to analyze message groups", e);
            analysis.setMessageGroupOffsets(Map.of(DEFAULT_MESSAGE_GROUP, new ArrayList<>(msgQueueOffsetList)));
        }

        return analysis;
    }

    /**
     * 从 MessageExt 中提取消息组（sharding key）
     */
    private String extractMessageGroup(MessageExt messageExt) {
        String shardingKey = messageExt.getProperty(MessageConst.PROPERTY_SHARDING_KEY);
        return shardingKey != null && !shardingKey.isEmpty() ? shardingKey : DEFAULT_MESSAGE_GROUP;
    }

    /**
     * 构建消费次数信息
     */
    private void buildOrderCountInfo(StringBuilder orderInfoBuilder, String topic, int queueId,
        String messageGroup, ConsumerOrderInfoManager.OrderInfo orderInfo) {
        Map<Long, Integer> offsetConsumedCount = orderInfo.getOffsetConsumedCount();
        int minConsumedTimes = Integer.MAX_VALUE;

        if (offsetConsumedCount != null) {
            Set<Long> offsetSet = offsetConsumedCount.keySet();
            for (Long offset : offsetSet) {
                Integer consumedTimes = offsetConsumedCount.getOrDefault(offset, 0);
                ExtraInfoUtil.buildQueueOffsetOrderCountInfo(orderInfoBuilder, topic, queueId, offset, consumedTimes);
                minConsumedTimes = Math.min(minConsumedTimes, consumedTimes);
            }
            if (offsetConsumedCount.size() != orderInfo.getOffsetList().size()) {
                minConsumedTimes = 0;
            }
        } else {
            minConsumedTimes = 0;
        }

        // 构建消息组级别的消费次数信息（扩展标准格式）
        ExtraInfoUtil.buildQueueIdOrderCountInfo(orderInfoBuilder, topic + "#" + messageGroup, queueId, minConsumedTimes);
    }

    /**
     * 更新锁释放时间戳
     */
    private void updateLockFreeTimestamp(String topic, String group, int queueId, MessageGroupAnalysis analysis) {
        if (consumerOrderInfoLockManager != null) {
            String key = buildKey(topic, group);
            ConcurrentHashMap<Integer, ConcurrentHashMap<String, ConsumerOrderInfoManager.OrderInfo>> queueMap = table.get(key);
            if (queueMap != null) {
                ConcurrentHashMap<String, ConsumerOrderInfoManager.OrderInfo> messageGroupMap = queueMap.get(queueId);
                if (messageGroupMap != null) {
                    // 为每个消息组更新锁释放时间戳
                    for (ConsumerOrderInfoManager.OrderInfo orderInfo : messageGroupMap.values()) {
                        consumerOrderInfoLockManager.updateLockFreeTimestamp(topic, group, queueId, orderInfo);
                    }
                }
            }
        }
    }

    @Override
    public boolean checkBlock(String attemptId, String topic, String group, int queueId, long invisibleTime) {
        // 需要通过 attemptId 获取即将要分发的消息偏移量列表
        // 然后分析这些消息的消息组，检查是否有被阻塞的消息组

        String key = buildKey(topic, group);

        ConcurrentHashMap<Integer, ConcurrentHashMap<String, ConsumerOrderInfoManager.OrderInfo>> queueMap = table.get(key);
        if (queueMap == null) {
            return false;
        }

        ConcurrentHashMap<String, ConsumerOrderInfoManager.OrderInfo> messageGroupMap = queueMap.get(queueId);
        if (messageGroupMap == null || messageGroupMap.isEmpty()) {
            return false;
        }

        // TODO: 这里需要根据 attemptId 获取即将要分发的消息偏移量列表
        // 然后读取这些消息，提取消息组，只检查相关的消息组是否被阻塞
        //
        // 理想的实现步骤：
        // 1. 根据 attemptId 获取即将要分发的消息偏移量列表
        // 2. 读取这些消息，提取消息组
        // 3. 只检查这些消息组是否有未完成的消息（被阻塞状态）
        //
        // 当前简化实现：检查所有消息组的阻塞状态
        for (ConsumerOrderInfoManager.OrderInfo orderInfo : messageGroupMap.values()) {
            if (orderInfo.needBlock(attemptId, invisibleTime)) {
                return true;
            }
        }

        return false;
    }

    /**
     * 改进版本的 checkBlock 方法，根据具体的消息偏移量列表来检查阻塞状态
     *
     * @param attemptId          尝试ID
     * @param topic              主题
     * @param group              消费组
     * @param queueId            队列ID
     * @param invisibleTime      不可见时间
     * @param msgQueueOffsetList 即将要分发的消息偏移量列表
     * @return 是否需要阻塞
     */
    public boolean checkBlock(String attemptId, String topic, String group, int queueId,
        long invisibleTime, List<Long> msgQueueOffsetList) {
        if (msgQueueOffsetList == null || msgQueueOffsetList.isEmpty()) {
            return false;
        }

        String key = buildKey(topic, group);

        ConcurrentHashMap<Integer, ConcurrentHashMap<String, ConsumerOrderInfoManager.OrderInfo>> queueMap = table.get(key);
        if (queueMap == null) {
            return false;
        }

        ConcurrentHashMap<String, ConsumerOrderInfoManager.OrderInfo> messageGroupMap = queueMap.get(queueId);
        if (messageGroupMap == null || messageGroupMap.isEmpty()) {
            return false;
        }

        try {
            // 分析即将要分发的消息的消息组
            MessageGroupAnalysis analysis = analyzeMessageGroups(topic, group, queueId, msgQueueOffsetList);

            // 检查这些消息组是否有被阻塞的
            for (String messageGroup : analysis.getMessageGroupOffsets().keySet()) {
                ConsumerOrderInfoManager.OrderInfo orderInfo = messageGroupMap.get(messageGroup);
                if (orderInfo != null && orderInfo.needBlock(attemptId, invisibleTime)) {
                    // 发现有被阻塞的消息组，需要阻塞当前请求
                    log.debug("Message group {} is blocked for topic={}, group={}, queueId={}, attemptId={}",
                        messageGroup, topic, group, queueId, attemptId);
                    return true;
                }
            }

            return false;
        } catch (Exception e) {
            log.error("Failed to check block for message groups: topic={}, group={}, queueId={}, attemptId={}",
                topic, group, queueId, attemptId, e);
            // 出现异常时，为了安全起见，阻塞请求
            return true;
        }
    }

    @Override
    public long commitAndNext(String topic, String group, int queueId, long queueOffset, long popTime) {
        String key = buildKey(topic, group);

        ConcurrentHashMap<Integer, ConcurrentHashMap<String, ConsumerOrderInfoManager.OrderInfo>> queueMap = table.get(key);
        if (queueMap == null) {
            return queueOffset + 1;
        }

        ConcurrentHashMap<String, ConsumerOrderInfoManager.OrderInfo> messageGroupMap = queueMap.get(queueId);
        if (messageGroupMap == null) {
            log.warn("MessageGroup map is null, {}, {}", key, queueOffset);
            return queueOffset + 1;
        }

        // 需要根据消息偏移量确定消息组
        String messageGroup = getMessageGroupByOffset(topic, group, queueId, queueOffset);
        if (messageGroup == null) {
            log.warn("Cannot determine message group for offset {}, {}, {}", key, queueId, queueOffset);
            return queueOffset + 1;
        }

        ConsumerOrderInfoManager.OrderInfo orderInfo = messageGroupMap.get(messageGroup);
        if (orderInfo == null) {
            log.warn("OrderInfo is null for message group {}, {}, {}", key, messageGroup, queueOffset);
            return queueOffset + 1;
        }

        List<Long> offsetList = orderInfo.getOffsetList();
        if (offsetList == null || offsetList.isEmpty()) {
            log.warn("OrderInfo offset list is empty, {}, {}, {}", key, messageGroup, queueOffset);
            return -1;
        }

        // 验证 popTime
        if (popTime != orderInfo.getPopTime()) {
            log.warn("popTime mismatch for message group: key={}, messageGroup={}, offset={}, orderInfo={}, popTime={}",
                key, messageGroup, queueOffset, orderInfo, popTime);
            return -2;
        }

        // 查找偏移量在列表中的位置
        long firstOffset = offsetList.get(0);
        int offsetIndex = -1;

        for (int i = 0; i < offsetList.size(); i++) {
            long currentOffset = (i == 0) ? firstOffset : firstOffset + offsetList.get(i);
            if (currentOffset == queueOffset) {
                offsetIndex = i;
                break;
            }
        }

        if (offsetIndex == -1) {
            log.warn("Offset not found in message group: key={}, messageGroup={}, offset={}, orderInfo={}",
                key, messageGroup, queueOffset, orderInfo);
            return -1;
        }

        // 设置对应位的ACK标记
        orderInfo.setCommitOffsetBit(orderInfo.getCommitOffsetBit() | (1L << offsetIndex));

        // 计算下一个消费偏移量
        long nextOffset = orderInfo.getNextOffset();

        // 更新锁释放时间戳
        if (consumerOrderInfoLockManager != null) {
            consumerOrderInfoLockManager.updateLockFreeTimestamp(topic, group, queueId, orderInfo);
        }

        return nextOffset;
    }

    /**
     * 根据消息偏移量获取消息组
     */
    private String getMessageGroupByOffset(String topic, String group, int queueId, long queueOffset) {
        try {
            // 读取单个消息来确定消息组
            CompletableFuture<GetMessageResult> future = brokerController.getMessageStore()
                .getMessageAsync(group, topic, queueId, queueOffset, 1, null);

            GetMessageResult result = future.get();
            if (result != null && result.getStatus() == GetMessageStatus.FOUND) {
                for (SelectMappedBufferResult bufferResult : result.getMessageMapedList()) {
                    try {
                        List<MessageExt> messageExtList = MessageDecoder.decodesBatch(
                            bufferResult.getByteBuffer(), true, false, true);

                        for (MessageExt messageExt : messageExtList) {
                            if (messageExt.getQueueOffset() == queueOffset) {
                                return extractMessageGroup(messageExt);
                            }
                        }
                    } finally {
                        bufferResult.release();
                    }
                }
            }
        } catch (Exception e) {
            log.error("Failed to get message group by offset: topic={}, group={}, queueId={}, offset={}",
                topic, group, queueId, queueOffset, e);
        }

        return DEFAULT_MESSAGE_GROUP;
    }

    @Override
    public void updateNextVisibleTime(String topic, String group, int queueId, long queueOffset,
        long popTime, long nextVisibleTime) {
        String key = buildKey(topic, group);

        ConcurrentHashMap<Integer, ConcurrentHashMap<String, ConsumerOrderInfoManager.OrderInfo>> queueMap = table.get(key);
        if (queueMap == null) {
            log.warn("Queue map is null for updateNextVisibleTime: key={}, queueOffset={}, queueId={}",
                key, queueOffset, queueId);
            return;
        }

        ConcurrentHashMap<String, ConsumerOrderInfoManager.OrderInfo> messageGroupMap = queueMap.get(queueId);
        if (messageGroupMap == null) {
            log.warn("Message group map is null for updateNextVisibleTime: key={}, queueOffset={}, queueId={}",
                key, queueOffset, queueId);
            return;
        }

        // 根据偏移量确定消息组
        String messageGroup = getMessageGroupByOffset(topic, group, queueId, queueOffset);
        if (messageGroup == null) {
            log.warn("Cannot determine message group for updateNextVisibleTime: key={}, queueOffset={}, queueId={}",
                key, queueOffset, queueId);
            return;
        }

        ConsumerOrderInfoManager.OrderInfo orderInfo = messageGroupMap.get(messageGroup);
        if (orderInfo == null) {
            log.warn("OrderInfo is null for updateNextVisibleTime: key={}, messageGroup={}, queueOffset={}, queueId={}",
                key, messageGroup, queueOffset, queueId);
            return;
        }

        // 验证 popTime
        if (popTime != orderInfo.getPopTime()) {
            log.warn("popTime mismatch for updateNextVisibleTime: key={}, messageGroup={}, queueOffset={}, orderInfo={}, popTime={}",
                key, messageGroup, queueOffset, orderInfo, popTime);
            return;
        }

        // 更新指定偏移量的下次可见时间
        orderInfo.updateOffsetNextVisibleTime(queueOffset, nextVisibleTime);

        // 更新锁释放时间戳
        if (consumerOrderInfoLockManager != null) {
            consumerOrderInfoLockManager.updateLockFreeTimestamp(topic, group, queueId, orderInfo);
        }
    }

    @Override
    public void clearBlock(String topic, String group, int queueId) {
        String key = buildKey(topic, group);

        ConcurrentHashMap<Integer, ConcurrentHashMap<String, ConsumerOrderInfoManager.OrderInfo>> queueMap = table.get(key);
        if (queueMap != null) {
            queueMap.remove(queueId);
        }

        // 清除锁管理器中的相关信息
        if (consumerOrderInfoLockManager != null) {
            consumerOrderInfoLockManager.clearLock(topic, group, queueId);
        }
    }

    @Override
    public String getControllerType() {
        return CONTROLLER_TYPE;
    }

    @Override
    public void start() {
        if (consumerOrderInfoLockManager != null) {
            consumerOrderInfoLockManager.start();
        }
        log.info("MessageGroupOrderlyConsumeController started");
    }

    @Override
    public void shutdown() {
        if (consumerOrderInfoLockManager != null) {
            consumerOrderInfoLockManager.shutdown();
        }
        log.info("MessageGroupOrderlyConsumeController shutdown");
    }

    /**
     * 构建Topic和Group的组合键
     */
    private static String buildKey(String topic, String group) {
        return topic + TOPIC_GROUP_SEPARATOR + group;
    }

    /**
     * 消息组分析结果
     */
    private static class MessageGroupAnalysis {
        private Map<String, List<Long>> messageGroupOffsets = new HashMap<>();

        public Map<String, List<Long>> getMessageGroupOffsets() {
            return messageGroupOffsets;
        }

        public void setMessageGroupOffsets(Map<String, List<Long>> messageGroupOffsets) {
            this.messageGroupOffsets = messageGroupOffsets;
        }
    }
}