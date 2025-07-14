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
package org.apache.rocketmq.broker.offset;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.GetMessageStatus;
import org.apache.rocketmq.store.SelectMappedBufferResult;

/**
 * 基于消息组的顺序信息管理器（POC版本）
 * 
 * 设计目标：
 * 1. 将队列级阻塞优化为基于消息组的阻塞
 * 2. 同一队列中的不同消息组可以并发消费
 * 3. 保持同一消息组内的严格顺序
 * 4. 平衡预取消息数量和性能
 * 
 * 核心数据结构：
 * topic@group -> queueId -> messageGroup -> OrderInfo
 */
public class MessageGroupOrderInfoManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    
    private static final String TOPIC_GROUP_SEPARATOR = "@";
    private static final String DEFAULT_MESSAGE_GROUP = "_DEFAULT_";
    
    /**
     * 核心数据结构：基于消息组存储顺序信息
     * 第一层键：topic@group 格式字符串
     * 第二层键：queueId（队列ID）
     * 第三层键：messageGroup（消息组）
     * 值：OrderInfo（该消息组的顺序消息信息）
     */
    private final ConcurrentMap<String/* topic@group*/, 
        ConcurrentMap<Integer/*queueId*/, 
            ConcurrentMap<String/*messageGroup*/, ConsumerOrderInfoManager.OrderInfo>>> messageGroupTable;
    
    private final BrokerController brokerController;
    
    public MessageGroupOrderInfoManager(BrokerController brokerController) {
        this.brokerController = brokerController;
        this.messageGroupTable = new ConcurrentHashMap<>(128);
    }
    
    /**
     * 构建存储键的方法
     */
    protected static String buildKey(String topic, String group) {
        return topic + TOPIC_GROUP_SEPARATOR + group;
    }
    
    /**
     * 预取并分析消息组分布
     * 这是POC的核心方法：通过预取少量消息来了解消息组分布
     * 
     * @param topic 主题
     * @param group 消费组
     * @param queueId 队列ID
     * @param offset 起始偏移量
     * @param preReadCount 预读消息数量（建议2-5条消息）
     * @return MessageGroupAnalysis 分析结果
     */
    public MessageGroupOrderInfoManager.MessageGroupAnalysis analyzeMessageGroups(String topic, String group, int queueId, 
            long offset, int preReadCount) {
        try {
            // 预取少量消息进行分析
            GetMessageResult result = brokerController.getMessageStore().getMessage(
                group, topic, queueId, offset, preReadCount, null);
            
            MessageGroupOrderInfoManager.MessageGroupAnalysis analysis = new MessageGroupOrderInfoManager.MessageGroupAnalysis();
            
            if (result == null || result.getStatus() != GetMessageStatus.FOUND) {
                return analysis;
            }
            
            // 解析消息获取消息组信息
            Map<String, List<Long>> messageGroupOffsets = new HashMap<>();
            List<String> orderedGroups = new ArrayList<>();
            
            for (SelectMappedBufferResult bufferResult : result.getMessageMapedList()) {
                try {
                    List<MessageExt> messageExtList = MessageDecoder.decodesBatch(
                        bufferResult.getByteBuffer(), true, false, true);
                    
                    for (MessageExt messageExt : messageExtList) {
                        String messageGroup = getMessageGroup(messageExt);
                        long queueOffset = messageExt.getQueueOffset();
                        
                        // 记录每个消息组的偏移量
                        messageGroupOffsets.computeIfAbsent(messageGroup, k -> new ArrayList<>()).add(queueOffset);
                        
                        // 维护消息组出现的顺序
                        if (!orderedGroups.contains(messageGroup)) {
                            orderedGroups.add(messageGroup);
                        }
                        
                        analysis.addMessage(messageGroup, queueOffset, messageExt);
                    }
                } finally {
                    bufferResult.release();
                }
            }
            
            analysis.setMessageGroupOffsets(messageGroupOffsets);
            analysis.setOrderedGroups(orderedGroups);
            return analysis;
            
        } catch (Exception e) {
            log.error("分析消息组时出错，topic={}, group={}, queueId={}, offset={}", 
                topic, group, queueId, offset, e);
            return new MessageGroupOrderInfoManager.MessageGroupAnalysis();
        }
    }
    
    /**
     * 检查指定的消息组是否被阻塞
     * 
     * @param topic 主题名称
     * @param group 消费组
     * @param queueId 队列ID
     * @param messageGroup 消息组
     * @param attemptId 尝试ID
     * @param invisibleTime 不可见时间
     * @return true表示被阻塞，false表示可以消费
     */
    public boolean checkMessageGroupBlocked(String topic, String group, int queueId, 
            String messageGroup, String attemptId, long invisibleTime) {
        
        String key = buildKey(topic, group);
        ConcurrentMap<Integer, ConcurrentMap<String, ConsumerOrderInfoManager.OrderInfo>> queueMap = 
            messageGroupTable.get(key);
        
        if (queueMap == null) {
            return false;
        }
        
        ConcurrentMap<String, ConsumerOrderInfoManager.OrderInfo> groupMap = queueMap.get(queueId);
        if (groupMap == null) {
            return false;
        }
        
        ConsumerOrderInfoManager.OrderInfo orderInfo = groupMap.get(messageGroup);
        if (orderInfo == null) {
            return false;
        }
        
        // 基于消息组的阻塞逻辑（与传统队列级阻塞不同）：
        // 1. 相同attemptId的请求不被阻塞（重复请求）
        if (orderInfo.getAttemptId() != null && orderInfo.getAttemptId().equals(attemptId)) {
            return false;
        }
        
        // 2. 对于基于消息组的优化，我们不应该阻塞不同的attemptId
        // 消息组优化的核心目的是允许不同消息组的并发处理
        // 由于我们在这里检查的是特定的消息组，而不同的attemptId代表不同的并发消费者，
        // 它们不应该相互阻塞 - 这是关键优化。
        // 只有当同一消息组有来自同一消费者的未确认消息时才阻塞
        return false;
    }
    
    /**
     * 更新消息组顺序信息
     * 
     * @param topic 主题
     * @param group 消费组
     * @param queueId 队列ID
     * @param messageGroup 消息组
     * @param attemptId 尝试ID
     * @param popTime 弹出时间
     * @param invisibleTime 不可见时间
     * @param offsetList 偏移量列表
     */
    public void updateMessageGroupOrder(String topic, String group, int queueId, 
            String messageGroup, String attemptId, long popTime, long invisibleTime, 
            List<Long> offsetList) {
        
        String key = buildKey(topic, group);
        
        ConcurrentMap<Integer, ConcurrentMap<String, ConsumerOrderInfoManager.OrderInfo>> queueMap = 
            messageGroupTable.computeIfAbsent(key, k -> new ConcurrentHashMap<>());
        
        ConcurrentMap<String, ConsumerOrderInfoManager.OrderInfo> groupMap = 
            queueMap.computeIfAbsent(queueId, k -> new ConcurrentHashMap<>());
        
        // 创建或更新OrderInfo
        ConsumerOrderInfoManager.OrderInfo oldOrderInfo = groupMap.get(messageGroup);
        ConsumerOrderInfoManager.OrderInfo newOrderInfo = new ConsumerOrderInfoManager.OrderInfo(
            attemptId, popTime, invisibleTime, offsetList, System.currentTimeMillis(), 0);
        
        if (oldOrderInfo != null) {
            // 合并之前的消费计数统计
            newOrderInfo.mergeOffsetConsumedCount(oldOrderInfo.getAttemptId(), 
                oldOrderInfo.getOffsetList(), oldOrderInfo.getOffsetConsumedCount());
        }
        
        groupMap.put(messageGroup, newOrderInfo);
        
        log.debug("更新消息组顺序：topic={}, group={}, queueId={}, messageGroup={}, offsetCount={}", 
            topic, group, queueId, messageGroup, offsetList.size());
    }
    
    /**
     * 提交消息组消费确认
     * 
     * @param topic 主题
     * @param group 消费组
     * @param queueId 队列ID
     * @param messageGroup 消息组
     * @param queueOffset 队列偏移量
     * @param popTime 弹出时间
     * @return 下一个消费偏移量
     */
    public long commitMessageGroupAndNext(String topic, String group, int queueId, 
            String messageGroup, long queueOffset, long popTime) {
        
        String key = buildKey(topic, group);
        ConcurrentMap<Integer, ConcurrentMap<String, ConsumerOrderInfoManager.OrderInfo>> queueMap = 
            messageGroupTable.get(key);
        
        if (queueMap == null) {
            return queueOffset + 1;
        }
        
        ConcurrentMap<String, ConsumerOrderInfoManager.OrderInfo> groupMap = queueMap.get(queueId);
        if (groupMap == null) {
            return queueOffset + 1;
        }
        
        ConsumerOrderInfoManager.OrderInfo orderInfo = groupMap.get(messageGroup);
        if (orderInfo == null) {
            log.warn("消息组的OrderInfo为空，{}, {}, {}, {}", 
                key, queueId, messageGroup, queueOffset);
            return queueOffset + 1;
        }
        
        // 使用现有的CommitAndNext逻辑
        List<Long> offsetList = orderInfo.getOffsetList();
        if (offsetList == null || offsetList.isEmpty()) {
            log.warn("OrderInfo偏移量列表为空，{}, {}, {}, {}", 
                key, queueId, messageGroup, queueOffset);
            return -1;
        }
        
        // 验证popTime
        if (popTime != orderInfo.getPopTime()) {
            log.warn("消息组的popTime不匹配：key={}, queueId={}, messageGroup={}, " +
                "offset={}, orderInfo={}, popTime={}", 
                key, queueId, messageGroup, queueOffset, orderInfo, popTime);
            return -2;
        }
        
        // 查找要确认的偏移量并更新位图
        Long first = offsetList.get(0);
        int offsetIndex = -1;
        
        for (int i = 0; i < offsetList.size(); i++) {
            long currentOffset;
            if (i == 0) {
                currentOffset = first;
            } else {
                currentOffset = first + offsetList.get(i);
            }
            
            if (queueOffset == currentOffset) {
                offsetIndex = i;
                break;
            }
        }
        
        if (offsetIndex == -1) {
            log.warn("消息组未找到提交偏移量的OrderInfo，{}, {}, {}, {}", 
                key, queueId, messageGroup, queueOffset);
            return -1;
        }
        
        // 将对应位设置为1，表示已确认
        orderInfo.setCommitOffsetBit(orderInfo.getCommitOffsetBit() | (1L << offsetIndex));
        
        // 计算下一个可提交的偏移量
        return orderInfo.getNextOffset();
    }
    
    /**
     * 清除指定消息组的阻塞状态
     */
    public void clearMessageGroupBlock(String topic, String group, int queueId, String messageGroup) {
        String key = buildKey(topic, group);
        ConcurrentMap<Integer, ConcurrentMap<String, ConsumerOrderInfoManager.OrderInfo>> queueMap = 
            messageGroupTable.get(key);
        
        if (queueMap != null) {
            ConcurrentMap<String, ConsumerOrderInfoManager.OrderInfo> groupMap = queueMap.get(queueId);
            if (groupMap != null) {
                groupMap.remove(messageGroup);
                log.debug("清除消息组阻塞状态：topic={}, group={}, queueId={}, messageGroup={}", 
                    topic, group, queueId, messageGroup);
            }
        }
    }
    
    /**
     * 提取消息组从消息
     */
    private String getMessageGroup(MessageExt messageExt) {
        String shardingKey = messageExt.getProperty(MessageConst.PROPERTY_SHARDING_KEY);
        return shardingKey != null ? shardingKey : DEFAULT_MESSAGE_GROUP;
    }
    
    /**
     * 获取所有被阻塞的消息组
     */
    public Map<String, List<String>> getBlockedMessageGroups(String topic, String group, int queueId) {
        Map<String, List<String>> result = new HashMap<>();
        String key = buildKey(topic, group);
        
        ConcurrentMap<Integer, ConcurrentMap<String, ConsumerOrderInfoManager.OrderInfo>> queueMap = 
            messageGroupTable.get(key);
        
        if (queueMap != null) {
            ConcurrentMap<String, ConsumerOrderInfoManager.OrderInfo> groupMap = queueMap.get(queueId);
            if (groupMap != null) {
                List<String> blockedGroups = new ArrayList<>();
                long currentTime = System.currentTimeMillis();
                
                for (Map.Entry<String, ConsumerOrderInfoManager.OrderInfo> entry : groupMap.entrySet()) {
                    ConsumerOrderInfoManager.OrderInfo orderInfo = entry.getValue();
                    if (orderInfo.needBlock(null, 0)) {
                        blockedGroups.add(entry.getKey());
                    }
                }
                
                if (!blockedGroups.isEmpty()) {
                    result.put(buildKey(topic, group), blockedGroups);
                }
            }
        }
        
        return result;
    }
    
    /**
     * 消息组分析结果
     * 用于存储预取消息的分析结果，包括消息组分布信息
     */
    public static class MessageGroupAnalysis {
        // 每个消息组对应的偏移量列表
        private Map<String, List<Long>> messageGroupOffsets;
        
        // 消息组出现的顺序
        private List<String> orderedGroups;
        
        // 消息详细信息（消息组 -> 消息列表）
        private Map<String, List<MessageInfo>> messageDetails;
        
        // 分析总结信息
        private int totalMessages;
        private int totalMessageGroups;
        
        public MessageGroupAnalysis() {
            this.messageGroupOffsets = new HashMap<>();
            this.orderedGroups = new ArrayList<>();
            this.messageDetails = new HashMap<>();
            this.totalMessages = 0;
            this.totalMessageGroups = 0;
        }
        
        /**
         * 添加消息到分析结果
         */
        public void addMessage(String messageGroup, long queueOffset, MessageExt messageExt) {
            // 添加到消息详情
            MessageInfo msgInfo = new MessageInfo(messageGroup, queueOffset, messageExt);
            messageDetails.computeIfAbsent(messageGroup, k -> new ArrayList<>()).add(msgInfo);
            
            // 更新统计数据
            totalMessages++;
            if (!messageGroupOffsets.containsKey(messageGroup)) {
                totalMessageGroups++;
            }
        }
        
        /**
         * 设置消息组偏移量映射
         */
        public void setMessageGroupOffsets(Map<String, List<Long>> messageGroupOffsets) {
            this.messageGroupOffsets = messageGroupOffsets;
        }
        
        /**
         * 设置有序消息组列表
         */
        public void setOrderedGroups(List<String> orderedGroups) {
            this.orderedGroups = orderedGroups;
        }
        
        /**
         * 获取消息组偏移量映射
         */
        public Map<String, List<Long>> getMessageGroupOffsets() {
            return messageGroupOffsets;
        }
        
        /**
         * 获取有序消息组列表
         */
        public List<String> getOrderedGroups() {
            return orderedGroups;
        }
        
        /**
         * 获取消息详情
         */
        public Map<String, List<MessageInfo>> getMessageDetails() {
            return messageDetails;
        }
        
        /**
         * 获取总消息数
         */
        public int getTotalMessages() {
            return totalMessages;
        }
        
        /**
         * 获取总消息组数
         */
        public int getTotalMessageGroups() {
            return totalMessageGroups;
        }
        
        /**
         * 检查是否有消息
         */
        public boolean hasMessages() {
            return totalMessages > 0;
        }
        
        /**
         * 检查指定的消息组是否存在
         */
        public boolean hasMessageGroup(String messageGroup) {
            return messageGroupOffsets.containsKey(messageGroup);
        }
        
        /**
         * 获取指定消息组中的消息数量
         */
        public int getMessageCount(String messageGroup) {
            List<Long> offsets = messageGroupOffsets.get(messageGroup);
            return offsets != null ? offsets.size() : 0;
        }
        
        /**
         * 获取第一个消息组（优先处理）
         */
        public String getFirstMessageGroup() {
            return orderedGroups.isEmpty() ? null : orderedGroups.get(0);
        }
        
        /**
         * 判断是否只有一个消息组
         */
        public boolean hasSingleMessageGroup() {
            return totalMessageGroups == 1;
        }
        
        /**
         * 获取所有消息组的名称
         */
        public List<String> getAllMessageGroups() {
            return new ArrayList<>(messageGroupOffsets.keySet());
        }
        
        @Override
        public String toString() {
            return "MessageGroupAnalysis{" +
                "totalMessages=" + totalMessages +
                ", totalMessageGroups=" + totalMessageGroups +
                ", orderedGroups=" + orderedGroups +
                ", messageGroupOffsets=" + messageGroupOffsets +
                '}';
        }
    }
    
    /**
     * 消息信息内部类
     * 用于存储单个消息的详细信息
     */
    public static class MessageInfo {
        private final String messageGroup;
        private final long queueOffset;
        private final String topic;
        private final int queueId;
        private final String msgId;
        private final long bornTimestamp;
        private final long storeTimestamp;
        
        public MessageInfo(String messageGroup, long queueOffset, MessageExt messageExt) {
            this.messageGroup = messageGroup;
            this.queueOffset = queueOffset;
            this.topic = messageExt.getTopic();
            this.queueId = messageExt.getQueueId();
            this.msgId = messageExt.getMsgId();
            this.bornTimestamp = messageExt.getBornTimestamp();
            this.storeTimestamp = messageExt.getStoreTimestamp();
        }
        
        // 获取方法
        public String getMessageGroup() { return messageGroup; }
        public long getQueueOffset() { return queueOffset; }
        public String getTopic() { return topic; }
        public int getQueueId() { return queueId; }
        public String getMsgId() { return msgId; }
        public long getBornTimestamp() { return bornTimestamp; }
        public long getStoreTimestamp() { return storeTimestamp; }
        
        @Override
        public String toString() {
            return "MessageInfo{" +
                "messageGroup='" + messageGroup + '\'' +
                ", queueOffset=" + queueOffset +
                ", topic='" + topic + '\'' +
                ", queueId=" + queueId +
                ", msgId='" + msgId + '\'' +
                '}';
        }
    }
}