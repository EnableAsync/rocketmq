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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.offset.MessageGroupOrderInfoManager;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.header.PopMessageRequestHeader;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.GetMessageStatus;
import org.apache.rocketmq.store.MessageFilter;

/**
 * 基于Message Group优化的Pop消息处理器 (POC版本)
 * 
 * 核心优化：
 * 1. 预取少量消息分析message group分布
 * 2. 只阻塞相同message group的消息，不同group可并发处理
 * 3. 动态调整获取消息的策略
 * 4. 保持与原有PopMessageProcessor的兼容性
 */
public class PopMessageProcessorWithMessageGroup {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_POP_LOGGER_NAME);
    
    private final BrokerController brokerController;
    private final MessageGroupOrderInfoManager messageGroupOrderInfoManager;
    
    // POC配置参数
    private static final int PRE_READ_MESSAGE_COUNT = 3; // 预读消息数量
    private static final int MAX_RETRY_COUNT = 2; // 最大重试次数
    
    public PopMessageProcessorWithMessageGroup(BrokerController brokerController) {
        this.brokerController = brokerController;
        this.messageGroupOrderInfoManager = new MessageGroupOrderInfoManager(brokerController);
    }
    
    /**
     * 基于Message Group优化的Pop消息核心方法
     * 
     * 工作流程：
     * 1. 预取少量消息分析message group分布
     * 2. 检查各message group的阻塞状态
     * 3. 获取可用的message group的消息
     * 4. 更新message group的顺序信息
     * 
     * @param requestHeader Pop请求头
     * @param topic 主题
     * @param group 消费组
     * @param queueId 队列ID
     * @param offset 起始偏移量
     * @param batchSize 批量大小
     * @param messageFilter 消息过滤器
     * @return 异步获取消息结果
     */
    public CompletableFuture<GetMessageResult> popMsgFromQueueWithMessageGroup(
            PopMessageRequestHeader requestHeader, String topic, String group, int queueId, 
            long offset, int batchSize, MessageFilter messageFilter) {
        
        log.debug("PopWithMessageGroup: topic={}, group={}, queueId={}, offset={}, batchSize={}", 
            topic, group, queueId, offset, batchSize);
        
        return CompletableFuture.supplyAsync(() -> {
            try {
                // 第一步：预取消息分析message group分布
                MessageGroupOrderInfoManager.MessageGroupAnalysis analysis = 
                    messageGroupOrderInfoManager.analyzeMessageGroups(topic, group, queueId, offset, PRE_READ_MESSAGE_COUNT);
                
                if (!analysis.hasMessages()) {
                    log.debug("No messages found in analysis for topic={}, group={}, queueId={}", topic, group, queueId);
                    return createEmptyResult();
                }
                
                // 第二步：检查message group阻塞状态并选择可消费的group
                List<String> availableGroups = getAvailableMessageGroups(analysis, topic, group, queueId, 
                    requestHeader.getAttemptId(), requestHeader.getInvisibleTime());
                
                if (availableGroups.isEmpty()) {
                    log.debug("All message groups are blocked for topic={}, group={}, queueId={}", topic, group, queueId);
                    return createEmptyResult();
                }
                
                // 第三步：智能获取消息策略
                GetMessageResult finalResult = intelligentGetMessages(topic, group, queueId, offset, batchSize, 
                    messageFilter, analysis, availableGroups);
                
                // 第四步：更新message group顺序信息
                updateMessageGroupOrderInfo(requestHeader, topic, group, queueId, finalResult, analysis);
                
                log.info("PopWithMessageGroup completed: topic={}, group={}, queueId={}, messageCount={}, " +
                    "availableGroups={}, totalGroups={}", 
                    topic, group, queueId, finalResult.getMessageCount(), 
                    availableGroups.size(), analysis.getTotalMessageGroups());
                
                return finalResult;
                
            } catch (Exception e) {
                log.error("PopWithMessageGroup error: topic={}, group={}, queueId={}", topic, group, queueId, e);
                return createEmptyResult();
            }
        });
    }
    
    /**
     * 获取可用的message group列表
     * 只返回当前不被阻塞的message group
     */
    private List<String> getAvailableMessageGroups(MessageGroupOrderInfoManager.MessageGroupAnalysis analysis, 
            String topic, String group, int queueId, String attemptId, long invisibleTime) {
        
        List<String> availableGroups = new ArrayList<>();
        
        for (String messageGroup : analysis.getOrderedGroups()) {
            boolean isBlocked = messageGroupOrderInfoManager.checkMessageGroupBlocked(
                topic, group, queueId, messageGroup, attemptId, invisibleTime);
            
            if (!isBlocked) {
                availableGroups.add(messageGroup);
                log.debug("MessageGroup available: {}", messageGroup);
            } else {
                log.debug("MessageGroup blocked: {}", messageGroup);
            }
        }
        
        return availableGroups;
    }
    
    /**
     * 智能获取消息策略
     * 根据message group分析结果和可用性，决定如何获取消息
     */
    private GetMessageResult intelligentGetMessages(String topic, String group, int queueId, long offset, 
            int batchSize, MessageFilter messageFilter, MessageGroupOrderInfoManager.MessageGroupAnalysis analysis,
            List<String> availableGroups) {
        
        try {
            if (analysis.hasSingleMessageGroup() && !availableGroups.isEmpty()) {
                // 情况1：只有一个message group且可用，直接批量获取
                log.debug("Single message group scenario, batch fetching");
                return brokerController.getMessageStore().getMessage(group, topic, queueId, offset, batchSize, messageFilter);
                
            } else if (availableGroups.size() == analysis.getTotalMessageGroups()) {
                // 情况2：所有message group都可用，正常批量获取
                log.debug("All message groups available, batch fetching");
                return brokerController.getMessageStore().getMessage(group, topic, queueId, offset, batchSize, messageFilter);
                
            } else {
                // 情况3：部分message group被阻塞，需要选择性获取
                log.debug("Partial message groups available, selective fetching");
                return selectiveGetMessages(topic, group, queueId, offset, batchSize, messageFilter, 
                    analysis, availableGroups);
            }
            
        } catch (Exception e) {
            log.error("Error in intelligent get messages", e);
            return createEmptyResult();
        }
    }
    
    /**
     * 选择性获取消息
     * 当部分message group被阻塞时，只获取可用group的消息
     */
    private GetMessageResult selectiveGetMessages(String topic, String group, int queueId, long offset, 
            int batchSize, MessageFilter messageFilter, MessageGroupOrderInfoManager.MessageGroupAnalysis analysis,
            List<String> availableGroups) {
        
        // 计算第一个可用message group的消息数量
        String firstAvailableGroup = availableGroups.get(0);
        int targetCount = analysis.getMessageCount(firstAvailableGroup);
        
        // 限制获取数量，避免获取太多被阻塞的消息
        int adjustedBatchSize = Math.min(batchSize, Math.max(targetCount, PRE_READ_MESSAGE_COUNT * 2));
        
        log.debug("Selective fetch: firstAvailableGroup={}, targetCount={}, adjustedBatchSize={}", 
            firstAvailableGroup, targetCount, adjustedBatchSize);
        
        try {
            GetMessageResult result = brokerController.getMessageStore().getMessage(
                group, topic, queueId, offset, adjustedBatchSize, messageFilter);
            
            // TODO: 进一步优化 - 可以在这里过滤掉被阻塞的message group的消息
            // 这需要解析每条消息的message group，如果不在availableGroups中则跳过
            
            return result;
            
        } catch (Exception e) {
            log.error("Error in selective get messages", e);
            return createEmptyResult();
        }
    }
    
    /**
     * 更新message group的顺序信息
     */
    private void updateMessageGroupOrderInfo(PopMessageRequestHeader requestHeader, String topic, String group, 
            int queueId, GetMessageResult result, MessageGroupOrderInfoManager.MessageGroupAnalysis analysis) {
        
        if (result == null || result.getStatus() != GetMessageStatus.FOUND || result.getMessageQueueOffset().isEmpty()) {
            return;
        }
        
        try {
            // 按message group分组更新OrderInfo
            for (String messageGroup : analysis.getOrderedGroups()) {
                List<Long> groupOffsets = analysis.getMessageGroupOffsets().get(messageGroup);
                if (groupOffsets != null && !groupOffsets.isEmpty()) {
                    
                    // 只更新实际获取到的消息的OrderInfo
                    List<Long> actualOffsets = new ArrayList<>();
                    for (Long offset : groupOffsets) {
                        if (result.getMessageQueueOffset().contains(offset)) {
                            actualOffsets.add(offset);
                        }
                    }
                    
                    if (!actualOffsets.isEmpty()) {
                        messageGroupOrderInfoManager.updateMessageGroupOrder(
                            topic, group, queueId, messageGroup, requestHeader.getAttemptId(),
                            System.currentTimeMillis(), requestHeader.getInvisibleTime(), actualOffsets);
                        
                        log.debug("Updated order info for messageGroup={}, offsetCount={}", 
                            messageGroup, actualOffsets.size());
                    }
                }
            }
            
        } catch (Exception e) {
            log.error("Error updating message group order info", e);
        }
    }
    
    /**
     * 创建空的GetMessageResult
     */
    private GetMessageResult createEmptyResult() {
        GetMessageResult result = new GetMessageResult();
        result.setStatus(GetMessageStatus.NO_MESSAGE_IN_QUEUE);
        return result;
    }
    
    /**
     * 确认消息 - 基于message group的确认逻辑
     */
    public CompletableFuture<Boolean> ackMessageWithMessageGroup(String topic, String group, int queueId, 
            long queueOffset, String messageGroup, long popTime) {
        
        return CompletableFuture.supplyAsync(() -> {
            try {
                long nextOffset = messageGroupOrderInfoManager.commitMessageGroupAndNext(
                    topic, group, queueId, messageGroup, queueOffset, popTime);
                
                if (nextOffset > 0) {
                    // 更新消费偏移量
                    brokerController.getConsumerOffsetManager().commitOffset(
                        "MessageGroupAck", group, topic, queueId, nextOffset);
                    
                    log.debug("Ack message group: topic={}, group={}, queueId={}, messageGroup={}, " +
                        "offset={}, nextOffset={}", 
                        topic, group, queueId, messageGroup, queueOffset, nextOffset);
                    
                    return true;
                } else {
                    log.warn("Failed to ack message group: nextOffset={}", nextOffset);
                    return false;
                }
                
            } catch (Exception e) {
                log.error("Error in ack message with message group", e);
                return false;
            }
        });
    }
    
    /**
     * 获取message group管理器
     */
    public MessageGroupOrderInfoManager getMessageGroupOrderInfoManager() {
        return messageGroupOrderInfoManager;
    }
    
    /**
     * 检查指定message group是否被阻塞
     */
    public boolean isMessageGroupBlocked(String topic, String group, int queueId, 
            String messageGroup, String attemptId, long invisibleTime) {
        return messageGroupOrderInfoManager.checkMessageGroupBlocked(
            topic, group, queueId, messageGroup, attemptId, invisibleTime);
    }
}