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

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.offset.MessageGroupOrderInfoManager;
import org.apache.rocketmq.broker.processor.PopMessageProcessorWithMessageGroup;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.header.PopMessageRequestHeader;
import org.apache.rocketmq.store.GetMessageResult;

/**
 * 基于Message Group的Pop消息优化示例 (POC)
 * 
 * 本示例展示如何使用基于Message Group的优化来提升顺序消息的并发处理能力
 * 
 * ## 核心优化点
 * 
 * ### 传统方式的问题
 * - 整个队列级别的阻塞：只要队列中有未确认的消息就阻塞整个队列
 * - 不同Message Group之间无法并发处理
 * - 降低了系统的并发处理能力
 * 
 * ### 基于Message Group的优化
 * - 细粒度锁：只阻塞相同Message Group的消息
 * - 并发处理：不同Message Group可以并发处理
 * - 智能预取：通过预取少量消息分析Message Group分布
 * 
 * ## 使用场景
 * 
 * ### 适用场景
 * 1. 顺序消息中有多个不同的Message Group
 * 2. 不同Message Group之间没有依赖关系
 * 3. 希望提升并发处理能力
 * 
 * ### 性能提升预期
 * - 多Message Group场景下并发度提升N倍（N为Message Group数量）
 * - 减少消息处理延迟
 * - 提升系统吞吐量
 */
public class MessageGroupPopExample {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_POP_LOGGER_NAME);
    
    private final BrokerController brokerController;
    private final PopMessageProcessorWithMessageGroup messageGroupProcessor;
    
    public MessageGroupPopExample(BrokerController brokerController) {
        this.brokerController = brokerController;
        this.messageGroupProcessor = new PopMessageProcessorWithMessageGroup(brokerController);
    }
    
    /**
     * 示例1：传统方式 vs Message Group优化方式的对比
     */
    public void demonstratePerformanceComparison() {
        String topic = "OrderTopic";
        String group = "OrderConsumerGroup";
        int queueId = 0;
        
        log.info("=== 基于Message Group的Pop消息优化演示 ===");
        
        // 创建请求头
        PopMessageRequestHeader requestHeader = createRequestHeader(topic, group, queueId);
        
        try {
            // 使用优化后的处理器
            long startTime = System.currentTimeMillis();
            CompletableFuture<GetMessageResult> future = messageGroupProcessor.popMsgFromQueueWithMessageGroup(
                requestHeader, topic, group, queueId, 0L, 10, null);
            
            GetMessageResult result = future.get();
            long endTime = System.currentTimeMillis();
            
            log.info("Message Group优化方式 - 耗时: {}ms, 消息数: {}", 
                endTime - startTime, result.getMessageCount());
            
            // 展示Message Group分析结果
            demonstrateMessageGroupAnalysis(topic, group, queueId);
            
        } catch (Exception e) {
            log.error("演示过程出错", e);
        }
    }
    
    /**
     * 示例2：Message Group分析功能演示
     */
    public void demonstrateMessageGroupAnalysis(String topic, String group, int queueId) {
        log.info("=== Message Group分析功能演示 ===");
        
        try {
            MessageGroupOrderInfoManager mgManager = messageGroupProcessor.getMessageGroupOrderInfoManager();
            
            // 分析消息组分布
            MessageGroupOrderInfoManager.MessageGroupAnalysis analysis = 
                mgManager.analyzeMessageGroups(topic, group, queueId, 0L, 5);
            
            log.info("分析结果:");
            log.info("- 总消息数: {}", analysis.getTotalMessages());
            log.info("- Message Group数量: {}", analysis.getTotalMessageGroups());
            log.info("- Message Group列表: {}", analysis.getOrderedGroups());
            
            // 展示每个Message Group的消息数量
            for (String messageGroup : analysis.getOrderedGroups()) {
                int messageCount = analysis.getMessageCount(messageGroup);
                log.info("- Message Group '{}': {} 条消息", messageGroup, messageCount);
            }
            
        } catch (Exception e) {
            log.error("Message Group分析演示出错", e);
        }
    }
    
    /**
     * 示例3：并发处理多个Message Group
     */
    public void demonstrateConcurrentProcessing() {
        log.info("=== 并发处理多个Message Group演示 ===");
        
        String topic = "OrderTopic";
        String group = "OrderConsumerGroup"; 
        int queueId = 0;
        
        // 模拟3个不同的消费者处理不同的Message Group
        String[] attemptIds = {"consumer-1", "consumer-2", "consumer-3"};
        
        CompletableFuture<Void> allTasks = CompletableFuture.allOf(
            // 消费者1
            CompletableFuture.runAsync(() -> processMessageGroup(topic, group, queueId, attemptIds[0], 0L)),
            // 消费者2  
            CompletableFuture.runAsync(() -> processMessageGroup(topic, group, queueId, attemptIds[1], 10L)),
            // 消费者3
            CompletableFuture.runAsync(() -> processMessageGroup(topic, group, queueId, attemptIds[2], 20L))
        );
        
        try {
            long startTime = System.currentTimeMillis();
            allTasks.get(); // 等待所有任务完成
            long endTime = System.currentTimeMillis();
            
            log.info("并发处理完成 - 总耗时: {}ms", endTime - startTime);
            
        } catch (Exception e) {
            log.error("并发处理演示出错", e);
        }
    }
    
    /**
     * 示例4：Message Group阻塞检查
     */
    public void demonstrateBlockingCheck() {
        log.info("=== Message Group阻塞检查演示 ===");
        
        String topic = "OrderTopic";
        String group = "OrderConsumerGroup";
        int queueId = 0;
        String messageGroup = "group-1";
        
        // 检查Message Group是否被阻塞
        boolean isBlocked = messageGroupProcessor.isMessageGroupBlocked(
            topic, group, queueId, messageGroup, "test-attempt", 30000L);
        
        log.info("Message Group '{}' 阻塞状态: {}", messageGroup, isBlocked ? "已阻塞" : "可用");
        
        // 获取所有阻塞的Message Group
        MessageGroupOrderInfoManager mgManager = messageGroupProcessor.getMessageGroupOrderInfoManager();
        Map<String, List<String>> blockedGroups = mgManager.getBlockedMessageGroups(topic, group, queueId);
        
        log.info("当前阻塞的Message Group: {}", blockedGroups);
    }
    
    /**
     * 示例5：消息确认流程
     */
    public void demonstrateAckProcess() {
        log.info("=== 基于Message Group的消息确认演示 ===");
        
        String topic = "OrderTopic";
        String group = "OrderConsumerGroup";
        int queueId = 0;
        String messageGroup = "group-1";
        long queueOffset = 0L;
        long popTime = System.currentTimeMillis();
        
        try {
            // 确认消息
            CompletableFuture<Boolean> ackFuture = messageGroupProcessor.ackMessageWithMessageGroup(
                topic, group, queueId, queueOffset, messageGroup, popTime);
            
            Boolean ackResult = ackFuture.get();
            log.info("Message Group '{}' 消息确认结果: {}", messageGroup, ackResult ? "成功" : "失败");
            
        } catch (Exception e) {
            log.error("消息确认演示出错", e);
        }
    }
    
    /**
     * 辅助方法：处理单个Message Group
     */
    private void processMessageGroup(String topic, String group, int queueId, String attemptId, long offset) {
        try {
            PopMessageRequestHeader requestHeader = createRequestHeader(topic, group, queueId);
            requestHeader.setAttemptId(attemptId);
            
            log.info("开始处理Message Group - attemptId: {}, offset: {}", attemptId, offset);
            
            CompletableFuture<GetMessageResult> future = messageGroupProcessor.popMsgFromQueueWithMessageGroup(
                requestHeader, topic, group, queueId, offset, 5, null);
            
            GetMessageResult result = future.get();
            
            log.info("Message Group处理完成 - attemptId: {}, 消息数: {}", 
                attemptId, result.getMessageCount());
            
        } catch (Exception e) {
            log.error("处理Message Group出错 - attemptId: {}", attemptId, e);
        }
    }
    
    /**
     * 辅助方法：创建请求头
     */
    private PopMessageRequestHeader createRequestHeader(String topic, String group, int queueId) {
        PopMessageRequestHeader header = new PopMessageRequestHeader();
        header.setTopic(topic);
        header.setConsumerGroup(group);
        header.setQueueId(queueId);
        header.setAttemptId("demo-attempt-" + System.currentTimeMillis());
        header.setInvisibleTime(30000L); // 30秒
        header.setOrder(true); // 顺序消息
        header.setMaxMsgNums(10);
        return header;
    }
    
    /**
     * 运行完整演示
     */
    public void runCompleteDemo() {
        log.info("========================================");
        log.info("基于Message Group的Pop消息优化完整演示开始");
        log.info("========================================");
        
        // 1. 性能对比演示
        demonstratePerformanceComparison();
        
        // 2. 并发处理演示  
        demonstrateConcurrentProcessing();
        
        // 3. 阻塞检查演示
        demonstrateBlockingCheck();
        
        // 4. 消息确认演示
        demonstrateAckProcess();
        
        log.info("========================================");
        log.info("基于Message Group的Pop消息优化演示完成");
        log.info("========================================");
    }
}

/**
 * 配置说明和最佳实践
 * 
 * ## 配置参数调优
 * 
 * ### 预读消息数量 (PRE_READ_MESSAGE_COUNT)
 * - 默认值: 3
 * - 调优建议: 根据Message Group分布密度调整
 *   - Message Group较少时可以设置为 2-3
 *   - Message Group较多时可以设置为 5-8
 * 
 * ### 最大重试次数 (MAX_RETRY_COUNT)  
 * - 默认值: 2
 * - 调优建议: 根据业务容错要求调整
 * 
 * ## 性能优化建议
 * 
 * 1. **Message Group设计**
 *    - 保持Message Group数量适中（建议2-10个）
 *    - 每个Message Group的消息量相对均衡
 *    - 避免单个Message Group过大导致热点
 * 
 * 2. **消费者配置**
 *    - 根据Message Group数量配置消费者实例数
 *    - 适当调整批量大小和预取数量
 * 
 * 3. **监控指标**
 *    - Message Group级别的消费延迟
 *    - 阻塞状态监控
 *    - 并发处理效率
 * 
 * ## 兼容性说明
 * 
 * - 向后兼容：现有的顺序消息逻辑保持不变
 * - 渐进式升级：可以逐步开启Message Group优化
 * - 性能退化：在单Message Group场景下性能基本一致
 */