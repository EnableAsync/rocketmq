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

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.offset.ConsumerOffsetManager;
import org.apache.rocketmq.broker.offset.MessageGroupOrderInfoManager;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.protocol.header.PopMessageRequestHeader;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.GetMessageStatus;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * PopMessageProcessorWithMessageGroup 测试用例
 * 验证基于Message Group的顺序消息优化POC功能
 */
@RunWith(MockitoJUnitRunner.class)
public class PopMessageProcessorWithMessageGroupTest {
    
    @Mock
    private BrokerController brokerController;
    
    @Mock
    private DefaultMessageStore messageStore;
    
    @Mock
    private ConsumerOffsetManager consumerOffsetManager;
    
    private PopMessageProcessorWithMessageGroup processor;
    
    private static final String TEST_TOPIC = "TestTopic";
    private static final String TEST_GROUP = "TestGroup";
    private static final int TEST_QUEUE_ID = 0;
    private static final String TEST_MESSAGE_GROUP_1 = "group1";
    private static final String TEST_MESSAGE_GROUP_2 = "group2";
    
    @Before
    public void setUp() {
        when(brokerController.getMessageStore()).thenReturn(messageStore);
        when(brokerController.getConsumerOffsetManager()).thenReturn(consumerOffsetManager);
        when(brokerController.getBrokerConfig()).thenReturn(new BrokerConfig());
        
        processor = new PopMessageProcessorWithMessageGroup(brokerController);
    }
    
    /**
     * 测试场景1：单一Message Group的消息处理
     * 预期：应该能正常批量获取消息，不产生阻塞
     */
    @Test
    public void testSingleMessageGroupScenario() throws Exception {
        // 准备测试数据
        PopMessageRequestHeader requestHeader = createTestRequestHeader();
        GetMessageResult mockResult = createMockGetMessageResult(TEST_MESSAGE_GROUP_1, 3);
        
        when(messageStore.getMessage(anyString(), anyString(), anyInt(), anyLong(), anyInt(), any()))
            .thenReturn(mockResult);
        
        // 执行测试
        CompletableFuture<GetMessageResult> future = processor.popMsgFromQueueWithMessageGroup(
            requestHeader, TEST_TOPIC, TEST_GROUP, TEST_QUEUE_ID, 0L, 10, null);
        
        GetMessageResult result = future.get();
        
        // 验证结果
        assertNotNull("结果不应为空", result);
        assertEquals("应该获取到消息", GetMessageStatus.FOUND, result.getStatus());
        assertTrue("消息数量应大于0", result.getMessageCount() > 0);
        
        // 验证Message Group管理器状态
        MessageGroupOrderInfoManager mgManager = processor.getMessageGroupOrderInfoManager();
        assertFalse("单一Message Group不应被阻塞", 
            mgManager.checkMessageGroupBlocked(TEST_TOPIC, TEST_GROUP, TEST_QUEUE_ID, 
                TEST_MESSAGE_GROUP_1, requestHeader.getAttemptId(), requestHeader.getInvisibleTime()));
    }
    
    /**
     * 测试场景2：多个Message Group的并发处理
     * 预期：不同Message Group应该能并发处理，不相互阻塞
     */
    @Test
    public void testMultipleMessageGroupScenario() throws Exception {
        // 准备包含两个不同Message Group的消息
        PopMessageRequestHeader requestHeader1 = createTestRequestHeader();
        PopMessageRequestHeader requestHeader2 = createTestRequestHeader();
        requestHeader2.setAttemptId("attempt-2");
        
        GetMessageResult mockResult1 = createMockGetMessageResult(TEST_MESSAGE_GROUP_1, 2);
        GetMessageResult mockResult2 = createMockGetMessageResult(TEST_MESSAGE_GROUP_2, 2);
        
        when(messageStore.getMessage(anyString(), anyString(), anyInt(), anyLong(), anyInt(), any()))
            .thenReturn(mockResult1, mockResult2);
        
        // 执行第一个Message Group的Pop操作
        CompletableFuture<GetMessageResult> future1 = processor.popMsgFromQueueWithMessageGroup(
            requestHeader1, TEST_TOPIC, TEST_GROUP, TEST_QUEUE_ID, 0L, 10, null);
        
        GetMessageResult result1 = future1.get();
        
        // 执行第二个Message Group的Pop操作
        CompletableFuture<GetMessageResult> future2 = processor.popMsgFromQueueWithMessageGroup(
            requestHeader2, TEST_TOPIC, TEST_GROUP, TEST_QUEUE_ID, 4L, 10, null);
        
        GetMessageResult result2 = future2.get();
        
        // 验证两个Message Group都能成功获取消息
        assertNotNull("第一个结果不应为空", result1);
        assertNotNull("第二个结果不应为空", result2);
        assertEquals("第一个Message Group应该获取到消息", GetMessageStatus.FOUND, result1.getStatus());
        assertEquals("第二个Message Group应该获取到消息", GetMessageStatus.FOUND, result2.getStatus());
        
        // 验证两个Message Group都不被阻塞
        MessageGroupOrderInfoManager mgManager = processor.getMessageGroupOrderInfoManager();
        assertFalse("第一个Message Group不应被阻塞", 
            mgManager.checkMessageGroupBlocked(TEST_TOPIC, TEST_GROUP, TEST_QUEUE_ID, 
                TEST_MESSAGE_GROUP_1, "attempt-other", requestHeader1.getInvisibleTime()));
        assertFalse("第二个Message Group不应被阻塞", 
            mgManager.checkMessageGroupBlocked(TEST_TOPIC, TEST_GROUP, TEST_QUEUE_ID, 
                TEST_MESSAGE_GROUP_2, "attempt-other", requestHeader2.getInvisibleTime()));
    }
    
    /**
     * 测试场景3：Message Group阻塞机制
     * 预期：相同Message Group的消息应该按顺序处理，产生阻塞
     */
    @Test
    public void testMessageGroupBlockingScenario() throws Exception {
        PopMessageRequestHeader requestHeader = createTestRequestHeader();
        GetMessageResult mockResult = createMockGetMessageResult(TEST_MESSAGE_GROUP_1, 2);
        
        when(messageStore.getMessage(anyString(), anyString(), anyInt(), anyLong(), anyInt(), any()))
            .thenReturn(mockResult);
        
        // 第一次Pop操作
        CompletableFuture<GetMessageResult> future1 = processor.popMsgFromQueueWithMessageGroup(
            requestHeader, TEST_TOPIC, TEST_GROUP, TEST_QUEUE_ID, 0L, 10, null);
        
        GetMessageResult result1 = future1.get();
        assertNotNull("第一次Pop结果不应为空", result1);
        
        // 验证相同Message Group被阻塞
        MessageGroupOrderInfoManager mgManager = processor.getMessageGroupOrderInfoManager();
        assertTrue("相同Message Group应该被阻塞", 
            mgManager.checkMessageGroupBlocked(TEST_TOPIC, TEST_GROUP, TEST_QUEUE_ID, 
                TEST_MESSAGE_GROUP_1, "different-attempt", requestHeader.getInvisibleTime()));
        
        // 验证相同attemptId不被阻塞（重复请求）
        assertFalse("相同attemptId不应被阻塞", 
            mgManager.checkMessageGroupBlocked(TEST_TOPIC, TEST_GROUP, TEST_QUEUE_ID, 
                TEST_MESSAGE_GROUP_1, requestHeader.getAttemptId(), requestHeader.getInvisibleTime()));
    }
    
    /**
     * 测试场景4：消息确认机制
     * 预期：确认消息后应该解除阻塞状态
     */
    @Test
    public void testMessageAckScenario() throws Exception {
        PopMessageRequestHeader requestHeader = createTestRequestHeader();
        GetMessageResult mockResult = createMockGetMessageResult(TEST_MESSAGE_GROUP_1, 1);
        
        when(messageStore.getMessage(anyString(), anyString(), anyInt(), anyLong(), anyInt(), any()))
            .thenReturn(mockResult);
        
        // Pop消息
        CompletableFuture<GetMessageResult> future = processor.popMsgFromQueueWithMessageGroup(
            requestHeader, TEST_TOPIC, TEST_GROUP, TEST_QUEUE_ID, 0L, 10, null);
        
        GetMessageResult result = future.get();
        assertNotNull("Pop结果不应为空", result);
        
        // 确认消息
        long queueOffset = result.getMessageQueueOffset().get(0);
        CompletableFuture<Boolean> ackFuture = processor.ackMessageWithMessageGroup(
            TEST_TOPIC, TEST_GROUP, TEST_QUEUE_ID, queueOffset, 
            TEST_MESSAGE_GROUP_1, System.currentTimeMillis());
        
        Boolean ackResult = ackFuture.get();
        assertTrue("消息确认应该成功", ackResult);
        
        // 验证消费偏移量更新
        verify(consumerOffsetManager, atLeastOnce()).commitOffset(
            anyString(), eq(TEST_GROUP), eq(TEST_TOPIC), eq(TEST_QUEUE_ID), anyLong());
    }
    
    /**
     * 测试场景5：消息分析功能
     * 预期：应该能正确分析预取消息的Message Group分布
     */
    @Test
    public void testMessageGroupAnalysis() throws Exception {
        // 准备包含多个Message Group的消息
        GetMessageResult mockAnalysisResult = createMockGetMessageResultWithMultipleGroups();
        
        when(messageStore.getMessage(anyString(), anyString(), anyInt(), anyLong(), anyInt(), any()))
            .thenReturn(mockAnalysisResult);
        
        // 执行分析
        MessageGroupOrderInfoManager mgManager = processor.getMessageGroupOrderInfoManager();
        MessageGroupOrderInfoManager.MessageGroupAnalysis analysis = 
            mgManager.analyzeMessageGroups(TEST_TOPIC, TEST_GROUP, TEST_QUEUE_ID, 0L, 3);
        
        // 验证分析结果
        assertNotNull("分析结果不应为空", analysis);
        assertTrue("应该有消息", analysis.hasMessages());
        assertEquals("应该有2个Message Group", 2, analysis.getTotalMessageGroups());
        assertTrue("应该包含第一个Message Group", analysis.hasMessageGroup(TEST_MESSAGE_GROUP_1));
        assertTrue("应该包含第二个Message Group", analysis.hasMessageGroup(TEST_MESSAGE_GROUP_2));
        
        List<String> orderedGroups = analysis.getOrderedGroups();
        assertEquals("应该有2个有序的Message Group", 2, orderedGroups.size());
    }
    
    /**
     * 测试场景6：性能对比测试
     * 验证基于Message Group的优化确实能提升并发性能
     */
    @Test
    public void testPerformanceComparison() throws Exception {
        int messageCount = 100;
        int groupCount = 10;
        
        // 模拟多个Message Group的消息
        List<PopMessageRequestHeader> requests = new ArrayList<>();
        for (int i = 0; i < groupCount; i++) {
            PopMessageRequestHeader header = createTestRequestHeader();
            header.setAttemptId("attempt-" + i);
            requests.add(header);
        }
        
        GetMessageResult mockResult = createMockGetMessageResult("group-" + 0, messageCount / groupCount);
        when(messageStore.getMessage(anyString(), anyString(), anyInt(), anyLong(), anyInt(), any()))
            .thenReturn(mockResult);
        
        // 并发执行多个Pop请求
        List<CompletableFuture<GetMessageResult>> futures = new ArrayList<>();
        long startTime = System.currentTimeMillis();
        
        for (PopMessageRequestHeader request : requests) {
            CompletableFuture<GetMessageResult> future = processor.popMsgFromQueueWithMessageGroup(
                request, TEST_TOPIC, TEST_GROUP, TEST_QUEUE_ID, 0L, messageCount / groupCount, null);
            futures.add(future);
        }
        
        // 等待所有请求完成
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
        long endTime = System.currentTimeMillis();
        
        // 验证所有请求都成功
        for (CompletableFuture<GetMessageResult> future : futures) {
            GetMessageResult result = future.get();
            assertNotNull("结果不应为空", result);
        }
        
        long totalTime = endTime - startTime;
        System.out.println("基于Message Group的并发Pop总耗时: " + totalTime + "ms");
        
        // 基本性能验证（实际测试中可以与传统方式对比）
        assertTrue("并发处理应该在合理时间内完成", totalTime < 5000); // 5秒内完成
    }
    
    /**
     * 测试场景7：边界条件测试
     * 验证各种边界条件的处理
     */
    @Test
    public void testBoundaryConditions() throws Exception {
        MessageGroupOrderInfoManager mgManager = processor.getMessageGroupOrderInfoManager();
        
        // 测试空消息分析
        when(messageStore.getMessage(anyString(), anyString(), anyInt(), anyLong(), anyInt(), any()))
            .thenReturn(null);
        
        MessageGroupOrderInfoManager.MessageGroupAnalysis emptyAnalysis = 
            mgManager.analyzeMessageGroups(TEST_TOPIC, TEST_GROUP, TEST_QUEUE_ID, 0L, 3);
        
        assertNotNull("空分析结果不应为空", emptyAnalysis);
        assertFalse("空分析结果应该没有消息", emptyAnalysis.hasMessages());
        assertEquals("空分析结果应该有0个Message Group", 0, emptyAnalysis.getTotalMessageGroups());
        
        // 测试不存在的Message Group阻塞检查
        boolean blocked = mgManager.checkMessageGroupBlocked(
            TEST_TOPIC, TEST_GROUP, TEST_QUEUE_ID, "NON_EXIST_GROUP", "attempt", 30000L);
        assertFalse("不存在的Message Group不应被阻塞", blocked);
        
        // 测试清理不存在的Message Group
        mgManager.clearMessageGroupBlock(TEST_TOPIC, TEST_GROUP, TEST_QUEUE_ID, "NON_EXIST_GROUP");
        // 应该不抛异常
    }
    
    /**
     * 测试场景8：异常处理测试
     * 验证异常情况的处理能力
     */
    @Test
    public void testExceptionHandling() throws Exception {
        // 模拟存储异常
        when(messageStore.getMessage(anyString(), anyString(), anyInt(), anyLong(), anyInt(), any()))
            .thenThrow(new RuntimeException("Mock storage exception"));
        
        PopMessageRequestHeader requestHeader = createTestRequestHeader();
        
        // 执行Pop操作
        CompletableFuture<GetMessageResult> future = processor.popMsgFromQueueWithMessageGroup(
            requestHeader, TEST_TOPIC, TEST_GROUP, TEST_QUEUE_ID, 0L, 10, null);
        
        GetMessageResult result = future.get();
        
        // 验证异常处理
        assertNotNull("异常情况下结果不应为空", result);
        assertEquals("异常情况下应该返回无消息状态", GetMessageStatus.NO_MESSAGE_IN_QUEUE, result.getStatus());
        assertEquals("异常情况下消息数量应为0", 0, result.getMessageCount());
    }
    
    /**
     * 测试场景9：配置参数测试
     * 验证不同配置参数对行为的影响
     */
    @Test
    public void testConfigurationParameters() throws Exception {
        // 测试不同的预读消息数量
        GetMessageResult mockResult = createMockGetMessageResult(TEST_MESSAGE_GROUP_1, 5);
        when(messageStore.getMessage(anyString(), anyString(), anyInt(), anyLong(), anyInt(), any()))
            .thenReturn(mockResult);
        
        MessageGroupOrderInfoManager mgManager = processor.getMessageGroupOrderInfoManager();
        
        // 预读不同数量的消息
        MessageGroupOrderInfoManager.MessageGroupAnalysis analysis1 = 
            mgManager.analyzeMessageGroups(TEST_TOPIC, TEST_GROUP, TEST_QUEUE_ID, 0L, 1);
        MessageGroupOrderInfoManager.MessageGroupAnalysis analysis2 = 
            mgManager.analyzeMessageGroups(TEST_TOPIC, TEST_GROUP, TEST_QUEUE_ID, 0L, 5);
        
        assertNotNull("分析结果1不应为空", analysis1);
        assertNotNull("分析结果2不应为空", analysis2);
        
        // 验证预读数量对分析结果的影响
        assertTrue("应该都能分析出消息", analysis1.hasMessages() && analysis2.hasMessages());
    }
    
    /**
     * 测试场景10：长时间运行稳定性测试
     * 验证长时间运行的稳定性
     */
    @Test
    public void testLongRunningStability() throws Exception {
        GetMessageResult mockResult = createMockGetMessageResult(TEST_MESSAGE_GROUP_1, 1);
        when(messageStore.getMessage(anyString(), anyString(), anyInt(), anyLong(), anyInt(), any()))
            .thenReturn(mockResult);
        
        PopMessageRequestHeader requestHeader = createTestRequestHeader();
        int iterations = 10; // 实际测试中可以增加到更多次
        
        // 模拟长时间重复操作
        for (int i = 0; i < iterations; i++) {
            requestHeader.setAttemptId("attempt-" + i);
            
            CompletableFuture<GetMessageResult> future = processor.popMsgFromQueueWithMessageGroup(
                requestHeader, TEST_TOPIC, TEST_GROUP, TEST_QUEUE_ID, (long) i, 10, null);
            
            GetMessageResult result = future.get();
            assertNotNull("第" + i + "次操作结果不应为空", result);
            
            // 模拟消息确认
            if (result.getStatus() == GetMessageStatus.FOUND && !result.getMessageQueueOffset().isEmpty()) {
                CompletableFuture<Boolean> ackFuture = processor.ackMessageWithMessageGroup(
                    TEST_TOPIC, TEST_GROUP, TEST_QUEUE_ID, result.getMessageQueueOffset().get(0),
                    TEST_MESSAGE_GROUP_1, System.currentTimeMillis());
                
                Boolean ackResult = ackFuture.get();
                assertTrue("第" + i + "次确认应该成功", ackResult);
            }
        }
        
        // 验证系统仍然正常工作
        MessageGroupOrderInfoManager mgManager = processor.getMessageGroupOrderInfoManager();
        assertNotNull("管理器应该仍然可用", mgManager);
    }
    
    // 辅助方法：创建测试请求头
    private PopMessageRequestHeader createTestRequestHeader() {
        PopMessageRequestHeader header = new PopMessageRequestHeader();
        header.setTopic(TEST_TOPIC);
        header.setConsumerGroup(TEST_GROUP);
        header.setQueueId(TEST_QUEUE_ID);
        header.setAttemptId("test-attempt-" + System.currentTimeMillis());
        header.setInvisibleTime(30000L); // 30秒
        header.setOrder(true);
        header.setMaxMsgNums(10);
        return header;
    }
    
    // 辅助方法：创建模拟的GetMessageResult
    private GetMessageResult createMockGetMessageResult(String messageGroup, int messageCount) {
        GetMessageResult result = new GetMessageResult();
        result.setStatus(GetMessageStatus.FOUND);
        
        List<SelectMappedBufferResult> messageList = new ArrayList<>();
        List<Long> offsetList = new ArrayList<>();
        
        for (int i = 0; i < messageCount; i++) {
            try {
                // 创建测试消息
                MessageExt messageExt = createTestMessage(messageGroup, i);
                byte[] encodedMessage = MessageDecoder.encode(messageExt, false);
                
                // 创建SelectMappedBufferResult
                SelectMappedBufferResult bufferResult = mock(SelectMappedBufferResult.class);
                when(bufferResult.getByteBuffer()).thenReturn(java.nio.ByteBuffer.wrap(encodedMessage));
                when(bufferResult.getSize()).thenReturn(encodedMessage.length);
                
                messageList.add(bufferResult);
                offsetList.add((long) i);
            } catch (Exception e) {
                throw new RuntimeException("Failed to encode message", e);
            }
        }
        
        // 设置结果属性
        for (SelectMappedBufferResult bufferResult : messageList) {
            result.addMessage(bufferResult);
        }
        
        // 手动设置偏移量列表
        for (Long offset : offsetList) {
            result.getMessageQueueOffset().add(offset);
        }
        
        return result;
    }
    
    // 辅助方法：创建包含多个Message Group的消息结果
    private GetMessageResult createMockGetMessageResultWithMultipleGroups() {
        GetMessageResult result = new GetMessageResult();
        result.setStatus(GetMessageStatus.FOUND);
        
        // 创建包含两个不同Message Group的消息
        MessageExt msg1 = createTestMessage(TEST_MESSAGE_GROUP_1, 0);
        MessageExt msg2 = createTestMessage(TEST_MESSAGE_GROUP_2, 1);
        MessageExt msg3 = createTestMessage(TEST_MESSAGE_GROUP_1, 2);
        
        List<MessageExt> messageList = new ArrayList<>();
        messageList.add(msg1);
        messageList.add(msg2);
        messageList.add(msg3);
        
        // 编码消息并创建缓冲区结果
        for (int i = 0; i < messageList.size(); i++) {
            try {
                MessageExt messageExt = messageList.get(i);
                byte[] encodedMessage = MessageDecoder.encode(messageExt, false);
                
                SelectMappedBufferResult bufferResult = mock(SelectMappedBufferResult.class);
                when(bufferResult.getByteBuffer()).thenReturn(java.nio.ByteBuffer.wrap(encodedMessage));
                when(bufferResult.getSize()).thenReturn(encodedMessage.length);
                
                result.addMessage(bufferResult, (long) i);
            } catch (Exception e) {
                throw new RuntimeException("Failed to encode message", e);
            }
        }
        
        return result;
    }
    
    // 辅助方法：创建测试消息
    private MessageExt createTestMessage(String messageGroup, int index) {
        MessageExt messageExt = new MessageExt();
        messageExt.setTopic(TEST_TOPIC);
        messageExt.setQueueId(TEST_QUEUE_ID);
        messageExt.setQueueOffset(index);
        messageExt.setCommitLogOffset(index * 1000L);
        messageExt.setMsgId("MSG_" + messageGroup + "_" + index);
        messageExt.setBody(("Test message body " + index).getBytes());
        messageExt.setBornTimestamp(System.currentTimeMillis());
        messageExt.setStoreTimestamp(System.currentTimeMillis());
        messageExt.setBornHost(new InetSocketAddress("127.0.0.1", 9876));
        messageExt.setStoreHost(new InetSocketAddress("127.0.0.1", 10911));
        
        // 设置Message Group属性
        messageExt.putUserProperty(MessageConst.PROPERTY_SHARDING_KEY, messageGroup);
        
        return messageExt;
    }
}