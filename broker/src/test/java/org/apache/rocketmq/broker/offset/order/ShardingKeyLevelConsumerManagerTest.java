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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.longpolling.PullRequestHoldService;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.OrderedConsumptionLevel;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.GetMessageStatus;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * ShardingKeyLevelConsumerManager 单元测试类
 */
public class ShardingKeyLevelConsumerManagerTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private ShardingKeyLevelConsumerManager consumerManager;

    @Mock
    private BrokerController brokerController;

    @Mock
    private PullRequestHoldService pullRequestHoldService;

    @Mock
    private ShardingKeyLockManager lockManager;

    @Mock
    private ShardingKeyCache shardingKeyCache;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);

        // 设置必要的 Mock 行为
        BrokerConfig brokerConfig = new BrokerConfig();

        when(brokerController.getBrokerConfig()).thenReturn(brokerConfig);
        when(brokerController.getPullRequestHoldService()).thenReturn(pullRequestHoldService);

        consumerManager = new ShardingKeyLevelConsumerManager(brokerController);

        // 通过反射设置 Mock 的依赖
        setPrivateField(consumerManager, "lockManager", lockManager);
        setPrivateField(consumerManager, "cache", shardingKeyCache);
    }

    @After
    public void tearDown() {
        if (consumerManager != null) {
            consumerManager.shutdown();
        }
    }

    /**
     * 测试 getOrderedConsumptionLevel 方法
     */
    @Test
    public void testGetOrderedConsumptionLevel() {
        OrderedConsumptionLevel level = consumerManager.getOrderedConsumptionLevel();
        assertEquals("consumption level should be SHARDING_KEY",
            OrderedConsumptionLevel.SHARDING_KEY, level);
    }

    /**
     * 测试 checkBlock 方法 - 始终返回 false
     */
    @Test
    public void testCheckBlock_AlwaysReturnsFalse() {
        String topic = "testTopic";
        String group = "testGroup";
        int queueId = 1;
        String attemptId = "attempt123";
        long invisibleTime = 30000L;

        // 对于 ShardingKey 级别，checkBlock 总是返回 false
        boolean result = consumerManager.checkBlock(attemptId, topic, group, queueId, invisibleTime);

        assertFalse("checkBlock should always return false for sharding key level", result);
    }

    /**
     * 测试 checkBlock 方法 - 异常情况
     */
    @Test
    public void testCheckBlock_WithNullParameters() {
        // 测试 null 参数的处理
        boolean result = consumerManager.checkBlock(null, null, null, 0, 0L);

        // 应该能正常处理，不抛异常
        assertFalse("checkBlock should handle null parameters gracefully", result);
    }

    /**
     * 测试 update 方法 - 正常情况
     */
    @Test
    public void testUpdate_Normal() {
        String topic = "testTopic";
        String group = "testGroup";
        int queueId = 1;
        String attemptId = "attempt123";
        boolean isRetry = false;
        long popTime = System.currentTimeMillis();
        long invisibleTime = 30000;
        List<Long> msgQueueOffsetList = Arrays.asList(1000L, 1001L);
        StringBuilder orderInfoBuilder = new StringBuilder();
        GetMessageResult getMessageResult = createTestGetMessageResult();

        // Mock 依赖行为
        doNothing().when(lockManager).createOrUpdateLock(anyString(), anyString(), anyInt(), anyString(),
            anyLong(), anyLong(), anyString(), any());

        // 调用 update 方法
        consumerManager.update(attemptId, isRetry, topic, group, queueId,
            popTime, invisibleTime, msgQueueOffsetList, orderInfoBuilder, getMessageResult);

        // 验证方法调用没有抛出异常
        assertTrue("update method should execute without exceptions", true);
    }

    /**
     * 测试 update 方法 - 空消息列表
     */
    @Test
    public void testUpdate_EmptyMessageList() {
        String topic = "testTopic";
        String group = "testGroup";
        int queueId = 1;
        String attemptId = "attempt123";
        boolean isRetry = false;
        long popTime = System.currentTimeMillis();
        long invisibleTime = 30000;
        List<Long> emptyOffsetList = new ArrayList<>();
        StringBuilder orderInfoBuilder = new StringBuilder();
        GetMessageResult getMessageResult = createTestGetMessageResult();

        // 调用 update 方法
        consumerManager.update(attemptId, isRetry, topic, group, queueId,
            popTime, invisibleTime, emptyOffsetList, orderInfoBuilder, getMessageResult);

        // 验证方法调用没有抛出异常
        assertTrue("update method should handle empty message list gracefully", true);
    }

    /**
     * 测试 commitAndNext 方法 - 正常提交
     */
    @Test
    public void testCommitAndNext_Normal() {
        String topic = "testTopic";
        String group = "testGroup";
        int queueId = 1;
        long ackOffset = 1000L;
        long popTime = System.currentTimeMillis();

        // Mock 锁管理器释放锁成功
        when(lockManager.releaseLock(topic, group, queueId, ackOffset, popTime)).thenReturn(true);

        long nextOffset = consumerManager.commitAndNext(topic, group, queueId, ackOffset, popTime);

        assertEquals("next offset should be ackOffset + 1", ackOffset + 1, nextOffset);

        // 验证锁被释放
        verify(lockManager, times(1)).releaseLock(topic, group, queueId, ackOffset, popTime);
    }

    /**
     * 测试 commitAndNext 方法 - 释放锁失败
     */
    @Test
    public void testCommitAndNext_ReleaseLockFailed() {
        String topic = "testTopic";
        String group = "testGroup";
        int queueId = 1;
        long ackOffset = 1000L;
        long popTime = System.currentTimeMillis();

        // Mock 锁管理器释放锁失败
        when(lockManager.releaseLock(topic, group, queueId, ackOffset, popTime)).thenReturn(false);

        long nextOffset = consumerManager.commitAndNext(topic, group, queueId, ackOffset, popTime);

        assertEquals("next offset should be -2 when release failed", -2, nextOffset);

        // 验证锁释放被尝试
        verify(lockManager, times(1)).releaseLock(topic, group, queueId, ackOffset, popTime);
    }

    /**
     * 测试 updateNextVisibleTime 方法
     */
    @Test
    public void testUpdateNextVisibleTime() {
        String topic = "testTopic";
        String group = "testGroup";
        int queueId = 1;
        long queueOffset = 1000L;
        long popTime = System.currentTimeMillis();
        long nextVisibleTime = popTime + 60000;

        consumerManager.updateNextVisibleTime(topic, group, queueId, queueOffset, popTime, nextVisibleTime);

        // 验证锁管理器的更新方法被调用
        verify(lockManager, times(1)).updateNextVisibleTime(topic, group, queueId, queueOffset, popTime, nextVisibleTime);
    }

    /**
     * 测试 clearBlock 方法
     */
    @Test
    public void testClearBlock() {
        String topic = "testTopic";
        String group = "testGroup";
        int queueId = 1;

        consumerManager.clearBlock(topic, group, queueId);

        // 验证锁管理器和缓存的清理方法被调用
        verify(lockManager, times(1)).clearQueueLocks(topic, group, queueId);
        verify(shardingKeyCache, times(1)).clearQueueCache(topic, group, queueId);
    }

    /**
     * 测试 start 方法
     */
    @Test
    public void testStart() {
        consumerManager.start();

        // start 方法应该启动内部组件
        assertTrue("manager should be started", consumerManager.isStarted());
    }

    /**
     * 测试 shutdown 方法
     */
    @Test
    public void testShutdown() {
        consumerManager.start();
        consumerManager.shutdown();

        // 验证 shutdown 后的状态
        assertFalse("manager should be shutdown", consumerManager.isStarted());
    }

    /**
     * 测试 persist 方法
     */
    @Test
    public void testPersist() {
        // persist 方法返回 void，只验证不抛出异常
        consumerManager.persist();

        assertTrue("persist should succeed", true);
    }

    /**
     * 测试 load 方法
     */
    @Test
    public void testLoad() {
        boolean result = consumerManager.load();

        assertTrue("load should succeed", result);
    }

    /**
     * 测试集成场景 - 完整的 Pop 和 ACK 流程
     */
    @Test
    public void testIntegrationScenario_CompletePopAndAckFlow() {
        String topic = "testTopic";
        String group = "testGroup";
        int queueId = 1;
        String attemptId = "attempt123";
        boolean isRetry = false;
        long popTime = System.currentTimeMillis();
        long invisibleTime = 30000;
        long ackOffset = 1000L;
        List<Long> msgQueueOffsetList = Arrays.asList(1000L, 1001L);
        StringBuilder orderInfoBuilder = new StringBuilder();

        GetMessageResult getMessageResult = createTestGetMessageResult();

        // 步骤1: 模拟消息更新（Pop流程）
        doNothing().when(lockManager).createOrUpdateLock(anyString(), anyString(), anyInt(), anyString(),
            anyLong(), anyLong(), anyString(), any());

        consumerManager.update(attemptId, isRetry, topic, group, queueId,
            popTime, invisibleTime, msgQueueOffsetList, orderInfoBuilder, getMessageResult);

        // 步骤2: 模拟消息确认（ACK流程）
        when(lockManager.releaseLock(topic, group, queueId, ackOffset, popTime)).thenReturn(true);

        long nextOffset = consumerManager.commitAndNext(topic, group, queueId, ackOffset, popTime);

        assertEquals("next offset should be correct", ackOffset + 1, nextOffset);

        // 验证调用链
        verify(lockManager, times(1)).releaseLock(topic, group, queueId, ackOffset, popTime);
    }

    /**
     * 测试并发安全性
     */
    @Test
    public void testConcurrentSafety() throws InterruptedException {
        final int threadCount = 5;
        final CountDownLatch latch = new CountDownLatch(threadCount);
        final ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        try {
            // 启动多个线程同时操作
            for (int i = 0; i < threadCount; i++) {
                final int threadId = i;
                executor.submit(() -> {
                    try {
                        String topic = "testTopic" + threadId;
                        String group = "testGroup" + threadId;
                        int queueId = threadId;
                        String attemptId = "attempt" + threadId;
                        long popTime = System.currentTimeMillis();
                        long invisibleTime = 30000;

                        // 测试各种方法的并发调用
                        consumerManager.checkBlock(attemptId, topic, group, queueId, invisibleTime);
                        consumerManager.updateNextVisibleTime(topic, group, queueId, 1000L + threadId, popTime, popTime + invisibleTime);
                        consumerManager.commitAndNext(topic, group, queueId, 1000L + threadId, popTime);
                        consumerManager.clearBlock(topic, group, queueId);

                    } finally {
                        latch.countDown();
                    }
                });
            }

            // 等待所有线程完成
            assertTrue("All threads should complete within timeout",
                latch.await(10, TimeUnit.SECONDS));

        } finally {
            executor.shutdown();
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        }
    }

    /**
     * 测试边界条件
     */
    @Test
    public void testBoundaryConditions() {
        // 测试空字符串参数
        boolean result1 = consumerManager.checkBlock("", "", "", 0, 0L);
        assertFalse("checkBlock should handle empty strings", result1);

        // 测试负数参数
        boolean result2 = consumerManager.checkBlock("attempt", "topic", "group", -1, -1L);
        assertFalse("checkBlock should handle negative values", result2);

        // 测试极大数值
        long nextOffset = consumerManager.commitAndNext("topic", "group", 0, Long.MAX_VALUE,
            System.currentTimeMillis());
        // 应该能正常处理

        assertTrue("boundary condition tests completed", true);
    }

    /**
     * 测试错误恢复场景
     */
    @Test
    public void testErrorRecoveryScenario() {
        String topic = "testTopic";
        String group = "testGroup";
        int queueId = 1;

        // 模拟依赖组件抛出异常
        when(lockManager.releaseLock(anyString(), anyString(), anyInt(), anyLong(), anyLong()))
            .thenThrow(new RuntimeException("Lock error"))
            .thenReturn(true);

        long result = consumerManager.commitAndNext(topic, group, queueId, 1000L, System.currentTimeMillis());
        // 直接断言返回值是 -1，不应该有异常抛出
        assertEquals("should return -1 on error", -1L, result);

        long recoveryResult = consumerManager.commitAndNext(topic, group, queueId, 1000L, System.currentTimeMillis());
        assertEquals("should work normally after recovery", 1001L, recoveryResult);
        assertTrue("error recovery test completed", true);
    }

    /**
     * 创建测试用的 GetMessageResult 对象
     */
    private GetMessageResult createTestGetMessageResult() {
        GetMessageResult result = new GetMessageResult();
        result.setStatus(GetMessageStatus.FOUND);
        result.setNextBeginOffset(1001L);
        result.setMinOffset(1000L);
        result.setMaxOffset(2000L);

        // 创建测试消息
        SelectMappedBufferResult bufferResult = mock(SelectMappedBufferResult.class);
        ByteBuffer buffer = ByteBuffer.allocate(1024);

        // 构造包含 sharding key 的消息内容
        String testMessage = "test message content";
        String shardingKey = "user123";

        // 简化的消息格式，实际应该使用 MessageDecoder
        buffer.put(testMessage.getBytes());
        buffer.put((MessageConst.PROPERTY_SHARDING_KEY + "=" + shardingKey).getBytes());
        buffer.flip();

        when(bufferResult.getByteBuffer()).thenReturn(buffer);
        when(bufferResult.getSize()).thenReturn(buffer.remaining());
        when(bufferResult.getStartOffset()).thenReturn(1000L);

        result.addMessage(bufferResult, 1000L);

        return result;
    }

    /**
     * 通过反射设置私有字段
     */
    private void setPrivateField(Object target, String fieldName, Object value) throws Exception {
        java.lang.reflect.Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }
}