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

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.assertj.core.util.Lists;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class QueueLevelOrderlyConsumeManagerTest {

    private static final String TOPIC = "testTopic";
    private static final String GROUP = "testGroup";
    private static final int QUEUE_ID = 0;
    private static final String ATTEMPT_ID = "attempt_123";

    private QueueLevelOrderlyConsumeManager manager;
    private ConcurrentHashMap<String, ConcurrentHashMap<Integer, ConsumerOrderInfoManager.OrderInfo>> table;
    private ConsumerOrderInfoLockManager lockManager;
    private long popTime;

    @Before
    public void setUp() {
        table = new ConcurrentHashMap<>();
        lockManager = mock(ConsumerOrderInfoLockManager.class);
        manager = new QueueLevelOrderlyConsumeManager(table, lockManager);
        popTime = System.currentTimeMillis();
    }

    @Test
    public void testCheckBlockWithNoOrderInfo() {
        // 当没有OrderInfo时，应该不阻塞
        boolean blocked = manager.checkBlock(ATTEMPT_ID, TOPIC, GROUP, QUEUE_ID, 3000L);
        assertFalse("Should not block when no order info exists", blocked);
    }

    @Test
    public void testCheckBlockWithSameAttemptId() {
        // 先更新一些消息
        manager.update(ATTEMPT_ID, false, TOPIC, GROUP, QUEUE_ID, popTime, 3000L,
            Lists.newArrayList(100L, 101L), new StringBuilder(), null);

        // 使用相同的attemptId检查，应该不阻塞
        boolean blocked = manager.checkBlock(ATTEMPT_ID, TOPIC, GROUP, QUEUE_ID, 3000L);
        assertFalse("Should not block with same attemptId", blocked);
    }

    @Test
    public void testCheckBlockWithDifferentAttemptId() {
        // 先更新一些消息
        manager.update(ATTEMPT_ID, false, TOPIC, GROUP, QUEUE_ID, popTime, 3000L,
            Lists.newArrayList(100L, 101L), new StringBuilder(), null);

        // 使用不同的attemptId检查，应该阻塞（因为消息还在不可见期内）
        boolean blocked = manager.checkBlock("different_attempt", TOPIC, GROUP, QUEUE_ID, 3000L);
        assertTrue("Should block with different attemptId when messages are invisible", blocked);
    }

    @Test
    public void testCheckBlockAfterInvisibleTimeExpired() {
        // 设置很短的不可见时间
        long shortInvisibleTime = 100L;
        manager.update(ATTEMPT_ID, false, TOPIC, GROUP, QUEUE_ID, popTime, shortInvisibleTime,
            Lists.newArrayList(100L, 101L), new StringBuilder(), null);

        // 等待不可见时间过期
        await().atMost(Duration.ofSeconds(1))
            .until(() -> !manager.checkBlock("different_attempt", TOPIC, GROUP, QUEUE_ID, shortInvisibleTime));
    }

    @Test
    public void testCommitAndNextWithNoOrderInfo() {
        // 当没有OrderInfo时，应该返回下一个偏移量
        long result = manager.commitAndNext(TOPIC, GROUP, QUEUE_ID, 100L, popTime);
        assertEquals("Should return next offset when no order info", 101L, result);
    }

    @Test
    public void testCommitAndNextWithWrongPopTime() {
        // 先更新一些消息
        manager.update(ATTEMPT_ID, false, TOPIC, GROUP, QUEUE_ID, popTime, 3000L,
            Lists.newArrayList(100L, 101L), new StringBuilder(), null);

        // 使用错误的popTime提交
        long result = manager.commitAndNext(TOPIC, GROUP, QUEUE_ID, 100L, popTime - 1000L);
        assertEquals("Should return -2 for wrong popTime", -2L, result);
    }

    @Test
    public void testCommitAndNextWithInvalidOffset() {
        // 先更新一些消息
        manager.update(ATTEMPT_ID, false, TOPIC, GROUP, QUEUE_ID, popTime, 3000L,
            Lists.newArrayList(100L, 101L), new StringBuilder(), null);

        // 尝试提交不存在的偏移量
        long result = manager.commitAndNext(TOPIC, GROUP, QUEUE_ID, 999L, popTime);
        assertEquals("Should return -1 for invalid offset", -1L, result);
    }

    @Test
    public void testCommitAndNextSequential() {
        // 先更新一些连续的消息
        manager.update(ATTEMPT_ID, false, TOPIC, GROUP, QUEUE_ID, popTime, 3000L,
            Lists.newArrayList(100L, 101L, 102L), new StringBuilder(), null);

        // 按顺序提交消息
        long result1 = manager.commitAndNext(TOPIC, GROUP, QUEUE_ID, 100L, popTime);
        assertEquals("Should return offset of first unacked message", 101L, result1);

        long result2 = manager.commitAndNext(TOPIC, GROUP, QUEUE_ID, 101L, popTime);
        assertEquals("Should return offset of first unacked message", 102L, result2);

        long result3 = manager.commitAndNext(TOPIC, GROUP, QUEUE_ID, 102L, popTime);
        assertEquals("Should return next offset after all committed", 103L, result3);
    }

    @Test
    public void testCommitAndNextOutOfOrder() {
        // 先更新一些消息
        manager.update(ATTEMPT_ID, false, TOPIC, GROUP, QUEUE_ID, popTime, 3000L,
            Lists.newArrayList(100L, 101L, 102L), new StringBuilder(), null);

        // 乱序提交消息（先提交中间的）
        long result1 = manager.commitAndNext(TOPIC, GROUP, QUEUE_ID, 101L, popTime);
        assertEquals("Should still return first unacked offset", 100L, result1);

        // 提交第一个消息
        long result2 = manager.commitAndNext(TOPIC, GROUP, QUEUE_ID, 100L, popTime);
        assertEquals("Should return next unacked offset", 102L, result2);

        // 提交最后一个消息
        long result3 = manager.commitAndNext(TOPIC, GROUP, QUEUE_ID, 102L, popTime);
        assertEquals("Should return next offset after all committed", 103L, result3);
    }

    @Test
    public void testUpdateNextVisibleTimeWithNoOrderInfo() {
        // 当没有OrderInfo时，更新下次可见时间应该记录警告但不抛异常
        manager.updateNextVisibleTime(TOPIC, GROUP, QUEUE_ID, 100L, popTime,
            System.currentTimeMillis() + 5000L);
        // 没有抛异常就说明处理正确
    }

    @Test
    public void testUpdateNextVisibleTimeWithWrongPopTime() {
        // 先更新一些消息
        manager.update(ATTEMPT_ID, false, TOPIC, GROUP, QUEUE_ID, popTime, 3000L,
            Lists.newArrayList(100L, 101L), new StringBuilder(), null);

        // 使用错误的popTime更新可见时间（应该记录警告但不抛异常）
        manager.updateNextVisibleTime(TOPIC, GROUP, QUEUE_ID, 100L, popTime - 1000L,
            System.currentTimeMillis() + 5000L);
        // 没有抛异常就说明处理正确
    }

    @Test
    public void testUpdateNextVisibleTimeSuccess() {
        // 先更新一些消息
        manager.update(ATTEMPT_ID, false, TOPIC, GROUP, QUEUE_ID, popTime, 3000L,
            Lists.newArrayList(100L, 101L), new StringBuilder(), null);

        long newVisibleTime = System.currentTimeMillis() + 10000L;
        manager.updateNextVisibleTime(TOPIC, GROUP, QUEUE_ID, 100L, popTime, newVisibleTime);

        // 验证更新后，该消息仍然会阻塞
        boolean blocked = manager.checkBlock("different_attempt", TOPIC, GROUP, QUEUE_ID, 3000L);
        assertTrue("Should still block after updating visible time", blocked);

        // 验证锁管理器被调用
        verify(lockManager, times(2)).updateLockFreeTimestamp(anyString(), anyString(), anyInt(),
            (ConsumerOrderInfoManager.OrderInfo) ArgumentMatchers.any());
    }

    @Test
    public void testClearBlock() {
        // 先更新一些消息
        manager.update(ATTEMPT_ID, false, TOPIC, GROUP, QUEUE_ID, popTime, 3000L,
            Lists.newArrayList(100L, 101L), new StringBuilder(), null);

        // 验证当前会阻塞
        boolean blockedBefore = manager.checkBlock("different_attempt", TOPIC, GROUP, QUEUE_ID, 3000L);
        assertTrue("Should block before clear", blockedBefore);

        // 清除阻塞
        manager.clearBlock(TOPIC, GROUP, QUEUE_ID);

        // 验证清除后不再阻塞
        boolean blockedAfter = manager.checkBlock("different_attempt", TOPIC, GROUP, QUEUE_ID, 3000L);
        assertFalse("Should not block after clear", blockedAfter);

        // 验证锁管理器的clearLock方法被调用
        verify(lockManager).clearLock(TOPIC, GROUP, QUEUE_ID);
    }

    @Test
    public void testClearBlockWithNoOrderInfo() {
        // 即使没有OrderInfo，清除操作也应该正常完成
        manager.clearBlock(TOPIC, GROUP, QUEUE_ID);

        // 验证锁管理器的clearLock方法被调用
        verify(lockManager).clearLock(TOPIC, GROUP, QUEUE_ID);
    }

    @Test
    public void testStartAndShutdown() {
        // 测试启动
        manager.start();
        verify(lockManager).start();

        // 测试关闭
        manager.shutdown();
        verify(lockManager).shutdown();
    }

    @Test
    public void testGetControllerType() {
        assertEquals("QUEUE_LEVEL", manager.getControllerType());
    }

    @Test
    public void testUpdateWithMergeConsumedCount() {
        // 第一次更新
        StringBuilder orderInfoBuilder1 = new StringBuilder();
        manager.update(ATTEMPT_ID, false, TOPIC, GROUP, QUEUE_ID, popTime, 3000L,
            Lists.newArrayList(100L, 101L), orderInfoBuilder1, null);

        // 第二次更新相同的消息（模拟重新消费）
        StringBuilder orderInfoBuilder2 = new StringBuilder();
        manager.update("different_attempt", false, TOPIC, GROUP, QUEUE_ID, popTime, 3000L,
            Lists.newArrayList(100L, 101L), orderInfoBuilder2, null);

        // 验证消费次数信息被正确构建
        String orderInfo = orderInfoBuilder2.toString();
        assertTrue("Order info should contain consumption count information",
            orderInfo.length() > 0);
    }

    @Test
    public void testLockManagerIntegration() {
        // 测试update操作会调用锁管理器
        manager.update(ATTEMPT_ID, false, TOPIC, GROUP, QUEUE_ID, popTime, 3000L,
            Lists.newArrayList(100L, 101L), new StringBuilder(), null);

        verify(lockManager).updateLockFreeTimestamp(anyString(), anyString(), anyInt(),
            (ConsumerOrderInfoManager.OrderInfo) ArgumentMatchers.any());
    }

    @Test
    public void testMultipleQueuesIndependence() {
        int queueId1 = 0;
        int queueId2 = 1;

        // 在两个不同的队列中更新消息
        manager.update(ATTEMPT_ID, false, TOPIC, GROUP, queueId1, popTime, 3000L,
            Lists.newArrayList(100L, 101L), new StringBuilder(), null);
        manager.update(ATTEMPT_ID, false, TOPIC, GROUP, queueId2, popTime, 3000L,
            Lists.newArrayList(200L, 201L), new StringBuilder(), null);

        // 验证两个队列的操作互不影响
        boolean blocked1 = manager.checkBlock("different_attempt", TOPIC, GROUP, queueId1, 3000L);
        boolean blocked2 = manager.checkBlock("different_attempt", TOPIC, GROUP, queueId2, 3000L);

        assertTrue("Queue 1 should be blocked", blocked1);
        assertTrue("Queue 2 should be blocked", blocked2);

        // 提交队列1的消息，不应该影响队列2
        long result1 = manager.commitAndNext(TOPIC, GROUP, queueId1, 100L, popTime);
        assertEquals("Should return next offset for queue 1", 101L, result1);

        // 队列2仍然应该被阻塞
        boolean stillBlocked2 = manager.checkBlock("different_attempt", TOPIC, GROUP, queueId2, 3000L);
        assertTrue("Queue 2 should still be blocked", stillBlocked2);

        // 清除队列1的阻塞，不应该影响队列2
        manager.clearBlock(TOPIC, GROUP, queueId1);
        boolean clearedBlocked1 = manager.checkBlock("different_attempt", TOPIC, GROUP, queueId1, 3000L);
        boolean stillBlocked2After = manager.checkBlock("different_attempt", TOPIC, GROUP, queueId2, 3000L);

        assertFalse("Queue 1 should not be blocked after clear", clearedBlocked1);
        assertTrue("Queue 2 should still be blocked after queue 1 cleared", stillBlocked2After);
    }

    @Test
    public void testMultipleTopicGroupCombinations() {
        String topic1 = "topic1";
        String topic2 = "topic2";
        String group1 = "group1";
        String group2 = "group2";

        // 在不同的topic-group组合中更新消息
        manager.update(ATTEMPT_ID, false, topic1, group1, QUEUE_ID, popTime, 3000L,
            Lists.newArrayList(100L), new StringBuilder(), null);
        manager.update(ATTEMPT_ID, false, topic1, group2, QUEUE_ID, popTime, 3000L,
            Lists.newArrayList(200L), new StringBuilder(), null);
        manager.update(ATTEMPT_ID, false, topic2, group1, QUEUE_ID, popTime, 3000L,
            Lists.newArrayList(300L), new StringBuilder(), null);

        // 验证不同组合的独立性
        assertTrue("topic1-group1 should be blocked",
            manager.checkBlock("different", topic1, group1, QUEUE_ID, 3000L));
        assertTrue("topic1-group2 should be blocked",
            manager.checkBlock("different", topic1, group2, QUEUE_ID, 3000L));
        assertTrue("topic2-group1 should be blocked",
            manager.checkBlock("different", topic2, group1, QUEUE_ID, 3000L));

        // 提交一个组合的消息，不应该影响其他组合
        assertEquals(101L, manager.commitAndNext(topic1, group1, QUEUE_ID, 100L, popTime));

        assertTrue("topic1-group2 should still be blocked",
            manager.checkBlock("different", topic1, group2, QUEUE_ID, 3000L));
        assertTrue("topic2-group1 should still be blocked",
            manager.checkBlock("different", topic2, group1, QUEUE_ID, 3000L));
    }

    @Test
    public void testEmptyOffsetList() {
        // 测试空偏移量列表的情况
        manager.update(ATTEMPT_ID, false, TOPIC, GROUP, QUEUE_ID, popTime, 3000L,
            Lists.newArrayList(), new StringBuilder(), null);

        // 空列表应该不阻塞
        boolean blocked = manager.checkBlock("different_attempt", TOPIC, GROUP, QUEUE_ID, 3000L);
        assertFalse("Empty offset list should not block", blocked);
    }

    @Test
    public void testSingleMessageCommitFlow() {
        // 测试单个消息的完整提交流程
        manager.update(ATTEMPT_ID, false, TOPIC, GROUP, QUEUE_ID, popTime, 3000L,
            Lists.newArrayList(100L), new StringBuilder(), null);

        // 初始状态应该阻塞
        assertTrue("Should block initially",
            manager.checkBlock("different_attempt", TOPIC, GROUP, QUEUE_ID, 3000L));

        // 提交消息
        long nextOffset = manager.commitAndNext(TOPIC, GROUP, QUEUE_ID, 100L, popTime);
        assertEquals("Should return next offset", 101L, nextOffset);

        // 提交后应该不再阻塞
        assertFalse("Should not block after commit",
            manager.checkBlock("different_attempt", TOPIC, GROUP, QUEUE_ID, 3000L));
    }

    @Test
    public void testPartialCommitWithVisibleTimeUpdate() {
        // 测试部分提交结合可见时间更新的复杂场景
        manager.update(ATTEMPT_ID, false, TOPIC, GROUP, QUEUE_ID, popTime, 3000L,
            Lists.newArrayList(100L, 101L, 102L), new StringBuilder(), null);

        // 提交第一个和第三个消息
        assertEquals(101L, manager.commitAndNext(TOPIC, GROUP, QUEUE_ID, 100L, popTime));
        assertEquals(101L, manager.commitAndNext(TOPIC, GROUP, QUEUE_ID, 102L, popTime));

        // 更新第二个消息的可见时间为很远的未来
        long farFutureTime = System.currentTimeMillis() + 60000L; // 1分钟后
        manager.updateNextVisibleTime(TOPIC, GROUP, QUEUE_ID, 101L, popTime, farFutureTime);

        // 应该仍然阻塞，因为第二个消息还未提交且不可见时间未到
        assertTrue("Should still block due to uncommitted message with future visible time",
            manager.checkBlock("different_attempt", TOPIC, GROUP, QUEUE_ID, 3000L));

        // 提交第二个消息
        assertEquals(103L, manager.commitAndNext(TOPIC, GROUP, QUEUE_ID, 101L, popTime));

        // 现在应该不再阻塞
        assertFalse("Should not block after all messages committed",
            manager.checkBlock("different_attempt", TOPIC, GROUP, QUEUE_ID, 3000L));
    }

    @Test
    public void testRetryMessageHandling() {
        // 测试重试消息的处理
        StringBuilder orderInfoBuilder = new StringBuilder();
        manager.update(ATTEMPT_ID, true, TOPIC, GROUP, QUEUE_ID, popTime, 3000L,
            Lists.newArrayList(100L, 101L), orderInfoBuilder, null);

        // 重试消息也应该正常处理
        assertTrue("Retry messages should also block when necessary",
            manager.checkBlock("different_attempt", TOPIC, GROUP, QUEUE_ID, 3000L));

        // 提交重试消息
        assertEquals(101L, manager.commitAndNext(TOPIC, GROUP, QUEUE_ID, 100L, popTime));
    }

    @Test
    public void testConcurrentUpdateScenario() {
        // 模拟并发更新场景
        String attemptId1 = "attempt_1";
        String attemptId2 = "attempt_2";

        // 第一次更新
        manager.update(attemptId1, false, TOPIC, GROUP, QUEUE_ID, popTime, 3000L,
            Lists.newArrayList(100L, 101L), new StringBuilder(), null);

        // 第二次更新相同的队列（模拟并发场景）
        StringBuilder orderInfoBuilder = new StringBuilder();
        manager.update(attemptId2, false, TOPIC, GROUP, QUEUE_ID, popTime, 3000L,
            Lists.newArrayList(100L, 101L, 102L), orderInfoBuilder, null);

        // 新的attemptId应该不阻塞自己
        assertFalse("New attempt should not block itself",
            manager.checkBlock(attemptId2, TOPIC, GROUP, QUEUE_ID, 3000L));

        // 但会阻塞其他attemptId
        assertTrue("Should block different attempt",
            manager.checkBlock("other_attempt", TOPIC, GROUP, QUEUE_ID, 3000L));

        // 验证消费次数信息被正确记录
        String orderInfo = orderInfoBuilder.toString();
        assertTrue("Should contain consumption count info", orderInfo.length() > 0);
    }

    @Test
    public void testLockManagerInteractionOnAllOperations() {
        // 验证所有主要操作都正确与锁管理器交互

        // update操作
        manager.update(ATTEMPT_ID, false, TOPIC, GROUP, QUEUE_ID, popTime, 3000L,
            Lists.newArrayList(100L), new StringBuilder(), null);
        verify(lockManager).updateLockFreeTimestamp(anyString(), anyString(), anyInt(),
            (ConsumerOrderInfoManager.OrderInfo) ArgumentMatchers.any());

        // commitAndNext操作（会触发updateLockFreeTimestamp）
        manager.commitAndNext(TOPIC, GROUP, QUEUE_ID, 100L, popTime);
        verify(lockManager, org.mockito.Mockito.times(2))
            .updateLockFreeTimestamp(anyString(), anyString(), anyInt(),
                (ConsumerOrderInfoManager.OrderInfo) ArgumentMatchers.any());

        // updateNextVisibleTime操作
        manager.updateNextVisibleTime(TOPIC, GROUP, QUEUE_ID, 100L, popTime,
            System.currentTimeMillis() + 5000L);
        verify(lockManager, org.mockito.Mockito.times(3))
            .updateLockFreeTimestamp(anyString(), anyString(), anyInt(),
                (ConsumerOrderInfoManager.OrderInfo) ArgumentMatchers.any());

        // clearBlock操作
        manager.clearBlock(TOPIC, GROUP, QUEUE_ID);
        verify(lockManager).clearLock(TOPIC, GROUP, QUEUE_ID);
    }
}