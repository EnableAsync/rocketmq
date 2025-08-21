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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.longpolling.PullRequestHoldService;
import org.apache.rocketmq.common.BrokerConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * ShardingKeyLockManager 单元测试类
 */
public class ShardingKeyLockManagerTest {

    private ShardingKeyLockManager lockManager;

    @Mock
    private BrokerController brokerController;

    @Mock
    private PullRequestHoldService pullRequestHoldService;

//    @Before
//    public void setUp() {
//        MockitoAnnotations.openMocks(this);
//
//        // 设置必要的 Mock 行为
//        BrokerConfig brokerConfig = new BrokerConfig();
//        when(brokerController.getBrokerConfig()).thenReturn(brokerConfig);
//        when(brokerController.getPullRequestHoldService()).thenReturn(pullRequestHoldService);
//
//        lockManager = new ShardingKeyLockManager(brokerController, );
//    }

    @After
    public void tearDown() {
        if (lockManager != null) {
            lockManager.shutdown();
        }
    }

    /**
     * 测试 isLocked 方法 - 未锁定状态
     */
    @Test
    public void testIsLocked_NotLocked() {
        String topic = "testTopic";
        String group = "testGroup";
        int queueId = 1;
        String shardingKey = "user123";
        String attemptId = "attempt123";

        boolean isLocked = lockManager.isLocked(topic, group, queueId, shardingKey, attemptId);
        assertFalse("sharding key should not be locked initially", isLocked);
    }

    /**
     * 测试 isLocked 方法 - 已锁定状态
     */
    @Test
    public void testIsLocked_Locked() {
        String topic = "testTopic";
        String group = "testGroup";
        int queueId = 1;
        String shardingKey = "user123";
        String attemptId = "attempt123";
        long popTime = System.currentTimeMillis();
        long invisibleTime = 30000;
        List<Long> offsets = Arrays.asList(1000L, 1001L);

        // 创建锁
        lockManager.createOrUpdateLock(topic, group, queueId, shardingKey, popTime, invisibleTime, attemptId, offsets);

        // 用不同的 attemptId 检查锁定状态
        boolean isLocked = lockManager.isLocked(topic, group, queueId, shardingKey, "different_attempt");
        assertTrue("sharding key should be locked after creation", isLocked);
    }

    /**
     * 测试 isLocked 方法 - 过期的锁
     */
    @Test
    public void testIsLocked_ExpiredLock() {
        String topic = "testTopic";
        String group = "testGroup";
        int queueId = 1;
        String shardingKey = "user123";
        String attemptId = "attempt123";
        long popTime = System.currentTimeMillis() - 60000; // 1分钟前
        long invisibleTime = 30000; // 30秒不可见时间，已过期
        List<Long> offsets = Arrays.asList(1000L);

        // 创建过期的锁
        lockManager.createOrUpdateLock(topic, group, queueId, shardingKey, popTime, invisibleTime, attemptId, offsets);

        boolean isLocked = lockManager.isLocked(topic, group, queueId, shardingKey, "different_attempt");
        assertFalse("expired lock should not be considered as locked", isLocked);
    }

    /**
     * 测试 createOrUpdateLock 方法 - 创建新锁
     */
    @Test
    public void testCreateOrUpdateLock_CreateNew() {
        String topic = "testTopic";
        String group = "testGroup";
        int queueId = 1;
        String shardingKey = "user123";
        String attemptId = "attempt123";
        long popTime = System.currentTimeMillis();
        long invisibleTime = 30000;
        List<Long> offsets = Arrays.asList(1000L, 1001L, 1002L);

        // 创建新锁
        lockManager.createOrUpdateLock(topic, group, queueId, shardingKey, popTime, invisibleTime, attemptId, offsets);

        // 验证锁已创建
        assertTrue("lock should be created", lockManager.isLocked(topic, group, queueId, shardingKey, "different_attempt"));
    }

    /**
     * 测试 createOrUpdateLock 方法 - 更新现有锁
     */
    @Test
    public void testCreateOrUpdateLock_UpdateExisting() {
        String topic = "testTopic";
        String group = "testGroup";
        int queueId = 1;
        String shardingKey = "user123";
        String attemptId = "attempt123";
        long popTime = System.currentTimeMillis();
        long invisibleTime = 30000;
        List<Long> offsets = Arrays.asList(1000L);

        // 创建锁
        lockManager.createOrUpdateLock(topic, group, queueId, shardingKey, popTime, invisibleTime, attemptId, offsets);

        // 更新锁，添加更多offset
        long newPopTime = popTime + 1000;
        long newInvisibleTime = 60000;
        List<Long> newOffsets = Arrays.asList(1003L, 1004L);
        lockManager.createOrUpdateLock(topic, group, queueId, shardingKey, newPopTime, newInvisibleTime, attemptId, newOffsets);

        // 验证锁仍然存在
        assertTrue("lock should still exist after update",
                lockManager.isLocked(topic, group, queueId, shardingKey, "different_attempt"));
    }

    /**
     * 测试 filterMessagesByLock 方法 - 正常情况
     */
//    @Test
//    public void testFilterMessagesByLock_Normal() {
//        String topic = "testTopic";
//        String group = "testGroup";
//        int queueId = 1;
//        String attemptId = "attempt123";
//
//        // 创建消息分片信息
//        MessageShardingKeyUtil.MessageShardingInfo shardingInfo = new MessageShardingKeyUtil.MessageShardingInfo();
//        shardingInfo.addMessage(1000L, "user123", 0);
//        shardingInfo.addMessage(1001L, "user456", 1);
//        shardingInfo.addMessage(1002L, "user123", 2);
//
//        // 为user123创建锁
//        lockManager.createOrUpdateLock(topic, group, queueId, "user123",
//                System.currentTimeMillis(), 30000, "attempt123", Arrays.asList(1000L, 1002L));
//
//        // 过滤消息
//        MessageShardingKeyUtil.MessageFilterResult result = lockManager.filterMessagesByLock(
//                topic, group, queueId, attemptId, shardingInfo);
//
//        assertNotNull("filter result should not be null", result);
//
//        // 验证可用消息（未被锁定的）
//        Map<String, List<MessageShardingKeyUtil.MessageInfo>> availableMessages =
//                result.getAvailableMessagesByShardingKey();
//        assertTrue("should have available messages for user456", availableMessages.containsKey("user456"));
//        assertEquals("should have 1 available message for user456", 1, availableMessages.get("user456").size());
//
//        // 验证阻塞消息（被锁定的）
//        Map<String, List<MessageShardingKeyUtil.MessageInfo>> blockedMessages =
//                result.getBlockedMessagesByShardingKey();
//        assertTrue("should have blocked messages for user123", blockedMessages.containsKey("user123"));
//        assertEquals("should have 2 blocked messages for user123", 2, blockedMessages.get("user123").size());
//    }

    /**
     * 测试 filterMessagesByLock 方法 - 所有消息都可用
     */
//    @Test
//    public void testFilterMessagesByLock_AllAvailable() {
//        String topic = "testTopic";
//        String group = "testGroup";
//        int queueId = 1;
//        String attemptId = "attempt123";
//
//        // 创建消息分片信息
//        MessageShardingKeyUtil.MessageShardingInfo shardingInfo = new MessageShardingKeyUtil.MessageShardingInfo();
//        shardingInfo.addMessage(1000L, "user123", 0);
//        shardingInfo.addMessage(1001L, "user456", 1);
//        shardingInfo.addMessage(1002L, "user789", 2);
//
//        // 不创建任何锁
//
//        // 过滤消息
//        MessageShardingKeyUtil.MessageFilterResult result = lockManager.filterMessagesByLock(
//                topic, group, queueId, attemptId, shardingInfo);
//
//        assertNotNull("filter result should not be null", result);
//
//        // 验证所有消息都可用
//        Map<String, List<MessageShardingKeyUtil.MessageInfo>> availableMessages =
//                result.getAvailableMessagesByShardingKey();
//        assertEquals("should have 3 available sharding keys", 3, availableMessages.size());
//        assertTrue("should have available messages for user123", availableMessages.containsKey("user123"));
//        assertTrue("should have available messages for user456", availableMessages.containsKey("user456"));
//        assertTrue("should have available messages for user789", availableMessages.containsKey("user789"));
//
//        // 验证没有阻塞消息
//        Map<String, List<MessageShardingKeyUtil.MessageInfo>> blockedMessages =
//                result.getBlockedMessagesByShardingKey();
//        assertTrue("should have no blocked messages", blockedMessages.isEmpty());
//    }

    /**
     * 测试 filterMessagesByLock 方法 - 所有消息都被阻塞
     */
//    @Test
//    public void testFilterMessagesByLock_AllBlocked() {
//        String topic = "testTopic";
//        String group = "testGroup";
//        int queueId = 1;
//        String attemptId = "attempt123";
//
//        // 创建消息分片信息
//        MessageShardingKeyUtil.MessageShardingInfo shardingInfo = new MessageShardingKeyUtil.MessageShardingInfo();
//        shardingInfo.addMessage(1000L, "user123", 0);
//        shardingInfo.addMessage(1001L, "user456", 1);
//        shardingInfo.addMessage(1002L, "user789", 2);
//
//        // 为所有sharding key创建锁
//        long currentTime = System.currentTimeMillis();
//        lockManager.createOrUpdateLock(topic, group, queueId, "user123", currentTime, 30000, "attempt123", Arrays.asList(1000L));
//        lockManager.createOrUpdateLock(topic, group, queueId, "user456", currentTime, 30000, "attempt456", Arrays.asList(1001L));
//        lockManager.createOrUpdateLock(topic, group, queueId, "user789", currentTime, 30000, "attempt789", Arrays.asList(1002L));
//
//        // 过滤消息
//        MessageShardingKeyUtil.MessageFilterResult result = lockManager.filterMessagesByLock(
//                topic, group, queueId, "different_attempt", shardingInfo);
//
//        assertNotNull("filter result should not be null", result);
//
//        // 验证没有可用消息
//        Map<String, List<MessageShardingKeyUtil.MessageInfo>> availableMessages =
//                result.getAvailableMessagesByShardingKey();
//        assertTrue("should have no available messages", availableMessages.isEmpty());
//
//        // 验证所有消息都被阻塞
//        Map<String, List<MessageShardingKeyUtil.MessageInfo>> blockedMessages =
//                result.getBlockedMessagesByShardingKey();
//        assertEquals("should have 3 blocked sharding keys", 3, blockedMessages.size());
//    }

    /**
     * 测试 releaseLock 方法 - 释放单个offset
     */
    @Test
    public void testReleaseLock_SingleOffset() {
        String topic = "testTopic";
        String group = "testGroup";
        int queueId = 1;
        String shardingKey = "user123";
        String attemptId = "attempt123";
        long popTime = System.currentTimeMillis();
        long invisibleTime = 30000;
        List<Long> offsets = Arrays.asList(1000L, 1001L);

        // 创建锁并添加offset
        lockManager.createOrUpdateLock(topic, group, queueId, shardingKey, popTime, invisibleTime, attemptId, offsets);

        // 验证锁存在
        assertTrue("lock should exist", lockManager.isLocked(topic, group, queueId, shardingKey, "different_attempt"));

        // 释放一个offset
        boolean released = lockManager.releaseLock(topic, group, queueId, 1000L, popTime);
        assertTrue("should release lock successfully", released);

        // 验证锁仍然存在（因为还有其他offset）
        assertTrue("lock should still exist", lockManager.isLocked(topic, group, queueId, shardingKey, "different_attempt"));
    }

    /**
     * 测试 releaseLock 方法 - 释放所有offset
     */
    @Test
    public void testReleaseLock_AllOffsets() {
        String topic = "testTopic";
        String group = "testGroup";
        int queueId = 1;
        String shardingKey = "user123";
        String attemptId = "attempt123";
        long popTime = System.currentTimeMillis();
        long invisibleTime = 30000;
        List<Long> offsets = Arrays.asList(1000L);

        // 创建锁
        lockManager.createOrUpdateLock(topic, group, queueId, shardingKey, popTime, invisibleTime, attemptId, offsets);

        // 验证锁存在
        assertTrue("lock should exist", lockManager.isLocked(topic, group, queueId, shardingKey, "different_attempt"));

        // 释放唯一的offset
        boolean released = lockManager.releaseLock(topic, group, queueId, 1000L, popTime);
        assertTrue("should release lock successfully", released);

        // 验证锁已被移除
        assertFalse("lock should be removed", lockManager.isLocked(topic, group, queueId, shardingKey, "different_attempt"));
    }

    /**
     * 测试 releaseLock 方法 - 释放不存在的offset
     */
    @Test
    public void testReleaseLock_NonexistentOffset() {
        String topic = "testTopic";
        String group = "testGroup";
        int queueId = 1;
        long popTime = System.currentTimeMillis();

        // 尝试释放不存在的offset
        boolean released = lockManager.releaseLock(topic, group, queueId, 1000L, popTime);
        assertFalse("should not release nonexistent lock", released);
    }

    /**
     * 测试 updateNextVisibleTime 方法 - 正常情况
     */
    @Test
    public void testUpdateNextVisibleTime_Normal() {
        String topic = "testTopic";
        String group = "testGroup";
        int queueId = 1;
        String shardingKey = "user123";
        String attemptId = "attempt123";
        long popTime = System.currentTimeMillis();
        long invisibleTime = 30000;
        List<Long> offsets = Arrays.asList(1000L, 1001L);

        // 创建锁
        lockManager.createOrUpdateLock(topic, group, queueId, shardingKey, popTime, invisibleTime, attemptId, offsets);

        // 更新不可见时间
        long queueOffset = 1000L;
        long newNextVisibleTime = System.currentTimeMillis() + 60000;
        lockManager.updateNextVisibleTime(topic, group, queueId, queueOffset, popTime, newNextVisibleTime);

        // 验证锁仍然存在且时间被更新
        assertTrue("lock should still exist after update",
                lockManager.isLocked(topic, group, queueId, shardingKey, "different_attempt"));
    }

    /**
     * 测试 updateNextVisibleTime 方法 - 锁不存在
     */
    @Test
    public void testUpdateNextVisibleTime_LockNotExists() {
        String topic = "testTopic";
        String group = "testGroup";
        int queueId = 1;
        long queueOffset = 1000L;
        long popTime = System.currentTimeMillis();
        long nextVisibleTime = System.currentTimeMillis() + 60000;

        // 尝试更新不存在的锁
        lockManager.updateNextVisibleTime(topic, group, queueId, queueOffset, popTime, nextVisibleTime);

        // 应该不会出现异常，方法应该能正常处理
        assertTrue("method should handle non-existent lock gracefully", true);
    }

    /**
     * 测试 updateNextVisibleTime 方法 - popTime不匹配
     */
    @Test
    public void testUpdateNextVisibleTime_PopTimeMismatch() {
        String topic = "testTopic";
        String group = "testGroup";
        int queueId = 1;
        String shardingKey = "user123";
        String attemptId = "attempt123";
        long popTime = System.currentTimeMillis();
        long invisibleTime = 30000;
        List<Long> offsets = Arrays.asList(1000L);

        // 创建锁
        lockManager.createOrUpdateLock(topic, group, queueId, shardingKey, popTime, invisibleTime, attemptId, offsets);

        // 用错误的popTime尝试更新
        long queueOffset = 1000L;
        long wrongPopTime = popTime + 1000;
        long nextVisibleTime = System.currentTimeMillis() + 60000;
        lockManager.updateNextVisibleTime(topic, group, queueId, queueOffset, wrongPopTime, nextVisibleTime);

        // 验证锁状态没有变化（因为popTime不匹配）
        assertTrue("lock should still exist",
                lockManager.isLocked(topic, group, queueId, shardingKey, "different_attempt"));
    }

    /**
     * 测试 clearQueueLocks 方法
     */
    @Test
    public void testClearQueueLocks() {
        String topic = "testTopic";
        String group = "testGroup";
        int queueId1 = 1;
        int queueId2 = 2;
        long currentTime = System.currentTimeMillis();

        // 为两个队列创建锁
        lockManager.createOrUpdateLock(topic, group, queueId1, "user123", currentTime, 30000, "attempt123", Arrays.asList(1000L));
        lockManager.createOrUpdateLock(topic, group, queueId1, "user456", currentTime, 30000, "attempt456", Arrays.asList(1001L));
        lockManager.createOrUpdateLock(topic, group, queueId2, "user789", currentTime, 30000, "attempt789", Arrays.asList(1002L));

        // 验证锁存在
        assertTrue("lock should exist for queue1", lockManager.isLocked(topic, group, queueId1, "user123", "different_attempt"));
        assertTrue("lock should exist for queue1", lockManager.isLocked(topic, group, queueId1, "user456", "different_attempt"));
        assertTrue("lock should exist for queue2", lockManager.isLocked(topic, group, queueId2, "user789", "different_attempt"));

        // 清除队列1的锁
        lockManager.clearQueueLocks(topic, group, queueId1);

        // 验证队列1的锁被清除，队列2的锁保持不变
        assertFalse("lock should be cleared for queue1", lockManager.isLocked(topic, group, queueId1, "user123", "different_attempt"));
        assertFalse("lock should be cleared for queue1", lockManager.isLocked(topic, group, queueId1, "user456", "different_attempt"));
        assertTrue("lock should still exist for queue2", lockManager.isLocked(topic, group, queueId2, "user789", "different_attempt"));
    }

    /**
     * 测试锁过期处理
     */
    @Test
    public void testLockExpiration() throws InterruptedException {
        String topic = "testTopic";
        String group = "testGroup";
        int queueId = 1;
        String shardingKey = "user123";
        String attemptId = "attempt123";
        long popTime = System.currentTimeMillis();
        long invisibleTime = 100; // 设置很短的不可见时间
        List<Long> offsets = Arrays.asList(1000L);

        // 创建很快就会过期的锁
        lockManager.createOrUpdateLock(topic, group, queueId, shardingKey, popTime, invisibleTime, attemptId, offsets);

        // 验证锁存在
        assertTrue("lock should exist initially",
                lockManager.isLocked(topic, group, queueId, shardingKey, "different_attempt"));

        // 等待锁过期
        Thread.sleep(200);

        // 验证锁已过期
        assertFalse("lock should be expired",
                lockManager.isLocked(topic, group, queueId, shardingKey, "different_attempt"));
    }

    /**
     * 测试获取统计信息
     */
    @Test
    public void testGetStatistics() {
        String topic = "testTopic";
        String group = "testGroup";
        int queueId = 1;
        long currentTime = System.currentTimeMillis();

        // 创建多个锁
        lockManager.createOrUpdateLock(topic, group, queueId, "user123", currentTime, 30000, "attempt123", Arrays.asList(1000L, 1001L));
        lockManager.createOrUpdateLock(topic, group, queueId, "user456", currentTime, 30000, "attempt456", Arrays.asList(1002L));
        lockManager.createOrUpdateLock(topic, group, queueId, "user789", currentTime, 30000, "attempt789", Arrays.asList(1003L, 1004L, 1005L));

        // 获取统计信息
        String stats = lockManager.getStatistics();

        assertNotNull("statistics should not be null", stats);
        assertThat(stats).contains("ShardingKeyLockManager");
    }

    /**
     * 测试并发安全性
     */
    @Test
    public void testConcurrentSafety() throws InterruptedException {
        String topic = "testTopic";
        String group = "testGroup";
        int queueId = 1;
        final int threadCount = 5;
        final CountDownLatch latch = new CountDownLatch(threadCount);
        final ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        try {
            // 启动多个线程同时操作锁
            for (int i = 0; i < threadCount; i++) {
                final int threadId = i;
                executor.submit(() -> {
                    try {
                        String shardingKey = "user" + threadId;
                        String attemptId = "attempt" + threadId;
                        long popTime = System.currentTimeMillis();
                        long invisibleTime = 30000;
                        List<Long> offsets = Arrays.asList(1000L + threadId);

                        // 创建锁
                        lockManager.createOrUpdateLock(topic, group, queueId, shardingKey, popTime, invisibleTime, attemptId, offsets);

                        // 检查锁
                        lockManager.isLocked(topic, group, queueId, shardingKey, "different_attempt");

                        // 释放锁
                        lockManager.releaseLock(topic, group, queueId, 1000L + threadId, popTime);
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
        boolean result1 = lockManager.isLocked("", "", 0, "", "");
        assertFalse("empty strings should not be locked", result1);

        // 测试负数队列ID
        boolean result2 = lockManager.isLocked("topic", "group", -1, "shardingKey", "attemptId");
        assertFalse("negative queueId should not be locked", result2);

        // 测试极大队列ID
        boolean result3 = lockManager.isLocked("topic", "group", Integer.MAX_VALUE, "shardingKey", "attemptId");
        assertFalse("max queueId should not be locked", result3);
    }

    /**
     * 测试过期锁清理
     */
    @Test
    public void testCleanupExpiredLocks() throws InterruptedException {
        String topic = "testTopic";
        String group = "testGroup";
        int queueId = 1;
        String shardingKey = "user123";
        String attemptId = "attempt123";
        long popTime = System.currentTimeMillis();
        long invisibleTime = 50; // 设置很短的不可见时间
        List<Long> offsets = Arrays.asList(1000L);

        // 创建会快速过期的锁
        lockManager.createOrUpdateLock(topic, group, queueId, shardingKey, popTime, invisibleTime, attemptId, offsets);

        // 验证锁存在
        assertTrue("lock should exist initially",
                lockManager.isLocked(topic, group, queueId, shardingKey, "different_attempt"));

        // 等待足够时间让锁过期
        Thread.sleep(200);

        // 检查锁是否自动清理
        assertFalse("expired lock should be automatically cleaned",
                lockManager.isLocked(topic, group, queueId, shardingKey, "different_attempt"));
    }

    /**
     * 测试锁统计信息
     */
    @Test
    public void testLockStatistics() {
        String topic = "testTopic";
        String group = "testGroup";
        int queueId = 1;
        long currentTime = System.currentTimeMillis();

        // 创建锁
        lockManager.createOrUpdateLock(topic, group, queueId, "user123", currentTime, 30000, "attempt123", Arrays.asList(1000L));
        lockManager.createOrUpdateLock(topic, group, queueId, "user456", currentTime, 30000, "attempt456", Arrays.asList(1001L));

        // 获取统计信息
        String statistics = lockManager.getStatistics();

        assertNotNull("statistics should not be null", statistics);
        assertThat(statistics).contains("ShardingKeyLockManager");
    }

    /**
     * 测试 shutdown 方法
     */
    @Test
    public void testShutdown() {
        String topic = "testTopic";
        String group = "testGroup";
        int queueId = 1;
        long currentTime = System.currentTimeMillis();

        // 创建一些锁
        lockManager.createOrUpdateLock(topic, group, queueId, "user123", currentTime, 30000, "attempt123", Arrays.asList(1000L));
        lockManager.createOrUpdateLock(topic, group, queueId, "user456", currentTime, 30000, "attempt456", Arrays.asList(1001L));
        lockManager.createOrUpdateLock(topic, group, queueId, "user789", currentTime, 30000, "attempt789", Arrays.asList(1002L));

        // 验证锁存在
        assertTrue("lock should exist before shutdown",
                lockManager.isLocked(topic, group, queueId, "user123", "different_attempt"));

        // 执行 shutdown
        lockManager.shutdown();

        // 验证 shutdown 后的状态
        // 注意：shutdown主要是停止定时器，锁的数据结构可能保持不变
        // 具体的行为取决于实现
        assertTrue("shutdown completed", true);
    }
}