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

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.Test;

/**
 * ShardingKeyLock 单元测试类
 */
public class ShardingKeyLockTest {

    /**
     * 测试默认构造函数
     */
    @Test
    public void testDefaultConstructor() {
        ShardingKeyLock lock = new ShardingKeyLock();
        
        assertNotNull("offsetSet should not be null", lock.getOffsetSet());
        assertTrue("offsetSet should be empty", lock.getOffsetSet().isEmpty());
        assertTrue("createTime should be greater than 0", lock.getCreateTime() > 0);
        assertEquals("lockFreeTimestamp should be 0", 0, lock.getLockFreeTimestamp());
        assertEquals("popTime should be 0", 0, lock.getPopTime());
        assertEquals("invisibleTime should be 0", 0, lock.getInvisibleTime());
    }

    /**
     * 测试带参数的构造函数
     */
    @Test
    public void testParameterizedConstructor() {
        long popTime = System.currentTimeMillis();
        long lockFreeTimestamp = popTime + 30000; // 30秒后释放
        String attemptId = "test-attempt-123";
        
        ShardingKeyLock lock = new ShardingKeyLock(popTime, lockFreeTimestamp, attemptId);
        
        assertNotNull("offsetSet should not be null", lock.getOffsetSet());
        assertTrue("offsetSet should be empty", lock.getOffsetSet().isEmpty());
        assertEquals("popTime should match", popTime, lock.getPopTime());
        assertEquals("lockFreeTimestamp should match", lockFreeTimestamp, lock.getLockFreeTimestamp());
        assertEquals("attemptId should match", attemptId, lock.getAttemptId());
        assertEquals("invisibleTime should be calculated correctly", 
                lockFreeTimestamp - popTime, lock.getInvisibleTime());
        assertTrue("createTime should be greater than 0", lock.getCreateTime() > 0);
    }

    /**
     * 测试 createWithInvisibleTime 静态工厂方法
     */
    @Test
    public void testCreateWithInvisibleTime() {
        long popTime = System.currentTimeMillis();
        long invisibleTime = 30000; // 30秒不可见时间
        String attemptId = "test-attempt-456";
        
        ShardingKeyLock lock = ShardingKeyLock.createWithInvisibleTime(popTime, invisibleTime, attemptId);
        
        assertNotNull("lock should not be null", lock);
        assertNotNull("offsetSet should not be null", lock.getOffsetSet());
        assertTrue("offsetSet should be empty", lock.getOffsetSet().isEmpty());
        assertEquals("popTime should match", popTime, lock.getPopTime());
        assertEquals("invisibleTime should match", invisibleTime, lock.getInvisibleTime());
        assertEquals("lockFreeTimestamp should be calculated correctly", 
                popTime + invisibleTime, lock.getLockFreeTimestamp());
        assertEquals("attemptId should match", attemptId, lock.getAttemptId());
    }

    /**
     * 测试 addOffset 方法
     */
    @Test
    public void testAddOffset() {
        ShardingKeyLock lock = new ShardingKeyLock();
        
        // 测试添加正常 offset
        lock.addOffset(100L);
        assertEquals("size should be 1", 1, lock.size());
        assertTrue("should contain offset 100", lock.containsOffset(100L));
        
        // 测试添加多个 offset
        lock.addOffset(200L);
        lock.addOffset(300L);
        assertEquals("size should be 3", 3, lock.size());
        assertTrue("should contain offset 200", lock.containsOffset(200L));
        assertTrue("should contain offset 300", lock.containsOffset(300L));
        
        // 测试添加重复 offset
        lock.addOffset(100L);
        assertEquals("size should still be 3", 3, lock.size());
        
        // 测试添加 null offset
        lock.addOffset(null);
        assertEquals("size should still be 3", 3, lock.size());
    }

    /**
     * 测试 removeOffset 方法
     */
    @Test
    public void testRemoveOffset() {
        ShardingKeyLock lock = new ShardingKeyLock();
        
        // 添加一些 offset
        lock.addOffset(100L);
        lock.addOffset(200L);
        lock.addOffset(300L);
        
        // 测试移除存在的 offset
        assertTrue("should remove offset 200", lock.removeOffset(200L));
        assertEquals("size should be 2", 2, lock.size());
        assertFalse("should not contain offset 200", lock.containsOffset(200L));
        
        // 测试移除不存在的 offset
        assertFalse("should not remove non-existent offset", lock.removeOffset(400L));
        assertEquals("size should still be 2", 2, lock.size());
        
        // 测试移除 null offset
        assertFalse("should not remove null offset", lock.removeOffset(null));
        assertEquals("size should still be 2", 2, lock.size());
        
        // 测试移除所有 offset
        assertTrue("should remove offset 100", lock.removeOffset(100L));
        assertTrue("should remove offset 300", lock.removeOffset(300L));
        assertEquals("size should be 0", 0, lock.size());
        assertTrue("should be empty", lock.isEmpty());
    }

    /**
     * 测试 containsOffset 方法
     */
    @Test
    public void testContainsOffset() {
        ShardingKeyLock lock = new ShardingKeyLock();
        
        // 测试空锁
        assertFalse("empty lock should not contain any offset", lock.containsOffset(100L));
        assertFalse("empty lock should not contain null offset", lock.containsOffset(null));
        
        // 添加 offset 后测试
        lock.addOffset(100L);
        lock.addOffset(200L);
        
        assertTrue("should contain offset 100", lock.containsOffset(100L));
        assertTrue("should contain offset 200", lock.containsOffset(200L));
        assertFalse("should not contain offset 300", lock.containsOffset(300L));
        assertFalse("should not contain null offset", lock.containsOffset(null));
    }

    /**
     * 测试 isExpired 方法
     */
    @Test
    public void testIsExpired() {
        long currentTime = System.currentTimeMillis();
        
        // 测试未过期的锁
        ShardingKeyLock lock1 = new ShardingKeyLock();
        lock1.setLockFreeTimestamp(currentTime + 10000); // 10秒后过期
        assertFalse("lock should not be expired", lock1.isExpired());
        
        // 测试已过期的锁
        ShardingKeyLock lock2 = new ShardingKeyLock();
        lock2.setLockFreeTimestamp(currentTime - 1000); // 1秒前就过期了
        assertTrue("lock should be expired", lock2.isExpired());
        
        // 测试刚好到期的锁
        ShardingKeyLock lock3 = new ShardingKeyLock();
        lock3.setLockFreeTimestamp(currentTime);
        assertTrue("lock should be expired at exact time", lock3.isExpired());
    }

    /**
     * 测试 isEmpty 和 size 方法
     */
    @Test
    public void testIsEmptyAndSize() {
        ShardingKeyLock lock = new ShardingKeyLock();
        
        // 测试空锁
        assertTrue("new lock should be empty", lock.isEmpty());
        assertEquals("new lock size should be 0", 0, lock.size());
        
        // 添加 offset
        lock.addOffset(100L);
        assertFalse("lock with offset should not be empty", lock.isEmpty());
        assertEquals("lock size should be 1", 1, lock.size());
        
        // 添加更多 offset
        lock.addOffset(200L);
        lock.addOffset(300L);
        assertFalse("lock with multiple offsets should not be empty", lock.isEmpty());
        assertEquals("lock size should be 3", 3, lock.size());
        
        // 移除所有 offset
        lock.removeOffset(100L);
        lock.removeOffset(200L);
        lock.removeOffset(300L);
        assertTrue("lock should be empty after removing all offsets", lock.isEmpty());
        assertEquals("lock size should be 0 after removing all offsets", 0, lock.size());
    }

    /**
     * 测试 updateLockFreeTimestamp 方法
     */
    @Test
    public void testUpdateLockFreeTimestamp() {
        long popTime = System.currentTimeMillis();
        long initialInvisibleTime = 30000;
        ShardingKeyLock lock = ShardingKeyLock.createWithInvisibleTime(popTime, initialInvisibleTime, "test");
        
        // 更新锁释放时间
        long newLockFreeTimestamp = popTime + 60000; // 60秒后释放
        lock.updateLockFreeTimestamp(newLockFreeTimestamp);
        
        assertEquals("lockFreeTimestamp should be updated", newLockFreeTimestamp, lock.getLockFreeTimestamp());
        assertEquals("invisibleTime should be recalculated", 
                newLockFreeTimestamp - popTime, lock.getInvisibleTime());
        assertEquals("popTime should not change", popTime, lock.getPopTime());
    }

    /**
     * 测试 updateInvisibleTime 方法
     */
    @Test
    public void testUpdateInvisibleTime() {
        long popTime = System.currentTimeMillis();
        long initialInvisibleTime = 30000;
        ShardingKeyLock lock = ShardingKeyLock.createWithInvisibleTime(popTime, initialInvisibleTime, "test");
        
        // 更新不可见时间
        long newInvisibleTime = 60000; // 60秒不可见时间
        lock.updateInvisibleTime(newInvisibleTime);
        
        assertEquals("invisibleTime should be updated", newInvisibleTime, lock.getInvisibleTime());
        assertEquals("lockFreeTimestamp should be recalculated", 
                popTime + newInvisibleTime, lock.getLockFreeTimestamp());
        assertEquals("popTime should not change", popTime, lock.getPopTime());
    }

    /**
     * 测试 needBlock 方法 - 相同 attemptId 不阻塞
     */
    @Test
    public void testNeedBlock_SameAttemptId() {
        long popTime = System.currentTimeMillis();
        long invisibleTime = 30000;
        String attemptId = "test-attempt-123";
        
        ShardingKeyLock lock = ShardingKeyLock.createWithInvisibleTime(popTime, invisibleTime, attemptId);
        lock.addOffset(100L);
        
        // 相同的 attemptId 不应该被阻塞
        assertFalse("same attemptId should not be blocked", lock.needBlock(attemptId));
    }

    /**
     * 测试 needBlock 方法 - 不同 attemptId 可能阻塞
     */
    @Test
    public void testNeedBlock_DifferentAttemptId() {
        long popTime = System.currentTimeMillis();
        long invisibleTime = 30000; // 30秒不可见时间
        String attemptId = "test-attempt-123";
        
        ShardingKeyLock lock = ShardingKeyLock.createWithInvisibleTime(popTime, invisibleTime, attemptId);
        lock.addOffset(100L);
        
        // 不同的 attemptId 且锁未过期且不为空，应该被阻塞
        assertTrue("different attemptId should be blocked when lock is active", 
                lock.needBlock("different-attempt-456"));
    }

    /**
     * 测试 needBlock 方法 - 锁已过期不阻塞
     */
    @Test
    public void testNeedBlock_ExpiredLock() {
        long popTime = System.currentTimeMillis() - 60000; // 1分钟前
        long invisibleTime = 30000; // 30秒不可见时间（已过期）
        String attemptId = "test-attempt-123";
        
        ShardingKeyLock lock = ShardingKeyLock.createWithInvisibleTime(popTime, invisibleTime, attemptId);
        lock.addOffset(100L);
        
        // 过期的锁不应该阻塞
        assertFalse("expired lock should not block", lock.needBlock("different-attempt-456"));
    }

    /**
     * 测试 needBlock 方法 - 空锁不阻塞
     */
    @Test
    public void testNeedBlock_EmptyLock() {
        long popTime = System.currentTimeMillis();
        long invisibleTime = 30000;
        String attemptId = "test-attempt-123";
        
        ShardingKeyLock lock = ShardingKeyLock.createWithInvisibleTime(popTime, invisibleTime, attemptId);
        // 不添加任何 offset，保持锁为空
        
        // 空锁不应该阻塞
        assertFalse("empty lock should not block", lock.needBlock("different-attempt-456"));
    }

    /**
     * 测试 needBlock 方法 - null attemptId 处理
     */
    @Test
    public void testNeedBlock_NullAttemptId() {
        long popTime = System.currentTimeMillis();
        long invisibleTime = 30000;
        String attemptId = "test-attempt-123";
        
        ShardingKeyLock lock = ShardingKeyLock.createWithInvisibleTime(popTime, invisibleTime, attemptId);
        lock.addOffset(100L);
        
        // null attemptId 应该被阻塞（因为与锁的 attemptId 不同）
        assertTrue("null attemptId should be blocked", lock.needBlock(null));
        
        // 测试锁的 attemptId 为 null 的情况
        ShardingKeyLock lockWithNullAttemptId = ShardingKeyLock.createWithInvisibleTime(popTime, invisibleTime, null);
        lockWithNullAttemptId.addOffset(200L);
        
        assertTrue("lock with null attemptId should block non-null attemptId", 
                lockWithNullAttemptId.needBlock("test-attempt"));
        assertTrue("lock with null attemptId should block null attemptId", 
                lockWithNullAttemptId.needBlock(null));
    }

    /**
     * 测试 Getters 和 Setters
     */
    @Test
    public void testGettersAndSetters() {
        ShardingKeyLock lock = new ShardingKeyLock();
        
        // 测试 offsetSet
        Set<Long> offsetSet = ConcurrentHashMap.newKeySet();
        offsetSet.add(100L);
        offsetSet.add(200L);
        lock.setOffsetSet(offsetSet);
        assertEquals("offsetSet should be set correctly", offsetSet, lock.getOffsetSet());
        
        // 测试设置 null offsetSet
        lock.setOffsetSet(null);
        assertNotNull("offsetSet should not be null after setting null", lock.getOffsetSet());
        assertTrue("offsetSet should be empty after setting null", lock.getOffsetSet().isEmpty());
        
        // 测试其他属性
        long lockFreeTimestamp = System.currentTimeMillis() + 30000;
        lock.setLockFreeTimestamp(lockFreeTimestamp);
        assertEquals("lockFreeTimestamp should be set correctly", lockFreeTimestamp, lock.getLockFreeTimestamp());
        
        long popTime = System.currentTimeMillis();
        lock.setPopTime(popTime);
        assertEquals("popTime should be set correctly", popTime, lock.getPopTime());
        
        long invisibleTime = 30000;
        lock.setInvisibleTime(invisibleTime);
        assertEquals("invisibleTime should be set correctly", invisibleTime, lock.getInvisibleTime());
        
        String attemptId = "test-attempt";
        lock.setAttemptId(attemptId);
        assertEquals("attemptId should be set correctly", attemptId, lock.getAttemptId());
        
        long createTime = System.currentTimeMillis();
        lock.setCreateTime(createTime);
        assertEquals("createTime should be set correctly", createTime, lock.getCreateTime());
    }

    /**
     * 测试 toString 方法
     */
    @Test
    public void testToString() {
        long popTime = System.currentTimeMillis();
        long invisibleTime = 30000;
        String attemptId = "test-attempt-123";
        
        ShardingKeyLock lock = ShardingKeyLock.createWithInvisibleTime(popTime, invisibleTime, attemptId);
        lock.addOffset(100L);
        lock.addOffset(200L);
        
        String toString = lock.toString();
        assertNotNull("toString should not be null", toString);
        assertThat(toString).contains("ShardingKeyLock");
        assertThat(toString).contains("offsetSet");
        assertThat(toString).contains("lockFreeTimestamp");
        assertThat(toString).contains("popTime");
        assertThat(toString).contains("invisibleTime");
        assertThat(toString).contains("attemptId");
        assertThat(toString).contains("createTime");
        assertThat(toString).contains("expired");
        assertThat(toString).contains("empty");
    }

    /**
     * 测试并发安全性
     */
    @Test
    public void testConcurrentSafety() throws InterruptedException {
        ShardingKeyLock lock = new ShardingKeyLock();
        
        // 创建多个线程同时操作 offsetSet
        Thread[] threads = new Thread[10];
        for (int i = 0; i < threads.length; i++) {
            final int threadId = i;
            threads[i] = new Thread(() -> {
                for (int j = 0; j < 100; j++) {
                    long offset = threadId * 100 + j;
                    lock.addOffset(offset);
                    lock.containsOffset(offset);
                    if (j % 2 == 0) {
                        lock.removeOffset(offset);
                    }
                }
            });
        }
        
        // 启动所有线程
        for (Thread thread : threads) {
            thread.start();
        }
        
        // 等待所有线程完成
        for (Thread thread : threads) {
            thread.join();
        }
        
        // 验证结果：应该有一半的offset被保留
        assertThat(lock.size()).isGreaterThan(0);
        assertThat(lock.size()).isLessThanOrEqualTo(500); // 最多500个offset（每个线程50个奇数offset）
    }
}