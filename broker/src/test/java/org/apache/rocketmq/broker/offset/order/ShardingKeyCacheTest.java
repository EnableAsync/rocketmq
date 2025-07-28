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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * ShardingKeyCache 单元测试类
 */
public class ShardingKeyCacheTest {

    private ShardingKeyCache cache;

    @Before
    public void setUp() {
        cache = new ShardingKeyCache();
    }

    @After
    public void tearDown() {
        if (cache != null) {
            cache.clear();
        }
    }

    /**
     * 测试 addAvailableMessage 方法 - 正常情况
     */
    @Test
    public void testAddAvailableMessage_Normal() throws Exception {
        String topic = "testTopic";
        String group = "testGroup";
        int queueId = 1;
        String shardingKey = "user123";
        GetMessageResult messageResult = createTestGetMessageResult();

        cache.addAvailableMessage(topic, group, queueId, shardingKey, messageResult);

        // 验证缓存统计
        ShardingKeyCache.CacheStatistics stats = cache.getStatistics();
        assertEquals("cached messages should be 1", 1, stats.getTotalCachedMessages());
        assertEquals("queue count should be 1", 1, stats.getQueueCount());

        // 验证队列缓存大小
        int cacheSize = cache.getQueueCacheSize(topic, group, queueId);
        assertEquals("queue cache size should be 1", 1, cacheSize);

        // 验证是否有可用消息
        assertTrue("should have available messages", cache.hasAvailableMessages(topic, group, queueId));
    }

    /**
     * 测试 addAvailableMessage 方法 - null GetMessageResult
     */
    @Test
    public void testAddAvailableMessage_NullMessageResult() {
        String topic = "testTopic";
        String group = "testGroup";
        int queueId = 1;
        String shardingKey = "user123";

        cache.addAvailableMessage(topic, group, queueId, shardingKey, null);

        // 应该没有添加任何消息
        ShardingKeyCache.CacheStatistics stats = cache.getStatistics();
        assertEquals("cached messages should be 0", 0, stats.getTotalCachedMessages());
    }

    /**
     * 测试 addAvailableMessage 方法 - 空消息列表
     */
    @Test
    public void testAddAvailableMessage_EmptyMessageList() {
        String topic = "testTopic";
        String group = "testGroup";
        int queueId = 1;
        String shardingKey = "user123";
        GetMessageResult emptyResult = new GetMessageResult();

        cache.addAvailableMessage(topic, group, queueId, shardingKey, emptyResult);

        // 应该没有添加任何消息
        ShardingKeyCache.CacheStatistics stats = cache.getStatistics();
        assertEquals("cached messages should be 0", 0, stats.getTotalCachedMessages());
    }

    /**
     * 测试 getAvailableMessages 方法 - 正常情况
     */
    @Test
    public void testGetAvailableMessages_Normal() throws Exception {
        String topic = "testTopic";
        String group = "testGroup";
        int queueId = 1;

        // 添加多个消息
        for (int i = 0; i < 5; i++) {
            String shardingKey = "user" + i;
            GetMessageResult messageResult = createTestGetMessageResult();
            cache.addAvailableMessage(topic, group, queueId, shardingKey, messageResult);
        }

        // 获取可用消息
        List<ShardingKeyCache.AvailableMessage> messages = cache.getAvailableMessages(topic, group, queueId, 3);

        assertNotNull("messages should not be null", messages);
        assertEquals("should get 3 messages", 3, messages.size());

        // 验证消息属性
        for (ShardingKeyCache.AvailableMessage message : messages) {
            assertEquals("topic should match", topic, message.getTopic());
            assertEquals("group should match", group, message.getGroup());
            assertEquals("queueId should match", queueId, message.getQueueId());
            assertNotNull("shardingKey should not be null", message.getShardingKey());
            assertNotNull("messageResult should not be null", message.getMessageResult());
            assertTrue("createTime should be greater than 0", message.getCreateTime() > 0);
        }

        // 验证缓存减少
        int remainingSize = cache.getQueueCacheSize(topic, group, queueId);
        assertEquals("remaining cache size should be 2", 2, remainingSize);

        // 验证统计信息
        ShardingKeyCache.CacheStatistics stats = cache.getStatistics();
        assertEquals("total hits should be 1", 1, stats.getTotalHits());
        assertEquals("cached messages should be 2", 2, stats.getTotalCachedMessages());
    }

    /**
     * 测试 getAvailableMessages 方法 - 空队列
     */
    @Test
    public void testGetAvailableMessages_EmptyQueue() {
        String topic = "testTopic";
        String group = "testGroup";
        int queueId = 1;

        List<ShardingKeyCache.AvailableMessage> messages = cache.getAvailableMessages(topic, group, queueId, 5);

        assertNotNull("messages should not be null", messages);
        assertTrue("messages should be empty", messages.isEmpty());

        // 验证统计信息
        ShardingKeyCache.CacheStatistics stats = cache.getStatistics();
        assertEquals("total misses should be 1", 1, stats.getTotalMisses());
    }

    /**
     * 测试 getAvailableMessagesByShardingKey 方法 - 正常情况
     */
    @Test
    public void testGetAvailableMessagesByShardingKey_Normal() throws Exception {
        String topic = "testTopic";
        String group = "testGroup";
        int queueId = 1;
        String targetShardingKey = "user123";

        // 添加不同 sharding key 的消息
        String[] shardingKeys = {"user123", "user456", "user123", "user789", "user123"};
        for (String shardingKey : shardingKeys) {
            GetMessageResult messageResult = createTestGetMessageResult();
            cache.addAvailableMessage(topic, group, queueId, shardingKey, messageResult);
        }

        // 获取特定 sharding key 的消息
        List<ShardingKeyCache.AvailableMessage> messages = cache.getAvailableMessagesByShardingKey(
                topic, group, queueId, targetShardingKey, 2);

        assertNotNull("messages should not be null", messages);
        assertEquals("should get 2 messages", 2, messages.size());

        // 验证所有消息都有正确的 sharding key
        for (ShardingKeyCache.AvailableMessage message : messages) {
            assertEquals("shardingKey should match", targetShardingKey, message.getShardingKey());
        }

        // 验证其他 sharding key 的消息还在缓存中
        int remainingSize = cache.getQueueCacheSize(topic, group, queueId);
        assertEquals("remaining cache size should be 3", 3, remainingSize);
    }

    /**
     * 测试 getAvailableMessagesByShardingKey 方法 - 没有匹配的 sharding key
     */
    @Test
    public void testGetAvailableMessagesByShardingKey_NoMatch() throws Exception {
        String topic = "testTopic";
        String group = "testGroup";
        int queueId = 1;

        // 添加消息
        GetMessageResult messageResult = createTestGetMessageResult();
        cache.addAvailableMessage(topic, group, queueId, "user123", messageResult);

        // 查找不存在的 sharding key
        List<ShardingKeyCache.AvailableMessage> messages = cache.getAvailableMessagesByShardingKey(
                topic, group, queueId, "user999", 5);

        assertNotNull("messages should not be null", messages);
        assertTrue("messages should be empty", messages.isEmpty());

        // 验证原消息还在缓存中
        int remainingSize = cache.getQueueCacheSize(topic, group, queueId);
        assertEquals("cache size should still be 1", 1, remainingSize);
    }

    /**
     * 测试 clearQueueCache 方法
     */
    @Test
    public void testClearQueueCache() throws Exception {
        String topic = "testTopic";
        String group = "testGroup";
        int queueId1 = 1;
        int queueId2 = 2;

        // 添加多个队列的消息
        GetMessageResult messageResult1 = createTestGetMessageResult();
        cache.addAvailableMessage(topic, group, queueId1, "user123", messageResult1);

        GetMessageResult messageResult2 = createTestGetMessageResult();
        cache.addAvailableMessage(topic, group, queueId2, "user456", messageResult2);

        // 验证初始状态
        assertEquals("queue1 cache size should be 1", 1, cache.getQueueCacheSize(topic, group, queueId1));
        assertEquals("queue2 cache size should be 1", 1, cache.getQueueCacheSize(topic, group, queueId2));

        // 清除队列1的缓存
        cache.clearQueueCache(topic, group, queueId1);

        // 验证队列1被清除，队列2保持不变
        assertEquals("queue1 cache size should be 0", 0, cache.getQueueCacheSize(topic, group, queueId1));
        assertEquals("queue2 cache size should still be 1", 1, cache.getQueueCacheSize(topic, group, queueId2));

        // 验证统计信息
        ShardingKeyCache.CacheStatistics stats = cache.getStatistics();
        assertEquals("total cached messages should be 1", 1, stats.getTotalCachedMessages());
    }

    /**
     * 测试 cleanupExpiredMessages 方法
     */
    @Test
    public void testCleanupExpiredMessages() throws Exception {
        String topic = "testTopic";
        String group = "testGroup";
        int queueId = 1;

        // 添加消息
        GetMessageResult messageResult = createTestGetMessageResult();
        cache.addAvailableMessage(topic, group, queueId, "user123", messageResult);

        // 验证初始状态
        assertEquals("cache size should be 1", 1, cache.getQueueCacheSize(topic, group, queueId));

        // 执行清理（由于消息是刚添加的，不会被清理）
        cache.cleanupExpiredMessages();

        // 验证消息仍然存在
        assertEquals("cache size should still be 1", 1, cache.getQueueCacheSize(topic, group, queueId));
    }

    /**
     * 测试 getStatistics 方法
     */
    @Test
    public void testGetStatistics() throws Exception {
        // 初始状态
        ShardingKeyCache.CacheStatistics initialStats = cache.getStatistics();
        assertEquals("initial cached messages should be 0", 0, initialStats.getTotalCachedMessages());
        assertEquals("initial hits should be 0", 0, initialStats.getTotalHits());
        assertEquals("initial misses should be 0", 0, initialStats.getTotalMisses());
        assertEquals("initial queue count should be 0", 0, initialStats.getQueueCount());
        assertEquals("initial hit ratio should be 0", 0.0, initialStats.getHitRatio(), 0.001);

        // 添加消息
        String topic = "testTopic";
        String group = "testGroup";
        int queueId = 1;
        GetMessageResult messageResult = createTestGetMessageResult();
        cache.addAvailableMessage(topic, group, queueId, "user123", messageResult);

        // 命中缓存
        List<ShardingKeyCache.AvailableMessage> messages = cache.getAvailableMessages(topic, group, queueId, 1);
        assertEquals("should get 1 message", 1, messages.size());

        // 未命中缓存
        cache.getAvailableMessages("otherTopic", "otherGroup", 99, 1);

        // 验证统计信息
        ShardingKeyCache.CacheStatistics stats = cache.getStatistics();
        assertEquals("cached messages should be 0 after retrieval", 0, stats.getTotalCachedMessages());
        assertEquals("hits should be 1", 1, stats.getTotalHits());
        assertEquals("misses should be 1", 1, stats.getTotalMisses());
        assertEquals("queue count should be 1", 1, stats.getQueueCount());
        assertEquals("hit ratio should be 0.5", 0.5, stats.getHitRatio(), 0.001);

        // 测试统计信息的 toString
        String statsString = stats.toString();
        assertNotNull("stats toString should not be null", statsString);
        assertThat(statsString).contains("CacheStatistics");
    }

    /**
     * 测试 hasAvailableMessages 方法
     */
    @Test
    public void testHasAvailableMessages() throws Exception {
        String topic = "testTopic";
        String group = "testGroup";
        int queueId = 1;

        // 初始状态
        assertFalse("should not have available messages initially",
                cache.hasAvailableMessages(topic, group, queueId));

        // 添加消息
        GetMessageResult messageResult = createTestGetMessageResult();
        cache.addAvailableMessage(topic, group, queueId, "user123", messageResult);

        // 验证有可用消息
        assertTrue("should have available messages after adding",
                cache.hasAvailableMessages(topic, group, queueId));

        // 取出消息
        cache.getAvailableMessages(topic, group, queueId, 1);

        // 验证没有可用消息
        assertFalse("should not have available messages after retrieval",
                cache.hasAvailableMessages(topic, group, queueId));
    }

    /**
     * 测试 clear 方法
     */
    @Test
    public void testClear() throws Exception {
        String topic = "testTopic";
        String group = "testGroup";

        // 添加多个队列的消息
        for (int queueId = 0; queueId < 3; queueId++) {
            GetMessageResult messageResult = createTestGetMessageResult();
            cache.addAvailableMessage(topic, group, queueId, "user" + queueId, messageResult);
        }

        // 验证初始状态
        ShardingKeyCache.CacheStatistics initialStats = cache.getStatistics();
        assertEquals("should have 3 cached messages", 3, initialStats.getTotalCachedMessages());
        assertEquals("should have 3 queues", 3, initialStats.getQueueCount());

        // 清空缓存
        cache.clear();

        // 验证清空后状态
        ShardingKeyCache.CacheStatistics clearedStats = cache.getStatistics();
        assertEquals("should have 0 cached messages after clear", 0, clearedStats.getTotalCachedMessages());
        assertEquals("should have 0 queues after clear", 0, clearedStats.getQueueCount());

        // 验证所有队列都被清空
        for (int queueId = 0; queueId < 3; queueId++) {
            assertEquals("queue " + queueId + " should be empty", 0,
                    cache.getQueueCacheSize(topic, group, queueId));
            assertFalse("queue " + queueId + " should not have messages",
                    cache.hasAvailableMessages(topic, group, queueId));
        }
    }

    /**
     * 测试 AvailableMessage 内部类
     */
    @Test
    public void testAvailableMessage() throws Exception {
        String topic = "testTopic";
        String group = "testGroup";
        int queueId = 1;
        String shardingKey = "user123";
        GetMessageResult messageResult = createTestGetMessageResult();

        ShardingKeyCache.AvailableMessage message = new ShardingKeyCache.AvailableMessage(
                topic, group, queueId, shardingKey, messageResult);

        // 验证属性
        assertEquals("topic should match", topic, message.getTopic());
        assertEquals("group should match", group, message.getGroup());
        assertEquals("queueId should match", queueId, message.getQueueId());
        assertEquals("shardingKey should match", shardingKey, message.getShardingKey());
        assertEquals("messageResult should match", messageResult, message.getMessageResult());
        assertTrue("createTime should be greater than 0", message.getCreateTime() > 0);

        // 测试匹配方法
        assertTrue("should match same queue", message.matchQueue(topic, group, queueId));
        assertFalse("should not match different topic", message.matchQueue("otherTopic", group, queueId));
        assertFalse("should not match different group", message.matchQueue(topic, "otherGroup", queueId));
        assertFalse("should not match different queueId", message.matchQueue(topic, group, 999));

        assertTrue("should match same shardingKey", message.matchShardingKey(shardingKey));
        assertFalse("should not match different shardingKey", message.matchShardingKey("otherKey"));

        // 测试过期检查
        Thread.sleep(100);
        assertFalse("should not be expired with large max time", message.isExpired(Long.MAX_VALUE));
        assertTrue("should be expired with 0 max time", message.isExpired(0));

        // 测试 toString
        String toString = message.toString();
        assertNotNull("toString should not be null", toString);
        assertThat(toString).contains("AvailableMessage");
    }

    /**
     * 测试 CacheStatistics 内部类
     */
    @Test
    public void testCacheStatistics() {
        long totalCachedMessages = 100;
        long totalHits = 80;
        long totalMisses = 20;
        int queueCount = 5;

        ShardingKeyCache.CacheStatistics stats = new ShardingKeyCache.CacheStatistics(
                totalCachedMessages, totalHits, totalMisses, queueCount);

        // 验证属性
        assertEquals("totalCachedMessages should match", totalCachedMessages, stats.getTotalCachedMessages());
        assertEquals("totalHits should match", totalHits, stats.getTotalHits());
        assertEquals("totalMisses should match", totalMisses, stats.getTotalMisses());
        assertEquals("queueCount should match", queueCount, stats.getQueueCount());

        // 验证命中率计算
        double expectedHitRatio = (double) totalHits / (totalHits + totalMisses);
        assertEquals("hit ratio should be calculated correctly", expectedHitRatio, stats.getHitRatio(), 0.001);

        // 测试零总数的命中率
        ShardingKeyCache.CacheStatistics zeroStats = new ShardingKeyCache.CacheStatistics(0, 0, 0, 0);
        assertEquals("hit ratio should be 0 when no hits or misses", 0.0, zeroStats.getHitRatio(), 0.001);

        // 测试 toString
        String toString = stats.toString();
        assertNotNull("toString should not be null", toString);
        assertThat(toString).contains("CacheStatistics");
    }

    /**
     * 测试并发安全性
     */
    @Test
    public void testConcurrentSafety() throws InterruptedException {
        final int threadCount = 10;
        final int operationsPerThread = 100;
        final CountDownLatch latch = new CountDownLatch(threadCount);
        final ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        try {
            // 启动多个线程同时操作缓存
            for (int i = 0; i < threadCount; i++) {
                final int threadId = i;
                executor.submit(() -> {
                    try {
                        for (int j = 0; j < operationsPerThread; j++) {
                            String topic = "topic" + (threadId % 3);
                            String group = "group" + (threadId % 3);
                            int queueId = threadId % 5;
                            String shardingKey = "user" + (threadId * operationsPerThread + j);

                            // 添加消息
                            GetMessageResult messageResult = createTestGetMessageResult();
                            cache.addAvailableMessage(topic, group, queueId, shardingKey, messageResult);

                            // 随机执行其他操作
                            if (j % 3 == 0) {
                                cache.getAvailableMessages(topic, group, queueId, 1);
                            } else if (j % 3 == 1) {
                                cache.getAvailableMessagesByShardingKey(topic, group, queueId, shardingKey, 1);
                            } else {
                                cache.getStatistics();
                            }
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        latch.countDown();
                    }
                });
            }

            // 等待所有线程完成
            assertTrue("All threads should complete within timeout",
                    latch.await(30, TimeUnit.SECONDS));

            // 验证最终状态一致性
            ShardingKeyCache.CacheStatistics finalStats = cache.getStatistics();
            assertTrue("final cached messages should be >= 0", finalStats.getTotalCachedMessages() >= 0);
            assertTrue("final hits should be >= 0", finalStats.getTotalHits() >= 0);
            assertTrue("final misses should be >= 0", finalStats.getTotalMisses() >= 0);
            assertTrue("final queue count should be >= 0", finalStats.getQueueCount() >= 0);
            assertTrue("final hit ratio should be between 0 and 1",
                    finalStats.getHitRatio() >= 0.0 && finalStats.getHitRatio() <= 1.0);

        } finally {
            executor.shutdown();
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        }
    }

    /**
     * 创建测试用的 GetMessageResult 对象
     */
    private GetMessageResult createTestGetMessageResult() throws Exception {
        GetMessageResult result = new GetMessageResult();

        // 创建包含不同 sharding key 的消息
        Map<String, String> properties1 = new HashMap<>();
        properties1.put(MessageConst.PROPERTY_SHARDING_KEY, "user123");
        MessageExt msg1 = createTestMessage("testTopic", "body1", properties1);
        byte[] bytes1 = MessageDecoder.encode(msg1, false);
        SelectMappedBufferResult buffer1 = createSelectMappedBufferResult(bytes1, 1000L);
        result.addMessage(buffer1, 1000L);

        Map<String, String> properties2 = new HashMap<>();
        properties2.put(MessageConst.PROPERTY_SHARDING_KEY, "user123");
        MessageExt msg2 = createTestMessage("testTopic", "body2", properties2);
        byte[] bytes2 = MessageDecoder.encode(msg2, false);
        SelectMappedBufferResult buffer2 = createSelectMappedBufferResult(bytes2, 1001L);
        result.addMessage(buffer2, 1001L);

        Map<String, String> properties3 = new HashMap<>();
        properties3.put(MessageConst.PROPERTY_SHARDING_KEY, "user456");
        MessageExt msg3 = createTestMessage("testTopic", "body3", properties3);
        byte[] bytes3 = MessageDecoder.encode(msg3, false);
        SelectMappedBufferResult buffer3 = createSelectMappedBufferResult(bytes3, 1002L);
        result.addMessage(buffer3, 1002L);

        return result;
    }

    /**
     * 创建测试用的 MessageExt 对象
     */
    private MessageExt createTestMessage(String topic, String body, Map<String, String> properties) {
        MessageExt messageExt = new MessageExt();
        messageExt.setTopic(topic);
        messageExt.setBody(body.getBytes(StandardCharsets.UTF_8));
        messageExt.setQueueId(0);
        messageExt.setQueueOffset(0L);
        messageExt.setCommitLogOffset(0L);
        messageExt.setBornTimestamp(System.currentTimeMillis());
        messageExt.setStoreTimestamp(System.currentTimeMillis());
        messageExt.setBornHost(new java.net.InetSocketAddress("127.0.0.1", 9876));
        messageExt.setStoreHost(new java.net.InetSocketAddress("127.0.0.1", 10911));
        messageExt.setMsgId("12345678901234567890123456789012");

        if (properties != null) {
            MessageAccessor.setProperties(messageExt, properties);
        }

        return messageExt;
    }

    /**
     * 创建测试用的 SelectMappedBufferResult 对象
     */
    private SelectMappedBufferResult createSelectMappedBufferResult(byte[] data, long startOffset) {
        SelectMappedBufferResult bufferResult = mock(SelectMappedBufferResult.class);
        when(bufferResult.getByteBuffer()).thenReturn(ByteBuffer.wrap(data));
        when(bufferResult.getSize()).thenReturn(data.length);
        when(bufferResult.getStartOffset()).thenReturn(startOffset);
        return bufferResult;
    }
}