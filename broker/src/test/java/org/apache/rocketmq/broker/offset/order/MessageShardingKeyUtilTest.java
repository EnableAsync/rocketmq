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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.junit.Test;

/**
 * MessageShardingKeyUtil 单元测试类
 */
public class MessageShardingKeyUtilTest {

    /**
     * 测试 calculateShardingKeyHash 方法
     */
    @Test
    public void testCalculateShardingKeyHash() {
        // 测试正常的 sharding key
        String shardingKey1 = "user123";
        String hash1 = MessageShardingKeyUtil.calculateHashKey(shardingKey1);
        assertNotNull("hash should not be null", hash1);
        
        // 测试相同的 sharding key 应该产生相同的 hash
        String hash1Again = MessageShardingKeyUtil.calculateHashKey(shardingKey1);
        assertEquals("same sharding key should produce same hash", hash1, hash1Again);
        
        // 测试不同的 sharding key 应该产生不同的 hash（大概率）
        String shardingKey2 = "user456";
        String hash2 = MessageShardingKeyUtil.calculateHashKey(shardingKey2);
        assertNotNull("hash2 should not be null", hash2);
        assertThat(hash1).isNotEqualTo(hash2);
        
        // 测试 null sharding key
        String hashNull = MessageShardingKeyUtil.calculateHashKey(null);
        assertNotNull("hash for null should not be null", hashNull);
        
        // 测试空字符串
        String hashEmpty = MessageShardingKeyUtil.calculateHashKey("");
        assertNotNull("hash for empty string should not be null", hashEmpty);
        
        // 测试特殊字符
        String specialKey = "user@#$%^&*()_+-={}[]|\\:;\"'<>?,./";
        String hashSpecial = MessageShardingKeyUtil.calculateHashKey(specialKey);
        assertNotNull("hash for special characters should not be null", hashSpecial);
        
        // 测试中文字符
        String chineseKey = "用户123";
        String hashChinese = MessageShardingKeyUtil.calculateHashKey(chineseKey);
        assertNotNull("hash for chinese characters should not be null", hashChinese);
    }

    /**
     * 测试 buildTopicGroupIdentifier 方法
     */
    @Test
    public void testBuildTopicGroupIdentifier() {
        String topic = "testTopic";
        String group = "testGroup";
        
        String identifier = MessageShardingKeyUtil.buildTopicGroupIdentifier(topic, group);
        assertEquals("topic@group format should be correct", "testTopic@testGroup", identifier);
        
        // 测试包含特殊字符的情况
        String topicSpecial = "topic@with@at";
        String groupSpecial = "group@with@at";
        String identifierSpecial = MessageShardingKeyUtil.buildTopicGroupIdentifier(topicSpecial, groupSpecial);
        assertEquals("special characters should be preserved", "topic@with@at@group@with@at", identifierSpecial);
        
        // 测试空字符串
        String identifierEmpty = MessageShardingKeyUtil.buildTopicGroupIdentifier("", "");
        assertEquals("empty strings should work", "@", identifierEmpty);
        
        // 测试 null 参数（如果支持的话）
        String identifierNull = MessageShardingKeyUtil.buildTopicGroupIdentifier(null, null);
        assertEquals("null parameters should be handled", "null@null", identifierNull);
    }

    /**
     * 测试 buildTopicGroupQueueIdentifier 方法
     */
    @Test
    public void testBuildTopicGroupQueueIdentifier() {
        String topic = "testTopic";
        String group = "testGroup";
        int queueId = 5;
        
        String identifier = MessageShardingKeyUtil.buildTopicGroupQueueIdentifier(topic, group, queueId);
        assertEquals("topic@group@queueId format should be correct", "testTopic@testGroup@5", identifier);
        
        // 测试负数队列ID
        String identifierNegative = MessageShardingKeyUtil.buildTopicGroupQueueIdentifier(topic, group, -1);
        assertEquals("negative queueId should work", "testTopic@testGroup@-1", identifierNegative);
        
        // 测试大数队列ID
        String identifierLarge = MessageShardingKeyUtil.buildTopicGroupQueueIdentifier(topic, group, Integer.MAX_VALUE);
        assertEquals("large queueId should work", "testTopic@testGroup@" + Integer.MAX_VALUE, identifierLarge);
    }

    /**
     * 测试 buildShardingKeyIdentifier 方法
     */
    @Test
    public void testBuildShardingKeyIdentifier() {
        String topic = "testTopic";
        String group = "testGroup";
        int queueId = 3;
        String shardingKey = "user123";
        
        String identifier = MessageShardingKeyUtil.buildShardingKeyIdentifier(topic, group, queueId, shardingKey);
        assertEquals("sharding key identifier format should be correct", 
                "testTopic@testGroup@3@user123", identifier);
        
        // 测试包含分隔符的 sharding key
        String shardingKeyWithSeparator = "user@123";
        String identifierWithSeparator = MessageShardingKeyUtil.buildShardingKeyIdentifier(
                topic, group, queueId, shardingKeyWithSeparator);
        assertEquals("sharding key with separator should work", 
                "testTopic@testGroup@3@user@123", identifierWithSeparator);
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