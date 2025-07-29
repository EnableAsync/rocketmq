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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.SelectMappedBufferResult;

/**
 * 消息 Sharding Key 提取和处理工具类
 * 负责从消息中提取 sharding key，并提供相关的分组和过滤功能
 */
public class MessageShardingKeyUtil {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    /**
     * 默认 sharding key，用于没有指定 sharding key 的消息
     */
    public static final String DEFAULT_SHARDING_KEY = "__DEFAULT_SHARDING_KEY__";

    /**
     * 从 GetMessageResult 中提取 sharding key 信息
     *
     * @param getMessageResult 消息结果
     * @return MessageShardingInfo 包含 sharding key 分组信息
     */
    public static MessageShardingInfo extractShardingKeyInfo(GetMessageResult getMessageResult) {
        MessageShardingInfo shardingInfo = new MessageShardingInfo();

        if (getMessageResult == null || getMessageResult.getMessageMapedList() == null) {
            return shardingInfo;
        }

        List<SelectMappedBufferResult> messageMapedList = getMessageResult.getMessageMapedList();
        List<Long> messageQueueOffsetList = getMessageResult.getMessageQueueOffset();

        for (int i = 0; i < messageMapedList.size(); i++) {
            SelectMappedBufferResult mappedBuffer = messageMapedList.get(i);
            Long offset = i < messageQueueOffsetList.size() ? messageQueueOffsetList.get(i) : -1L;

            String shardingKey = extractShardingKeyFromBuffer(mappedBuffer);
            shardingInfo.addMessage(offset, shardingKey, i);
        }

        return shardingInfo;
    }

    /**
     * 从 SelectMappedBufferResult 中提取 sharding key
     *
     * @param mappedBuffer 消息缓冲区
     * @return sharding key，如果没有则返回默认值
     */
    public static String extractShardingKeyFromBuffer(SelectMappedBufferResult mappedBuffer) {
        if (mappedBuffer == null) {
            return DEFAULT_SHARDING_KEY;
        }

        try {
            ByteBuffer byteBuffer = mappedBuffer.getByteBuffer();
            if (byteBuffer == null) {
                return DEFAULT_SHARDING_KEY;
            }

            // 使用 decodeProperties 直接解析属性
            Map<String, String> properties = MessageDecoder.decodeProperties(byteBuffer);

            if (properties != null) {
                String shardingKey = properties.get(MessageConst.PROPERTY_SHARDING_KEY);
                return shardingKey != null ? shardingKey : DEFAULT_SHARDING_KEY;
            }
        } catch (Exception e) {
            log.warn("Failed to decode properties for sharding key extraction", e);
        }

        return DEFAULT_SHARDING_KEY;
    }

    /**
     * 计算 sharding key 的哈希值（用于分片）
     */
    public static long calculateShardingKeyHash(String shardingKey) {
        if (shardingKey == null || shardingKey.isEmpty()) {
            return DEFAULT_SHARDING_KEY.hashCode();
        }
        return Math.abs(shardingKey.hashCode());
    }

    /**
     * 构建主题组标识符
     */
    public static String buildTopicGroupIdentifier(String topic, String group) {
        return topic + "@" + group;
    }

    /**
     * 构建主题组队列标识符
     */
    public static String buildTopicGroupQueueIdentifier(String topic, String group, int queueId) {
        return topic + "@" + group + "@" + queueId;
    }

    /**
     * 构建 sharding key 标识符
     */
    public static String buildShardingKeyIdentifier(String topic, String group, int queueId, String shardingKey) {
        return topic + "@" + group + "@" + queueId + "@" + shardingKey;
    }

    /**
     * 消息 sharding key 信息容器
     */
    public static class MessageShardingInfo {
        /**
         * offset到sharding key的映射
         */
        private final Map<Long, String> offsetToShardingKey = new HashMap<>();

        /**
         * offset到消息索引的映射（在GetMessageResult中的索引）
         */
        private final Map<Long, Integer> offsetToIndex = new HashMap<>();

        /**
         * 按sharding key分组的消息
         */
        private final Map<String, List<MessageInfo>> shardingKeyGroups = new HashMap<>();

        public void addMessage(Long offset, String shardingKey, Integer index) {
            offsetToShardingKey.put(offset, shardingKey);
            offsetToIndex.put(offset, index);

            shardingKeyGroups.computeIfAbsent(shardingKey, k -> new ArrayList<>())
                .add(new MessageInfo(offset, index, shardingKey));
        }

        public Map<Long, String> getOffsetToShardingKey() {
            return offsetToShardingKey;
        }

        public Map<Long, Integer> getOffsetToIndex() {
            return offsetToIndex;
        }

        public Map<String, List<MessageInfo>> getShardingKeyGroups() {
            return shardingKeyGroups;
        }

        public String getShardingKeyByOffset(Long offset) {
            return offsetToShardingKey.get(offset);
        }
    }

    /**
     * 消息信息
     */
    public static class MessageInfo {
        private final Long offset;
        private final Integer index;
        private final String shardingKey;

        public MessageInfo(Long offset, Integer index, String shardingKey) {
            this.offset = offset;
            this.index = index;
            this.shardingKey = shardingKey;
        }

        public Long getOffset() {
            return offset;
        }

        public Integer getIndex() {
            return index;
        }

        public String getShardingKey() {
            return shardingKey;
        }
    }

    /**
     * 消息过滤结果
     */
    public static class MessageFilterResult {
        /**
         * 可用的消息（按sharding key分组）
         */
        private final Map<String, List<MessageInfo>> availableMessagesByShardingKey = new HashMap<>();

        /**
         * 被阻塞的消息（按sharding key分组）
         */
        private final Map<String, List<MessageInfo>> blockedMessagesByShardingKey = new HashMap<>();

        public void addAvailableMessages(String shardingKey, List<MessageInfo> messages) {
            availableMessagesByShardingKey.put(shardingKey, messages);
        }

        public void addBlockedMessages(String shardingKey, List<MessageInfo> messages) {
            blockedMessagesByShardingKey.put(shardingKey, messages);
        }

        public Map<String, List<MessageInfo>> getAvailableMessagesByShardingKey() {
            return availableMessagesByShardingKey;
        }

        public Map<String, List<MessageInfo>> getBlockedMessagesByShardingKey() {
            return blockedMessagesByShardingKey;
        }

        public boolean hasAvailableMessages() {
            return !availableMessagesByShardingKey.isEmpty();
        }

        public boolean hasBlockedMessages() {
            return !blockedMessagesByShardingKey.isEmpty();
        }
    }
}