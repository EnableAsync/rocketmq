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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.codec.digest.MurmurHash3;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.tieredstore.util.MessageFormatUtil;

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
     * 从 ByteBuffer 中提取 sharding key
     *
     * @param byteBuffer 消息缓冲区
     * @return sharding key，如果没有则返回默认值
     */
    public static String extractShardingKeyFromBuffer(ByteBuffer byteBuffer) {
        if (byteBuffer == null) {
            return DEFAULT_SHARDING_KEY;
        }

        try {
            Map<String, String> properties = MessageFormatUtil.getProperties(byteBuffer);

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
     * 将长字符串通过 MurmurHash3 转换为一个固定长度的、适合做 Key 的十六进制字符串。
     * @param input 原始字符串
     * @return 32个字符的十六进制字符串 Key
     */
    public static String calculateHashKey(String input) {
        if (input == null) {
            long[] hash = MurmurHash3.hash128(DEFAULT_SHARDING_KEY);
            long h1 = hash[0];
            long h2 = hash[1];
            return String.format("%016x%016x", h1, h2);
        }
        byte[] data = input.getBytes(StandardCharsets.UTF_8);
        long[] hash = MurmurHash3.hash128(data);
        long h1 = hash[0];
        long h2 = hash[1];
        // 使用 String.format 进行零填充的十六进制转换
        // %016x 表示：0-补零，16-总长度16位，x-转为小写十六进制
        // 最终得到一个 16 + 16 = 32 个字符的字符串
        return String.format("%016x%016x", h1, h2);
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

        @Override
        public String toString() {
            return "MessageShardingInfo{" +
                "offsetToShardingKey=" + offsetToShardingKey +
                ", offsetToIndex=" + offsetToIndex +
                ", shardingKeyGroups=" + shardingKeyGroups +
                '}';
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
}