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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.header.ExtraInfoUtil;

/**
 * 消息组级别的顺序消费控制器
 * 基于消息组（sharding key）实现队列内的并发消费，显著提升并发度
 * 
 * 核心特点：
 * 1. 同一队列内不同消息组可以并行消费
 * 2. 只有相同消息组内的消息需要保持严格顺序
 * 3. 大幅提升并发度，特别适用于消息组分布均匀的场景
 * 
 * 数据结构：
 * topic@group -> queueId -> messageGroup -> OrderInfo
 */
public class MessageGroupOrderlyConsumeController implements OrderlyConsumeController {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    
    public static final String CONTROLLER_TYPE = "MESSAGE_GROUP_LEVEL";
    
    // Topic和Group的分隔符
    private static final String TOPIC_GROUP_SEPARATOR = "@";
    
    // 默认消息组，用于没有sharding key的消息
    private static final String DEFAULT_MESSAGE_GROUP = "_DEFAULT_GROUP_";
    
    /**
     * 核心数据结构：四层嵌套Map
     * 第一层：topic@group -> 第二层Map
     * 第二层：queueId -> 第三层Map  
     * 第三层：messageGroup -> OrderInfo
     * 
     * 这种结构支持队列内不同消息组的并发消费
     */
    private ConcurrentHashMap<String/* topic@group*/, 
            ConcurrentHashMap<Integer/*queueId*/, 
                ConcurrentHashMap<String/*messageGroup*/, ConsumerOrderInfoManager.OrderInfo>>> table =
        new ConcurrentHashMap<>(128);
    
    // 锁管理器，用于管理消费者的锁状态
    private ConsumerOrderInfoLockManager consumerOrderInfoLockManager;
    
    // Broker控制器引用
    private BrokerController brokerController;
    
    /**
     * 构造函数
     * @param brokerController Broker控制器
     */
    public MessageGroupOrderlyConsumeController(BrokerController brokerController, ConsumerOrderInfoLockManager consumerOrderInfoLockManager) {
        this.brokerController = brokerController;
    }
    
    @Override
    public void update(String attemptId, boolean isRetry, String topic, String group, int queueId,
                       long popTime, long invisibleTime, List<Long> msgQueueOffsetList,
                       StringBuilder orderInfoBuilder) {
        
        // 从attemptId中提取消息组信息
        String messageGroup = extractMessageGroup(attemptId);
        
        // 构建存储键
        String key = buildKey(topic, group);
        
        // 获取或创建队列映射
        ConcurrentHashMap<Integer/*queueId*/, ConcurrentHashMap<String/*messageGroup*/, ConsumerOrderInfoManager.OrderInfo>> queueMap = 
            table.computeIfAbsent(key, k -> new ConcurrentHashMap<>(16));
        
        // 获取或创建消息组映射
        ConcurrentHashMap<String/*messageGroup*/, ConsumerOrderInfoManager.OrderInfo> messageGroupMap = 
            queueMap.computeIfAbsent(queueId, k -> new ConcurrentHashMap<>(16));
        
        // 获取或创建该消息组的顺序信息
        ConsumerOrderInfoManager.OrderInfo orderInfo = messageGroupMap.get(messageGroup);
        if (orderInfo != null) {
            // 如果已存在，创建新的OrderInfo并合并消费计数信息
            ConsumerOrderInfoManager.OrderInfo newOrderInfo = new ConsumerOrderInfoManager.OrderInfo(
                attemptId, popTime, invisibleTime, msgQueueOffsetList, System.currentTimeMillis(), 0);
            
            // 合并消费次数信息（只合并相同消息组的）
            newOrderInfo.mergeOffsetConsumedCount(orderInfo.getAttemptId(), orderInfo.getOffsetList(), orderInfo.getOffsetConsumedCount());
            orderInfo = newOrderInfo;
        } else {
            // 创建新的OrderInfo
            orderInfo = new ConsumerOrderInfoManager.OrderInfo(
                attemptId, popTime, invisibleTime, msgQueueOffsetList, System.currentTimeMillis(), 0);
        }
        
        // 更新消息组的顺序信息
        messageGroupMap.put(messageGroup, orderInfo);
        
        // 构建消费次数信息，用于返回给客户端
        buildConsumeCountInfo(topic, queueId, messageGroup, orderInfo, orderInfoBuilder);
        
        // 更新锁释放时间戳（使用消息组级别的锁）
        updateLockFreeTimestamp(key, queueId, messageGroup, orderInfo);
    }
    
    @Override
    public boolean checkBlock(String attemptId, String topic, String group, int queueId, long invisibleTime) {
        // 从attemptId中提取消息组信息
        String messageGroup = extractMessageGroup(attemptId);
        
        String key = buildKey(topic, group);
        
        // 获取队列映射
        ConcurrentHashMap<Integer/*queueId*/, ConcurrentHashMap<String/*messageGroup*/, ConsumerOrderInfoManager.OrderInfo>> queueMap = 
            table.get(key);
        if (queueMap == null) {
            return false; // 没有顺序信息，不需要阻塞
        }
        
        // 获取消息组映射
        ConcurrentHashMap<String/*messageGroup*/, ConsumerOrderInfoManager.OrderInfo> messageGroupMap = queueMap.get(queueId);
        if (messageGroupMap == null) {
            return false; // 没有消息组信息，不需要阻塞
        }
        
        // 只检查相同消息组的阻塞状态
        ConsumerOrderInfoManager.OrderInfo orderInfo = messageGroupMap.get(messageGroup);
        if (orderInfo == null) {
            return false; // 该消息组没有顺序信息，不需要阻塞
        }
        
        // 调用OrderInfo的needBlock方法判断是否需要阻塞
        return orderInfo.needBlock(attemptId, invisibleTime);
    }
    
    @Override
    public long commitAndNext(String topic, String group, int queueId, long queueOffset, long popTime) {
        String key = buildKey(topic, group);
        
        // 获取队列映射
        ConcurrentHashMap<Integer/*queueId*/, ConcurrentHashMap<String/*messageGroup*/, ConsumerOrderInfoManager.OrderInfo>> queueMap = 
            table.get(key);
        if (queueMap == null) {
            return queueOffset + 1; // 没有顺序信息，返回下一个偏移量
        }
        
        // 获取消息组映射
        ConcurrentHashMap<String/*messageGroup*/, ConsumerOrderInfoManager.OrderInfo> messageGroupMap = queueMap.get(queueId);
        if (messageGroupMap == null) {
            return queueOffset + 1; // 没有消息组信息，返回下一个偏移量
        }
        
        // 遍历所有消息组，找到包含该偏移量的消息组
        for (Map.Entry<String, ConsumerOrderInfoManager.OrderInfo> entry : messageGroupMap.entrySet()) {
            String messageGroup = entry.getKey();
            ConsumerOrderInfoManager.OrderInfo orderInfo = entry.getValue();
            
            // 验证popTime
            if (popTime != orderInfo.getPopTime()) {
                continue; // popTime不匹配，跳过这个消息组
            }
            
            // 查找该偏移量在当前消息组中是否存在
            List<Long> offsetList = orderInfo.getOffsetList();
            if (offsetList.isEmpty()) {
                continue;
            }
            
            long firstOffset = offsetList.get(0);
            int size = offsetList.size();
            int offsetIndex = -1;
            
            // 找到对应的偏移量在列表中的位置
            for (int i = 0; i < size; i++) {
                long currentOffset = (i == 0) ? firstOffset : firstOffset + offsetList.get(i);
                if (currentOffset == queueOffset) {
                    offsetIndex = i;
                    break;
                }
            }
            
            if (offsetIndex >= 0) {
                // 找到了对应的偏移量，设置ACK标记
                orderInfo.setCommitOffsetBit(orderInfo.getCommitOffsetBit() | (1L << offsetIndex));
                
                // 计算下一个需要消费的偏移量
                long nextOffset = orderInfo.getNextOffset();
                
                // 更新锁释放时间戳
                updateLockFreeTimestamp(key, queueId, messageGroup, orderInfo);
                
                return nextOffset;
            }
        }
        
        log.warn("OrderInfo not found commit offset, key: {}, queueOffset: {}, queueId: {}", key, queueOffset, queueId);
        return -1; // 没有找到对应的偏移量
    }
    
    @Override
    public void updateNextVisibleTime(String topic, String group, int queueId, long queueOffset,
                                      long popTime, long nextVisibleTime) {
        String key = buildKey(topic, group);
        
        // 获取队列映射
        ConcurrentHashMap<Integer/*queueId*/, ConcurrentHashMap<String/*messageGroup*/, ConsumerOrderInfoManager.OrderInfo>> queueMap = 
            table.get(key);
        if (queueMap == null) {
            log.warn("queueMap is null. key: {}, queueOffset: {}, queueId: {}", key, queueOffset, queueId);
            return;
        }
        
        // 获取消息组映射
        ConcurrentHashMap<String/*messageGroup*/, ConsumerOrderInfoManager.OrderInfo> messageGroupMap = queueMap.get(queueId);
        if (messageGroupMap == null) {
            log.warn("messageGroupMap is null, key: {}, queueOffset: {}, queueId: {}", key, queueOffset, queueId);
            return;
        }
        
        // 遍历所有消息组，找到包含该偏移量的消息组
        for (Map.Entry<String, ConsumerOrderInfoManager.OrderInfo> entry : messageGroupMap.entrySet()) {
            String messageGroup = entry.getKey();
            ConsumerOrderInfoManager.OrderInfo orderInfo = entry.getValue();
            
            // 验证popTime
            if (popTime != orderInfo.getPopTime()) {
                continue; // popTime不匹配，跳过这个消息组
            }
            
            // 检查该偏移量是否在当前消息组中
            List<Long> offsetList = orderInfo.getOffsetList();
            if (offsetList.isEmpty()) {
                continue;
            }
            
            long firstOffset = offsetList.get(0);
            boolean found = false;
            
            for (int i = 0; i < offsetList.size(); i++) {
                long currentOffset = (i == 0) ? firstOffset : firstOffset + offsetList.get(i);
                if (currentOffset == queueOffset) {
                    found = true;
                    break;
                }
            }
            
            if (found) {
                // 更新指定偏移量的下次可见时间
                orderInfo.updateOffsetNextVisibleTime(queueOffset, nextVisibleTime);
                updateLockFreeTimestamp(key, queueId, messageGroup, orderInfo);
                return;
            }
        }
        
        log.warn("OrderInfo not found for offset update, key: {}, queueOffset: {}, queueId: {}", key, queueOffset, queueId);
    }
    
    @Override
    public void clearBlock(String topic, String group, int queueId) {
        String key = buildKey(topic, group);
        
        // 获取队列映射
        ConcurrentHashMap<Integer/*queueId*/, ConcurrentHashMap<String/*messageGroup*/, ConsumerOrderInfoManager.OrderInfo>> queueMap = 
            table.get(key);
        if (queueMap != null) {
            // 清除整个队列的消息组映射
            ConcurrentHashMap<String/*messageGroup*/, ConsumerOrderInfoManager.OrderInfo> messageGroupMap = queueMap.remove(queueId);
            if (messageGroupMap != null) {
                // 清除所有消息组的锁信息
                for (String messageGroup : messageGroupMap.keySet()) {
                    clearMessageGroupLock(key, queueId, messageGroup);
                }
                log.info("Clear all message groups for queue, topic:{}, group:{}, queueId:{}, groupCount:{}",
                    topic, group, queueId, messageGroupMap.size());
            }
        }
    }

    @Override
    public String getControllerType() {
        return CONTROLLER_TYPE;
    }

    @Override
    public void start() {
        if (consumerOrderInfoLockManager != null) {
            consumerOrderInfoLockManager.start();
        }
        log.info("MessageGroupOrderlyConsumeController started");
    }

    @Override
    public void shutdown() {
        if (consumerOrderInfoLockManager != null) {
            consumerOrderInfoLockManager.shutdown();
        }
        log.info("MessageGroupOrderlyConsumeController shutdown");
    }

    // ================== 私有辅助方法 ==================

    /**
     * 构建Topic和Group的组合键
     */
    private static String buildKey(String topic, String group) {
        return topic + TOPIC_GROUP_SEPARATOR + group;
    }

    /**
     * 从attemptId中提取消息组信息
     * attemptId格式通常包含sharding key信息，这里简化处理
     *
     * 实际实现中，可以从消息属性或attemptId中解析出sharding key
     * 如果没有sharding key，使用默认消息组
     */
    private String extractMessageGroup(String attemptId) {
        if (attemptId == null || attemptId.isEmpty()) {
            return DEFAULT_MESSAGE_GROUP;
        }

        // 尝试从attemptId中提取消息组信息
        // 格式可能是: {messageGroup}_{timestamp}_{random} 等
        // 这里简化处理，可以根据实际的attemptId格式进行调整
        int firstUnderScore = attemptId.indexOf('_');
        if (firstUnderScore > 0) {
            String possibleGroup = attemptId.substring(0, firstUnderScore);
            // 验证是否是有效的消息组标识
            if (possibleGroup.length() > 0 && possibleGroup.length() <= 64) {
                return possibleGroup;
            }
        }

        // 默认情况下，使用attemptId的hash作为消息组
        // 这样可以保证相同sharding key的消息有相同的消息组
        return String.valueOf(Math.abs(attemptId.hashCode() % 1000));
    }

    /**
     * 构建消费次数信息
     */
    private void buildConsumeCountInfo(String topic, int queueId, String messageGroup,
                                       ConsumerOrderInfoManager.OrderInfo orderInfo,
                                       StringBuilder orderInfoBuilder) {
        Map<Long, Integer> offsetConsumedCount = orderInfo.getOffsetConsumedCount();
        int minConsumedTimes = Integer.MAX_VALUE;

        if (offsetConsumedCount != null) {
            Set<Long> offsetSet = offsetConsumedCount.keySet();
            // 为每个偏移量构建消费次数信息
            for (Long offset : offsetSet) {
                Integer consumedTimes = offsetConsumedCount.getOrDefault(offset, 0);
                ExtraInfoUtil.buildQueueOffsetOrderCountInfo(orderInfoBuilder, topic, queueId, offset, consumedTimes);
                minConsumedTimes = Math.min(minConsumedTimes, consumedTimes);
            }
            if (offsetConsumedCount.size() != orderInfo.getOffsetList().size()) {
                // 如果大小不等，说明有新消息，最小消费次数为0
                minConsumedTimes = 0;
            }
        } else {
            minConsumedTimes = 0;
        }

        // 为了兼容性，构建队列级别的消费次数信息
        ExtraInfoUtil.buildQueueIdOrderCountInfo(orderInfoBuilder, topic, queueId, minConsumedTimes);

        // 添加消息组信息到orderInfoBuilder中，用于客户端识别
        orderInfoBuilder.append("_MG_").append(messageGroup).append("_");
    }

    /**
     * 更新锁释放时间戳（消息组级别）
     */
    private void updateLockFreeTimestamp(String key, int queueId, String messageGroup,
                                         ConsumerOrderInfoManager.OrderInfo orderInfo) {
        if (consumerOrderInfoLockManager != null) {
            // 注意：这里需要根据消息组来管理锁，而不是整个队列
            // 可以使用组合键来区分不同消息组的锁
            String[] keyParts = key.split(TOPIC_GROUP_SEPARATOR);
            if (keyParts.length == 2) {
                String topic = keyParts[0];
                String group = keyParts[1];
                // 使用消息组作为锁的标识，实现消息组级别的锁管理
                consumerOrderInfoLockManager.updateLockFreeTimestamp(topic, group + "_" + messageGroup, queueId, orderInfo);
            }
        }
    }

    /**
     * 清除消息组级别的锁信息
     */
    private void clearMessageGroupLock(String key, int queueId, String messageGroup) {
        if (consumerOrderInfoLockManager != null) {
            String[] keyParts = key.split(TOPIC_GROUP_SEPARATOR);
            if (keyParts.length == 2) {
                String topic = keyParts[0];
                String group = keyParts[1];
                consumerOrderInfoLockManager.clearLock(topic, group + "_" + messageGroup, queueId);
            }
        }
    }
}