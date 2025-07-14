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

import java.util.List;

/**
 * 顺序消费控制器接口
 * 这是顶层接口，封装了完整的顺序消费管理功能，支持不同的并发策略实现
 * 
 * 设计目标：
 * 1. 支持队列级别的顺序消费（现有实现）
 * 2. 支持消息组级别的顺序消费（提升并发度）
 * 3. 支持自定义的顺序消费策略
 * 
 * 与 OrderlyConsumeManager 的区别：
 * - OrderlyConsumeManager 专注于核心的顺序消费逻辑
 * - OrderlyConsumeController 提供完整的生命周期管理，包括数据更新、持久化等
 */
public interface OrderlyConsumeController {

    /**
     * 更新消息列表的接收状态
     * 当消费者POP消息时调用，用于记录消息状态和构建消费信息
     *
     * @param attemptId 尝试ID，用于标识同一批消息的消费尝试
     * @param isRetry 是否为重试主题
     * @param topic 主题名称
     * @param group 消费者组名称
     * @param queueId 队列ID
     * @param popTime 弹出消息的时间
     * @param invisibleTime 消息不可见时间
     * @param msgQueueOffsetList 消息的队列偏移量列表
     * @param orderInfoBuilder 用于构建顺序信息的字符串构建器
     */
    void update(String attemptId, boolean isRetry, String topic, String group, int queueId, 
                long popTime, long invisibleTime, List<Long> msgQueueOffsetList, 
                StringBuilder orderInfoBuilder);

    /**
     * 检查是否需要阻塞当前的POP请求
     * 用于确保顺序消息的顺序消费
     *
     * @param attemptId 尝试ID
     * @param topic 主题名称
     * @param group 消费者组名称
     * @param queueId 队列ID
     * @param invisibleTime 不可见时间
     * @return true表示需要阻塞，false表示可以继续
     */
    boolean checkBlock(String attemptId, String topic, String group, int queueId, long invisibleTime);

    /**
     * 提交消息并计算下一个消费偏移量
     * 当消费者ACK消息时调用
     *
     * @param topic 主题名称
     * @param group 消费者组名称
     * @param queueId 队列ID
     * @param queueOffset 消息的队列偏移量
     * @param popTime 弹出时间，用于验证
     * @return -1:非法, -2:无需提交, >=0:需要提交的偏移量
     */
    long commitAndNext(String topic, String group, int queueId, long queueOffset, long popTime);

    /**
     * 更新消息的下次可见时间
     * 用于消息的延时重新消费
     *
     * @param topic 主题名称
     * @param group 消费者组名称
     * @param queueId 队列ID
     * @param queueOffset 消息偏移量
     * @param popTime 弹出时间，用于验证
     * @param nextVisibleTime 下次可见时间
     */
    void updateNextVisibleTime(String topic, String group, int queueId, long queueOffset, 
                               long popTime, long nextVisibleTime);

    /**
     * 清除指定队列的阻塞状态
     * 通常在消费者重新平衡或队列重新分配时调用
     *
     * @param topic 主题名称
     * @param group 消费者组名称
     * @param queueId 队列ID
     */
    void clearBlock(String topic, String group, int queueId);

    /**
     * 获取控制器类型标识
     * 用于区分不同的实现策略
     *
     * @return 控制器类型，如：QUEUE_LEVEL, MESSAGE_GROUP_LEVEL 等
     */
    String getControllerType();

    /**
     * 启动控制器
     * 初始化必要的资源，如定时器、线程池等
     */
    void start();

    /**
     * 关闭控制器
     * 释放资源，清理定时任务等
     */
    void shutdown();
}