package org.apache.rocketmq.broker.offset.order;

import java.util.List;
import org.apache.rocketmq.common.OrderedConsumptionLevel;
import org.apache.rocketmq.store.GetMessageResult;

/**
 * 顺序消费控制器接口
 * 这是顶层接口，封装了完整的顺序消费管理功能，支持不同的并发策略实现
 * <p>
 * 设计目标：
 * 1. 支持队列级别的顺序消费（现有实现）
 * 2. 支持消息组级别的顺序消费（提升并发度）
 * 3. 支持自定义的顺序消费策略
 * </p>
 */
public interface OrderedConsumptionManager {

    /**
     * 更新消息列表的接收状态
     * 当消费者POP消息时被 handleGetMessageResult 调用，用于记录消息状态和构建消费信息
     *
     * @param attemptId          区分不同的 pop 请求
     * @param isRetry            是否为重试主题
     * @param topic              主题名称
     * @param group              消费者组名称
     * @param queueId            队列ID
     * @param popTime            弹出消息的时间
     * @param invisibleTime      消息不可见时间
     * @param msgQueueOffsetList 消息的队列偏移量列表
     * @param orderInfoBuilder   用于构建顺序信息的字符串构建器
     * @param getMessageResult   返回新的 result
     */
    void update(String attemptId, boolean isRetry, String topic, String group, int queueId,
        long popTime, long invisibleTime, List<Long> msgQueueOffsetList,
        StringBuilder orderInfoBuilder, GetMessageResult getMessageResult);

    /**
     * 检查是否需要阻塞当前的 POP 请求
     * 用于确保顺序消息的顺序消费
     * 当消费者 POP 消息时调用
     *
     * @param attemptId     尝试ID
     * @param topic         主题名称
     * @param group         消费者组名称
     * @param queueId       队列ID
     * @param invisibleTime 不可见时间
     * @return true表示需要阻塞，false表示可以继续
     */
    boolean checkBlock(String attemptId, String topic, String group, int queueId, long invisibleTime);

    /**
     * 提交消息并计算下一个消费偏移量
     * 当消费者 ACK 消息时调用
     *
     * @param topic       主题名称
     * @param group       消费者组名称
     * @param queueId     队列ID
     * @param queueOffset 消息的队列偏移量
     * @param popTime     弹出时间，用于验证
     * @return -1:非法, -2:无需提交, >=0:需要提交的偏移量(表明小于这个偏移量的消息已经被消费)
     */
    long commitAndNext(String topic, String group, int queueId, long queueOffset, long popTime);

    /**
     * 更新消息的下次可见时间
     * 用于消息的延时重新消费
     *
     * @param topic           主题名称
     * @param group           消费者组名称
     * @param queueId         队列ID
     * @param queueOffset     消息偏移量
     * @param popTime         弹出时间，用于验证
     * @param nextVisibleTime 下次可见时间
     */
    void updateNextVisibleTime(String topic, String group, int queueId, long queueOffset,
        long popTime, long nextVisibleTime);

    /**
     * 清除指定队列的阻塞状态
     * 通常在消费者重新平衡或队列重新分配时调用
     *
     * @param topic   主题名称
     * @param group   消费者组名称
     * @param queueId 队列ID
     */
    void clearBlock(String topic, String group, int queueId);

    /**
     * 获取顺序消费等级
     * 用于区分不同的实现策略
     *
     * @return 顺序消费等级，如：QUEUE, MESSAGE_GROUP 等
     */
    OrderedConsumptionLevel getOrderedConsumptionLevel();

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

    /**
     * 持久化控制器
     * 将控制器的数据持久化
     */
    void persist();

    /**
     * 加载控制器
     * 从存储加载数据
     */
    boolean load();

    /**
     * 获取可用消息结果
     * 用于从缓存中获取消息
     */
    GetMessageResult getAvailableMessageResult(String attemptId, long popTime, long invisibleTime, String groupId,
        String topicId, int queueId, int batchSize, StringBuilder orderCountInfoBuilder);
}