package org.apache.rocketmq.broker.offset.order;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

import static org.apache.rocketmq.broker.offset.order.ConsumerOrderInfoManager.buildKey;

public class SimpleOrderlyConsumeManager implements OrderlyConsumeManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    /**
     * 核心数据结构：存储所有的顺序信息
     * 外层Map的Key: "topic@group" 格式的字符串
     * 内层Map的Key: queueId（队列ID）
     * Value: OrderInfo对象，包含该队列的消费顺序信息
     */
    private ConcurrentHashMap<String/* topic@group*/, ConcurrentHashMap<Integer/*queueId*/, ConsumerOrderInfoManager.OrderInfo>> table =
        new ConcurrentHashMap<>(128);

    // 锁管理器，用于管理消费者的锁状态（transient表示不会被序列化）
    private transient ConsumerOrderInfoLockManager consumerOrderInfoLockManager;

    public SimpleOrderlyConsumeManager() {
    }

    public SimpleOrderlyConsumeManager(ConsumerOrderInfoLockManager consumerOrderInfoLockManager) {
        this.consumerOrderInfoLockManager = consumerOrderInfoLockManager;
    }

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
     *
     * 调用链路：
     * 1. PopMessageProcessor.processRequest() ->
     * 2. PopMessageProcessor.popMsgFromQueue() ->
     * 3. ConsumerOrderInfoManager.checkBlock()
     */
    @Override
    public boolean checkBlock(String attemptId, String topic, String group, int queueId, long invisibleTime) {
        String key = buildKey(topic, group);

        // 获取或创建队列映射
        ConcurrentHashMap<Integer/*queueId*/, ConsumerOrderInfoManager.OrderInfo> qs = table.get(key);
        if (qs == null) {
            qs = new ConcurrentHashMap<>(16);
            ConcurrentHashMap<Integer/*queueId*/, ConsumerOrderInfoManager.OrderInfo> old = table.putIfAbsent(key, qs);
            if (old != null) {
                qs = old;
            }
        }

        ConsumerOrderInfoManager.OrderInfo orderInfo = qs.get(queueId);
        if (orderInfo == null) {
            return false; // 没有顺序信息，不需要阻塞
        }

        // 调用OrderInfo的needBlock方法判断是否需要阻塞
        return orderInfo.needBlock(attemptId, invisibleTime);
    }

    /**
     * 提交消息并计算下一个消费偏移量
     * 这是核心方法之一，当消费者ACK消息时调用
     *
     * @param topic 主题名称
     * @param group 消费者组名称
     * @param queueId 队列ID
     * @param queueOffset 消息的队列偏移量
     * @param popTime 弹出时间，用于验证
     * @return -1:非法, -2:无需提交, >=0:需要提交的偏移量
     *
     * 调用链路：
     * 1. AckMessageProcessor.processRequest() ->
     * 2. PopMessageService.ackMessage() ->
     * 3. ConsumerOrderInfoManager.commitAndNext()
     */
    @Override
    public long commitAndNext(String topic, String group, int queueId, long queueOffset, long popTime) {
        String key = buildKey(topic, group);

        // 获取队列映射
        ConcurrentHashMap<Integer/*queueId*/, ConsumerOrderInfoManager.OrderInfo> qs = table.get(key);
        if (qs == null) {
            return queueOffset + 1; // 没有顺序信息，返回下一个偏移量
        }

        ConsumerOrderInfoManager.OrderInfo orderInfo = qs.get(queueId);
        if (orderInfo == null) {
            log.warn("OrderInfo is null, {}, {}, {}", key, queueOffset, orderInfo);
            return queueOffset + 1;
        }

        List<Long> o = orderInfo.getOffsetList();
        if (o == null || o.isEmpty()) {
            log.warn("OrderInfo is empty, {}, {}, {}", key, queueOffset, orderInfo);
            return -1;
        }

        // 验证popTime是否匹配，防止重复ACK
        if (popTime != orderInfo.getPopTime()) {
            log.warn("popTime is not equal to orderInfo saved. key: {}, offset: {}, orderInfo: {}, popTime: {}", key, queueOffset, orderInfo, popTime);
            return -2;
        }

        // 在偏移量列表中查找要ACK的消息
        Long first = o.get(0);
        int i = 0, size = o.size();
        for (; i < size; i++) {
            long temp;
            if (i == 0) {
                temp = first; // 第一个元素就是实际偏移量
            } else {
                temp = first + o.get(i); // 其他元素是相对于第一个元素的差值
            }
            if (queueOffset == temp) {
                break; // 找到要ACK的消息
            }
        }

        // 没有找到对应的偏移量
        if (i >= size) {
            log.warn("OrderInfo not found commit offset, {}, {}, {}", key, queueOffset, orderInfo);
            return -1;
        }

        // 设置对应位的ACK标记（使用位运算）
        orderInfo.setCommitOffsetBit(orderInfo.getCommitOffsetBit() | (1L << i));

        // 计算下一个需要消费的偏移量
        long nextOffset = orderInfo.getNextOffset();

        // 更新锁释放时间戳
        updateLockFreeTimestamp(topic, group, queueId, orderInfo);

        return nextOffset;
    }

    /**
     * 更新消息的下次可见时间
     * 用于消息的延时重新消费
     *
     * @param topic 主题名称
     * @param group 消费者组名称
     * @param queueId 队列ID
     * @param queueOffset 消息偏移量
     * @param nextVisibleTime 下次可见时间
     *
     * 调用链路：
     * 1. ChangeInvisibleTimeProcessor.processRequest() ->
     * 2. PopMessageService.changeInvisibleTime() ->
     * 3. ConsumerOrderInfoManager.updateNextVisibleTime()
     */
    @Override
    public void updateNextVisibleTime(String topic, String group, int queueId, long queueOffset, long popTime, long nextVisibleTime) {
        String key = buildKey(topic, group);
        ConcurrentHashMap<Integer/*queueId*/, ConsumerOrderInfoManager.OrderInfo> qs = table.get(key);
        if (qs == null) {
            log.warn("orderInfo of queueId is null. key: {}, queueOffset: {}, queueId: {}", key, queueOffset, queueId);
            return;
        }
        ConsumerOrderInfoManager.OrderInfo orderInfo = qs.get(queueId);
        if (orderInfo == null) {
            log.warn("orderInfo is null, key: {}, queueOffset: {}, queueId: {}", key, queueOffset, queueId);
            return;
        }
        // 验证popTime
        if (popTime != orderInfo.getPopTime()) {
            log.warn("popTime is not equal to orderInfo saved. key: {}, queueOffset: {}, orderInfo: {}, popTime: {}", key, queueOffset, orderInfo, popTime);
            return;
        }
        // 更新指定偏移量的下次可见时间
        orderInfo.updateOffsetNextVisibleTime(queueOffset, nextVisibleTime);
        updateLockFreeTimestamp(topic, group, queueId, orderInfo);
    }

    /**
     * 清除指定队列的阻塞状态
     * 通常在消费者重新平衡或队列重新分配时调用
     */
    @Override
    public void clearBlock(String topic, String group, int queueId) {
        table.computeIfPresent(buildKey(topic, group), (key, val) -> {
            val.remove(queueId);
            return val;
        });
    }
}
