package org.apache.rocketmq.broker.offset.order;

public interface OrderlyConsumeManager {

    // 被 org.apache.rocketmq.broker.pop.PopConsumerService.isFifoBlocked 调用，用于判断是否可以取数据
    // org.apache.rocketmq.broker.processor.AckMessageProcessor.ackOrderlyNew 也调用了，用于判断是否触发触发消息到达通知
    boolean checkBlock(String attemptId, String topic, String group, int queueId, long invisibleTime);

    // 被 org.apache.rocketmq.broker.processor.AckMessageProcessor.ackOrderlyNew 调用
    // 用于提交当前偏移量并获取下一个可消费偏移量，下一个可消费偏移量用于判断
    long commitAndNext(String topic, String group, int queueId, long queueOffset, long popTime);

    // 被 org.apache.rocketmq.broker.processor.ChangeInvisibleTimeProcessor.processChangeInvisibleTimeForOrderNew 调用
    // 用于延长顺序消息的不可见时间
    void updateNextVisibleTime(String topic, String group, int queueId, long queueOffset, long popTime, long nextVisibleTime);

    // 清除顺序消息的阻塞状态，用于重置消费位点
    // 目前是删除掉 queue 的锁
    void clearBlock(String topic, String group, int queueId);

}
