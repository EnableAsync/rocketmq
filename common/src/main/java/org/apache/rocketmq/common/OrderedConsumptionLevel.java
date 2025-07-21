package org.apache.rocketmq.common;

public enum OrderedConsumptionLevel {
    QUEUE(0),
    SHARDING_KEY(1);

    private final int value;

    OrderedConsumptionLevel(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static OrderedConsumptionLevel valueOf(int value) {
        if (value == 1) {
            return SHARDING_KEY;
        }
        return QUEUE;
    }
}
