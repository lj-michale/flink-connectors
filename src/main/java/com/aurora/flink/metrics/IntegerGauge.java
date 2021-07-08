package com.aurora.flink.metrics;

import org.apache.flink.metrics.Gauge;

/**
 * @author lj.michale
 * @description Metrics计数器
 * @date 2021-07-07
 */
public class IntegerGauge implements Gauge<Integer> {

    private Integer count;

    public IntegerGauge() {
    }

    public IntegerGauge(Integer count) {
        this.count = count;
    }

    @Override
    public Integer getValue() {
        return this.count;
    }
}