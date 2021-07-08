package com.aurora;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flume.Event;
import java.io.Serializable;

/**
 * A function that can create a Event from an incoming instance of the given type.
 * @param <IN>
 */
public interface FlumeEventBuilder<IN> extends Function, Serializable {
    Event createFlumeEvent(IN value, RuntimeContext ctx);
}