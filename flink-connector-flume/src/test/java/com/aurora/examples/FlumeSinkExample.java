package com.aurora.examples;

import com.aurora.FlumeEventBuilder;
import com.aurora.FlumeSink;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;

import java.nio.charset.Charset;
/**
 * @author lj.michale
 * @description An example FlumeSink that sends data to Flume service.
 * @date 2021-07-08
 */
public class FlumeSinkExample {

    private static String clientType = "thrift";
    private static String hostname = "localhost";
    private static int port = 9000;

    public static void main(String[] args) throws Exception {

        //FlumeSink send data
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        FlumeEventBuilder<String> flumeEventBuilder = new FlumeEventBuilder<String>() {
            @Override
            public Event createFlumeEvent(String value, RuntimeContext ctx) {
                return EventBuilder.withBody(value, Charset.defaultCharset());
            }
        };

        FlumeSink<String> flumeSink = new FlumeSink<>(clientType, hostname, port, flumeEventBuilder, 1, 1, 1);

        // Note: parallelisms and FlumeSink batchSize
        // if every parallelism not enough batchSize, this parallelism not word FlumeThriftService output
        DataStreamSink<String> dataStream = env.fromElements("one", "two", "three", "four", "five")
                .addSink(flumeSink);

        env.execute();
    }
}