package com.aurora;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.junit.jupiter.api.Test;
import java.nio.charset.Charset;
import static org.apache.flink.test.util.TestUtils.tryExecute;

/**
 * @author lj.michale
 * @description
 * @date 2021-07-08
 */
public class FlumeSinkTest extends FlumeServerBaseTest {

    @Test
    public void testSink() throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        FlumeEventBuilder<String> flumeEventBuilder = new FlumeEventBuilder<String>() {
            @Override
            public Event createFlumeEvent(String value, RuntimeContext ctx) {
                return EventBuilder.withBody(value, Charset.forName("UTF-8"));
            }
        };

        FlumeSink<String> flumeSink = new FlumeSink<>("default", getHost(), getPort(), flumeEventBuilder, 1, 1, 1);
        environment.fromElements("string1", "string2").addSink(flumeSink);

        tryExecute(environment, "FlumeTest");
    }

}
