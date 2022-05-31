package com.ccc.transformation;

import com.ccc.source.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransKeyByTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Carl", "./home", 1000L),
                new Event("Johnson", "./cart", 1100L),
                new Event("Carl", "./cart", 1200L),
                new Event("Johnson", "./home", 2000L)
        );

        stream.keyBy(event->event.user).print();

        env.execute();
    }
}
