package com.ccc.transformation;

import com.ccc.source.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransFilterTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Carl", "./home", 1000L),
                new Event("Johnson", "./cart", 2000L),
                new Event("Carl", "./cart", 3000L)
        );

        stream.filter(event -> {
            return event.user.contains("Carl");
        }).print("Filter: ");

        env.execute();
    }

}
