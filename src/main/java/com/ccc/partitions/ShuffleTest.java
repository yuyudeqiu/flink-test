package com.ccc.partitions;

import com.ccc.source.ClickSource;
import com.ccc.source.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ShuffleTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.addSource(new ClickSource());
        stream.shuffle().print("shuffle").setParallelism(4);

        env.execute();
    }
}
