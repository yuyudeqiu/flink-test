package com.ccc.partitions;

import com.ccc.source.ClickSource;
import com.ccc.source.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class BroadcastTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.addSource(new ClickSource());

        // 经广播后打印输出，并行度为 4
        stream.broadcast().print("broadcast").setParallelism(4);

        env.execute();
    }
}
