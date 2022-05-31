package com.ccc.transformation;

import com.ccc.source.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransMapTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Carl", "./home", 1000L),
                new Event("Johnson", "./cart", 2000L)
        );

        // 传入匿名内部类实现 MapFunction
        stream.map(new MapFunction<Event, String>() {
            @Override
            public String map(Event event) throws Exception {
                return event.user;
            }
        });

        // 传入 Lambda 表达式
        SingleOutputStreamOperator<String> result = stream.map((Event event) -> event.user);
        result.print();

        env.execute();
    }
}
