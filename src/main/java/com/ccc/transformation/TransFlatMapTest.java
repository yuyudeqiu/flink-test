package com.ccc.transformation;

import com.ccc.source.Event;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

public class TransFlatMapTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Carl", "./home", 1000L),
                new Event("Johnson", "./cart", 2000L),
                new Event("Carl", "./cart", 3000L)
        );


        SingleOutputStreamOperator<String> result = stream.flatMap(
                new FlatMapFunction<Event, String>() {
                    @Override
                    public void flatMap(Event event, Collector<String> collector) throws Exception {
                        collector.collect(event.user);
                        collector.collect(event.url);
                        collector.collect(new Timestamp(event.timestamp).toString());
                    }
                }
        );

        result.print();

        env.execute();
    }
}
