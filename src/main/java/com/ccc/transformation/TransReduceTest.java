package com.ccc.transformation;

import com.ccc.source.ClickSource;
import com.ccc.source.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransReduceTest {
    public static void main(String[] args) throws Exception {
        // 将数据流按照用户 id 进行分区，然后用一个 reduce 算子实现 sum 的功能，
        // 统计每个用户的访问频次；进而将所有用户统计结果分到一组，用另一个 reduce 算子实现 maxBy 的功能，
        // 记录所有用户中访问频次最高的那个
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> stream = env.addSource(new ClickSource());

        stream
                // 将 Event 数据类型转换成元组类型
                .map(new MapFunction<Event, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(Event event) throws Exception {
                        return Tuple2.of(event.user, 1L);
                    }
                })
                // 使用用户名进行分流分流
                .keyBy(data -> data.f0)
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> t1, Tuple2<String, Long> t2) throws Exception {
                        return Tuple2.of(t1.f0, t1.f1 + t2.f1);
                    }
                })
                // 为每一条数据分配到同一个 key，将聚合结果发送到一条流中去
                .keyBy(r -> true)
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> t1, Tuple2<String, Long> t2) throws Exception {
                        return t1.f1 > t2.f1 ? t1 : t2;
                    }
                })
                .print();

        env.execute();
    }
}
