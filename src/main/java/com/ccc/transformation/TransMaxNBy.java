package com.ccc.transformation;

import com.ccc.source.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransMaxNBy {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Carl", "./home", 1000L),
                new Event("Johnson", "./cart", 2000L),
                new Event("Johnson", "./home", 3000L),
                new Event("Carl", "./home", 3100L),
                new Event("Carl", "./home", 3000L)
        );

        SingleOutputStreamOperator<Event> max = stream.keyBy(event -> event.user).max("timestamp");
        max.print("max: ");

        // maxBy : 与 max()类似，在输入流上针对指定字段求最大值。不同的是，max()只计
        // 算指定字段的最大值，其他字段会保留最初第一个数据的值；
        // 而 minBy()则会返回包含字段最大值的整条数据。
        SingleOutputStreamOperator<Event> maxBy = stream.keyBy(event -> event.user).maxBy("timestamp");
        maxBy.print("maxBy: ");

        env.execute();
    }
}
