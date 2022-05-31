package com.ccc.source;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class UDParallelSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new ParallelSource()).setParallelism(2).print();

        env.execute();
    }
}
