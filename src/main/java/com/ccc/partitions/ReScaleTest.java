package com.ccc.partitions;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class ReScaleTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 这里使用了并行数据源的富函数版本
        // 这样可以调用 getRuntimeContext 方法来查看运行时上下文的一些信息
        env.
                addSource(new RichParallelSourceFunction<Integer>() {
                    @Override
                    public void run(SourceContext<Integer> sourceContext) throws Exception {
                        for (int i = 1; i <= 8; i++) {
                            if (i % 2 == getRuntimeContext().getIndexOfThisSubtask()) {
                                sourceContext.collect(i);
                            }
                        }
                    }

                    @Override
                    public void cancel() {

                    }
                })
                .setParallelism(2)
                .rescale()
                .print()
                .setParallelism(4);

        env.execute();
    }
}
