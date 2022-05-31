package com.ccc.source;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.Random;

/**
 * 如果想要自定义并行的数据源的话，需要使用 ParallelSourceFunction
 */
public class ParallelSource implements ParallelSourceFunction<Integer> {
    private boolean running = true;
    private Random random = new Random();

    @Override
    public void run(SourceContext<Integer> sourceContext) throws Exception {
        while (running) {
            sourceContext.collect(random.nextInt());
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
