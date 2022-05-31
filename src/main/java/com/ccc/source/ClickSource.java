package com.ccc.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

public class ClickSource implements SourceFunction<Event> {
    // 定义一个布尔变量，作为控制数据生成的表示为
    private boolean running = true;
    Random random = new Random(); // 在指定的数据集中随机选取数据

    @Override
    public void run(SourceContext<Event> sourceContext) throws Exception {
        String[] users = {"Carl", "Johnson", "CC", "CJ"};
        String[] urls = {"./home", "./cart", "./fav", "./prod?id=1", "./prod?id=2"};

        while (running) {
            sourceContext.collect(new Event(
                    users[random.nextInt(users.length)],
                    urls[random.nextInt(urls.length)],
                    Calendar.getInstance().getTimeInMillis()));
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
