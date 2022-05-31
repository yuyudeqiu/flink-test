package com.ccc.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.ArrayList;
import java.util.Properties;

public class SourceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        ArrayList<Event> clicks = new ArrayList<>();
        clicks.add(new Event("Marry", "./home", 1000L));
        clicks.add(new Event("Carl", "./cart", 2000L));

        // 1. 从集合中读取数据
        DataStreamSource<Event> streamFromCollection = env.fromCollection(clicks);
        streamFromCollection.print("From Collection: ");

        // 2. 从文件中读取数据
        DataStreamSource<String> streamFromFile = env.readTextFile("input/clicks.csv");
        streamFromFile.print("From File: ");

        // 3. 从 Socket 读取数据
        // DataStreamSource<String> streamFromSocket = env.socketTextStream("hadoop102", 7777);
        // streamFromSocket.print("From socket: ");

        // 4. 从 Kafka 读取数据
        Properties props = new Properties();
        props.setProperty("bootstrap.server", "hadoop102:9092");
        props.setProperty("group.id", "flinkTest");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("auto.offset.reset", "latest");

        DataStreamSource<String> streamFromKafka = env.addSource(
                new FlinkKafkaConsumer<String>("clicks", new SimpleStringSchema(), props)
        );
        streamFromKafka.print("From Kafka: ");



        env.execute();
    }
}
