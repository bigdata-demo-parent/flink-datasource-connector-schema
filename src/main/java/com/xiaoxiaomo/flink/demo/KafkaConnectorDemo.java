package com.xiaoxiaomo.flink.demo;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KafkaConnectorDemo {

    static String brokers = "192.168.13.36:9092,192.168.13.36:9092,192.168.13.36:9092";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // KafkaSource
        KafkaSource<User> source = KafkaSource.<User>builder()
                .setBootstrapServers(brokers)
                .setTopics("test")
                .setGroupId("test-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new UserSchema())
                .build();

        DataStreamSource<User> dataStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // 分割成单词，统计数量
        dataStream
                .map(new FunctionHandler())
                .keyBy((KeySelector<Tuple2<String, Integer>, Object>) value -> Tuple1.of(value.f0))
                .sum(1)
                .print();

        env.execute("Connector DataSource demo : kafka");
    }

    public static class FunctionHandler implements MapFunction<User, Tuple2<String, Integer>> {

        @Override
        public Tuple2<String, Integer> map(User obj) throws Exception {
            return new Tuple2<>(obj.getName(), 1);
        }
    }
}
