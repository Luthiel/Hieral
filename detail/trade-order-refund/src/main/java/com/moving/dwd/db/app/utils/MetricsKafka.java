package com.moving.dwd.db.app.utils;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MetricsKafka {
    public static void main(String[] args) {
        // 消费 Kafka
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ParameterTool parameters = ParameterUtil.getParameters(args);

        DataStream<EventPO> eventStream = KafkaUtil.read(parameters);
    }
}
