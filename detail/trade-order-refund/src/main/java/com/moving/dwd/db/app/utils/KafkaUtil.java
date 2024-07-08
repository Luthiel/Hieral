package com.moving.dwd.db.app.utils;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;

public class KafkaUtil {
    public static final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    private static ParameterTool parameterTool = null;

    public static DataStream<EventPO> read(ParameterTool parameter) {
        parameterTool = parameter;
        initEnv();
        return null;
    }

    private static void initEnv() {
        // environment 注册 Kafka 配置 Global
        env.getConfig().setGlobalJobParameters(parameterTool);
        // 简化上下文环境的配置 作业中的一些参数配置
        ParameterUtil.endWithConfig(env, parameterTool);
        kafkaSourceBuilder();
    }

    private static void kafkaSourceBuilder() {
        String brokers = parameterTool.get(ParameterConstantsUtil.KAFKA_BROKERS);
        String topic = parameterTool.get(ParameterConstantsUtil.KAFKA_TOPIC);
        String group = parameterTool.get(ParameterConstantsUtil.KAFKA_GROUP);

        /**
         * Flink-Kafka-Connector 是专门消费 Kafka 的连接器，
         *      - 基于 flink checkpoint 容错机制
         *      - 提供 flink 到 kafka 端到端精确一次语义（保障在系统运行中及时程序发生问题，数据不会丢失和重复）
         *      - flink 的 Exactly once 语义只适用于 flink 内部流转的数据
         */
        // Kafka source
        KafkaSource.<KafkaMessagePO>builder()
                .setBootstrapServers(brokers)
                .setTopics(topic)
                .setGroupId(group)
                .setStartingOffsets(OffsetsInitializer.earliest())
                // 反序列化器 需要自定义方法实现
                .setDeserializer(KafkaRecordDeserializationSchema.of(new SelfDefineDeserializationSchema())).build();
    }


}
