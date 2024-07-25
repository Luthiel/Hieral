package com.moving.utils;

import com.moving.function.SelfDefineDeserializationSchema;
import com.moving.pojo.EventPO;
import com.moving.pojo.KafkaMessagePO;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KafkaUtil {
    public static final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    public  static KafkaSource<KafkaMessagePO> KAFKA_SOURCE= null;
    private static ParameterTool parameterTool = null;

    public static DataStream<EventPO> read(ParameterTool parameter) {
        parameterTool = parameter;
        initEnv();
        kafkaSourceBuilder();
        return resultEventStream();
    }

    // Kafka 二进制数据流 -> String -> JsonObject -> KafkaMessagePO -> EventPO
    private static DataStream<EventPO> resultEventStream() {
        // Kafka 中读取的数据无法直接计算，需要进行清洗过滤、转换、封装等一系列操作
        // 处理后的数据才能服务于后面的开窗、聚合、关联
        // 如果是 Kafka 中的日志数据，会有大量的字段，如果不做预处理，那么在后续开窗计算中需要存储的状态就会非常大
        // 所以我们需要对 Kafka 数据进行列裁剪，即只过滤出我们需要的数据，防止后续计算资源浪费
        return env.fromSource(KAFKA_SOURCE,
                WatermarkStrategy.noWatermarks(),
                "kafka source")
                .map(new MapFunction<KafkaMessagePO, EventPO>() {
                    @Override
                    public EventPO map(KafkaMessagePO kafkaMessagePO) throws Exception {
                        return new EventPO(
                                kafkaMessagePO.getUser_id_int(),
                                kafkaMessagePO.getEvent_time(),
                                kafkaMessagePO.getEvent_target_name(),
                                kafkaMessagePO.getEvent_name(),
                                kafkaMessagePO.getEvent_type(),
                                kafkaMessagePO.getEvent_context()
                        );
                    }
                });
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
        KAFKA_SOURCE = KafkaSource.<KafkaMessagePO>builder()
                .setBootstrapServers(brokers)
                .setTopics(topic)
                .setGroupId(group)
                .setStartingOffsets(OffsetsInitializer.earliest())
                // 反序列化器 需要自定义方法实现
                .setDeserializer(KafkaRecordDeserializationSchema.of(new SelfDefineDeserializationSchema())).build();
    }


}
