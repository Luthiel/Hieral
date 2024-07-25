package com.moving.common.util;

import com.alibaba.fastjson.JSONObject;
import com.moving.common.bean.TableProcessDwd;
import com.moving.common.constant.Constant;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.SimpleStringSerializer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.Random;

public class FlinkSinkUtil {
  public static Sink<String> getKafkaSink(String topic) {
    return KafkaSink.<String>builder()
        .setBootstrapServers(Constant.KAFKA_BROKERS)
        .setRecordSerializer(
            KafkaRecordSerializationSchema.<String>builder()
                .setTopic(topic)
                .setValueSerializationSchema(new SimpleStringSchema())
                .build())
        .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
        .setTransactionalIdPrefix("luthiel-" + topic + new Random().nextLong())
        .setProperty("transaction.timeout.ms", 15 * 60 * 1000 + "")
        .build();
  }

  // 多态
  public static Sink<Tuple2<JSONObject, TableProcessDwd>> getKafkaSink() {
    return KafkaSink.<Tuple2<JSONObject, TableProcessDwd>>builder()
        .setBootstrapServers(Constant.KAFKA_BROKERS)
        .setRecordSerializer(
            new KafkaRecordSerializationSchema<Tuple2<JSONObject, TableProcessDwd>>() {
              @Nullable
              @Override
              public ProducerRecord<byte[], byte[]> serialize(
                  Tuple2<JSONObject, TableProcessDwd> dataWithConfig,
                  KafkaSinkContext context,
                  Long timestamp) {
                String topic = dataWithConfig.f1.getSinkTable();
                JSONObject data = dataWithConfig.f0;
                data.remove("op_type");
                return new ProducerRecord<>(
                    topic, data.toJSONString().getBytes(StandardCharsets.UTF_8));
              }
            })
        .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
        .setTransactionalIdPrefix("luthiel-" + new Random().nextLong())
        .setProperty("transaction.timeout.ms", 15 * 60 * 1000 + "")
        .build();
  }

  public static DorisSink<String> getDorisSink(String table, String labelPrefix) {
    Properties props = new Properties();
    props.setProperty("format", "json"); // 设置流输出格式为 json，默认是 csv
    props.setProperty("read_json_by_line", "true"); // 每行一条 json 数据
    // 注意要指定泛型
    return DorisSink.<String>builder()
        .setDorisReadOptions(DorisReadOptions.builder().build())
        .setDorisOptions(
            DorisOptions.builder()
                .setFenodes(Constant.DORIS_FE_NODES)
                .setTableIdentifier(table)
                .setUsername("root")
                .setPassword("doris2024")
                .build())
        .setDorisExecutionOptions(
            DorisExecutionOptions.builder()
                .setLabelPrefix(labelPrefix)
                .disable2PC() // 因为两阶段提交后，labelPrefix 需要全局唯一，我们在测试过程中可能反复测试，为了方便暂时关闭
                .setBufferCount(3) // 批次条数：默认 3
                .setBufferSize(1024 * 1024) // 批次大小：默认 1mb
                .setCheckInterval(3000) // 批次输出间隔
                .setMaxRetries(3)
                .setStreamLoadProp(props)
                .build())
        .setSerializer(new SimpleStringSerializer()) // 设置序列化器
        .build();
  }
}
