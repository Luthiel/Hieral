package com.moving.function;


import com.moving.pojo.KafkaMessagePO;
import com.moving.utils.JsonUtil;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;


import java.io.IOException;

public class SelfDefineDeserializationSchema implements KafkaDeserializationSchema<KafkaMessagePO> {
    /*
     * Kafka 存放的是二进制数据
     * 读数据时，需要将该数据进行反序列化，即.setDeserializer(new SelfDefineDeserializationSchema)
     * 自定义反序列化器（实现 DeserializationSchema 接口）
     *
     * flink 本身实现的 new SimpleStringSchema() 同样是实现 DeserializationSchema 接口
     * 但该实现知识将 Kafka 反序列化的数据转换成了 DataStream<String>
     */
    @Override
    public KafkaMessagePO deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws IOException {
        KafkaMessagePO kafkaMessagePO = null;
        if (consumerRecord != null) {
            String value = new String(consumerRecord.value(), "UTF-8");
            // 抽取公共逻辑封装一下
            // kafkaMessagePO = JSONObject.parseObject(value, KafkaMessagePO.class);
            kafkaMessagePO = JsonUtil.strToJsonObject(value, KafkaMessagePO.class);
        }
        return kafkaMessagePO;
    }

    // 判断是否到达流的最后
    @Override
    public boolean isEndOfStream(KafkaMessagePO kafkaMessagePO) {
        return false;
    }

    // 指定反序列化后的数据类型
    @Override
    public TypeInformation<KafkaMessagePO> getProducedType() {
        return TypeInformation.of(KafkaMessagePO.class);
    }
}
