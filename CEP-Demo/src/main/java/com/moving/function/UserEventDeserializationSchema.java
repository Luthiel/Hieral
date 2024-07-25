package com.moving.function;

import com.moving.pojo.UserEvent;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class UserEventDeserializationSchema implements KafkaDeserializationSchema<UserEvent> {
    private static  final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public boolean isEndOfStream(UserEvent userEvent) {
        return false;
    }

    @Override
    public UserEvent deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
        JsonNode jsonNode = objectMapper.readTree(String.valueOf(record));
        String userId = jsonNode.get("userId").asText();
        String eventType = jsonNode.get("eventType").asText();

        return new UserEvent(userId,eventType);
    }


    @Override
    public TypeInformation<UserEvent> getProducedType() {
        return TypeInformation.of(UserEvent.class);
    }
}