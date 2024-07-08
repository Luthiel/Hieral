package com.moving.common.util;

import com.moving.common.constant.Constant;

public class SQLUtil {
    public static String getKafkaDDLSource(String groupId, String topic) {
        return "with (" +
                " 'connector' = 'kafka'," +
                " 'properties.group.id' = '" + groupId + "'," +
                " 'topic' = '" + topic + "'," +
                " 'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "'," +
                " 'scan.startup.mode' = 'latest-offset'," +
                " 'json.ignore-parse-errors' = 'true'," +
                // 当 json 解析失败时忽略该数据
                " 'format' = 'json'" + ")";
    }

    public static  String getKafkaDDLSink(String topic) {
        return "with (" +
                " 'connector' = 'kafka'," +
                " 'topic' = '" + topic + "'," +
                " 'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "'," +
                " 'format' = 'json' " + ")";
    }

    public static String getUpsertKakfaDDL(String topic) {
        return "with (" +
                " 'connector' = 'upsert-kafka', " +
                " 'topic' = '" + topic + "', " +
                " 'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "'," +
                " 'key.json.ignore-parse-errors' = 'true', " +
                " 'key.format' = 'json' " + ")";
    }
}
