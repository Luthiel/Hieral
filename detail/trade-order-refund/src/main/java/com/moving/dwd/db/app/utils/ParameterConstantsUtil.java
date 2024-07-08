package com.moving.dwd.db.app.utils;

public class ParameterConstantsUtil {
    public static final String KAFKA_BROKERS = "hadoop1:9092,hadoop2:9092,hadoop3:9092";
    public static final String KAFKA_TOPIC = "test";
    public static final String KAFKA_GROUP = "group";


    public static final String FLINK_ROOT_FILE = "/user/flink.properties";
    public static final String FLINK_ENV_FILE = "/user/env/flink_env.properties";
    public static final String FLINK_ENV_ACTIVE = "/user/env/flink_env_dev.properties";

    public static final String FLINK_CHECKPOINT_INTERVAL = "6000L";
    public static final String FLINK_CHECKPOINT_RETRY_TIMES = "5";
    public static final String FLINK_CHECKPOINT_NUMS = "1";
    public static final String FLINK_PARALLELISM = "4";
}
