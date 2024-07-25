package com.moving.utils;

public class ParameterConstantsUtil {
    public static final String KAFKA_BROKERS = "kafka.brokers";
    public static final String KAFKA_TOPIC = "kafka.topic";
    public static final String KAFKA_GROUP = "kafka.group";


    public static final String FLINK_ROOT_FILE = "flink.properties";
    public static final String FLINK_ENV_FILE = "flink_%s.properties";
    public static final String FLINK_ENV_ACTIVE = "flink.env.active";

    // 出发 checkpoint 的时间间隔
    public static final String FLINK_CHECKPOINT_INTERVAL = "flink.checkpoint.interval";
    // checkpoint 超时
    public static final String FLINK_CHECKPOINT_TIMEOUT = "flink.checkpoint.timeout";
    // checkpoint 允许失败次数
    public static final String FLINK_CHECKPOINT_FAILURE_ALLOWED = "flink.checkpoint.failureNumber";
    // checkpoint 最大并发数量
    public static final String FLINK_CHECKPOINT_MAX_CONCURRENT = "flink.checkpoint.maxConcurrent";
    public static final String FLINK_CHECKPOINT_RETRY_TIMES = "flink.checkpoint.retry.times";
    // 并行度
    public static final String FLINK_PARALLELISM = "flink.parallelism";
    // 数据延迟的最大时间
    public static final String FLINK_MAX_OUTOFORDERNESS = "flink.maxOutOfOrderness";
}
