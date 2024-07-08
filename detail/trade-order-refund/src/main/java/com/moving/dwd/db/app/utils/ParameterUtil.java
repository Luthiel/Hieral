package com.moving.dwd.db.app.utils;

import io.lettuce.core.dynamic.annotation.Param;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.io.InputStream;

public class ParameterUtil {
    public static final String DEFAULT_CONFIG = ParameterConstantsUtil.FLINK_ROOT_FILE; // 默认的配置文件
    public static final String FLINK_ENV_FILE = ParameterConstantsUtil.FLINK_ENV_FILE; // 带环境变量的配置文件
    public static final String ENV_ACTIVE = ParameterConstantsUtil.FLINK_ENV_ACTIVE; // 环境变量

    public static ParameterTool getParameters(String[] args) {
        /** Java 读取资源文件的方法
         * 1. ParameterUtil.class.getResourceAsStream() 参数是一个路径，路径必须是 “/”，表明从 classpath 的根路径下开始读取
         * 2. ParameterUtil.class.getClassLoader().getResourceAsStream()，路径不必是 “/”
         */
        InputStream inputStream = ParameterUtil.class.getClassLoader().getResourceAsStream(DEFAULT_CONFIG);

        try {
            // 读取配置文件
            ParameterTool propertiesFile = ParameterTool.fromPropertiesFile(inputStream);
            // 读取配置文件中的参数
            String envActiveValue = getEnvActiveValue(propertiesFile);
            // 读取对应模式的配置文件 如 dev 开发模式
            ParameterTool.fromPropertiesFile(
                    // 从配置文件中获取配置，使用当前线程
                    Thread.currentThread().getContextClassLoader().getResourceAsStream(envActiveValue)
            ).mergeWith(ParameterTool.fromArgs(args)) // 从命令行传入的 args 获取变量
                    .mergeWith(ParameterTool.fromSystemProperties()); // 从系统配置中获取变量
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static String getEnvActiveValue(ParameterTool propertiesFile) {
        String envActive = null;
        // 在配置文件中也是按照 KV 形式定义参数和参数值的
        if (propertiesFile.has(ENV_ACTIVE)) {
            // 因为配置文件最常见的有三种模式：test, prod, dev --> 因此从 propertiesFile 中取出定义的模式放入配置文件名中，用于选择对应配置文件
            envActive = String.format(FLINK_ENV_FILE, propertiesFile.get(ENV_ACTIVE));
        }
        return envActive;
    }

    public static void endWithConfig(StreamExecutionEnvironment env, ParameterTool parameterTool) {
        // 单位：ms --> 多长时间执行一次 checkpoint
        env.enableCheckpointing(parameterTool.getInt(ParameterConstantsUtil.FLINK_CHECKPOINT_INTERVAL));

        // 限定 checkpoint 完成的时长，唱出时间则放弃此次 checkpoint
        env.getCheckpointConfig().setCheckpointTimeout(parameterTool.getInt(ParameterConstantsUtil.FLINK_CHECKPOINT_INTERVAL));
        // 设置 精确一次 语义
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 允许失败的次数
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(parameterTool.getInt(ParameterConstantsUtil.FLINK_CHECKPOINT_RETRY_TIMES));
        // 同一时间允许存在的最大 checkpoint 数量 --> 设置过多对内存消耗较大
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(parameterTool.getInt(ParameterConstantsUtil.FLINK_CHECKPOINT_NUMS));

        // 设置算子并行度，如有三台机器，每台有 3 个 taskSlot，共计 9 个 slot，不同算子之间共享 slot，那么最大并行度为 9
        // 设置的范围则为 1~9
        env.setParallelism(parameterTool.getInt(ParameterConstantsUtil.FLINK_PARALLELISM));

        // 设置状态后端
        /**
         * 设置流计算事件类型的方法 env.setStreamTimeCharateristic()，Flink 1.12 之后弃用
         * Flink 默认的时间语义是事件时间
         *
         * 配置 checkpoint 之后，带状态的算子，器算子状态会被保存到状态后端里
         * 三种 flink 支持的状态后端：
         *      - MemoryStateBackend 内存状态后端    存储在 TaskManager 的内存中，会把 checkpoint 状态存储在 JobManager 内存中
         *      - FsStateBackend 文件系统状态后端     存储在 TaskManager 的内存中，会把 checkpoint 状态存储在文件系统中，一般是分布式文件系统 HDFS
         *      - RocksDBStateBackend 自带数据库转台后端     存储在 RocksDB 中，会把 checkpoint 状态存储在文件系统中
         */
        env.setStateBackend(new HashMapStateBackend());

        // 读取配置文件，将 flink 的参数配置为全局
        env.getConfig().setGlobalJobParameters(parameterTool);
    }
}
