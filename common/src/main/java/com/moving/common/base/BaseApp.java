package com.moving.common.base;

import com.moving.common.util.FlinkSourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;

public abstract class BaseApp {
    public abstract void handle(StreamExecutionEnvironment env,
                                DataStreamSource<String> stream);

    public void start(int port, int parallelism, String ckAndGroupId, String topic) {
        // 1. 环境准备
        // 1.1 设置操作 Hadoop 的用户名为 Hadoop 超级用户 root
        System.setProperty("HADOOP_USER_NAME", "root");

        // 1.2 获取流处理环境，并指定本地测试时启动 WebUI 所绑定的端口
        Configuration conf = new Configuration();
        // 测试环境中 DIM 层分流使用 10001 端口，DWD 10011 开始自增，DWS 10021 自增
        conf.setInteger("rest.port", port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        // 1.3 设置并行度，项目中统设为 4
        env.setParallelism(parallelism);

        // 1.4 状态后端及检查点相关配置
        // 1.4.1 设置状态后端
        env.setStateBackend(new HashMapStateBackend());

        // 1.4.2 开启 checkpoint，默认是 barrier 对齐的
        env.enableCheckpointing(5000);
        // 其他检查点常用配置
        // 可以选择开启非对称检查点，大大减少产生反/背压时的检查点保存时间，但要求检查点模式必须为 精准一次，最大并发检查点数量为 1
        // env.getCheckpointConfig().enableUnalignedCheckpoints();
        // 填写 aligned 的超时时间，超出后采用 非 aligned 的方式保存 checkpoint
        env.getCheckpointConfig().setAlignedCheckpointTimeout(Duration.ofSeconds(1));

        // 1.4.3 设置 checkpoint 模式: 精准一次（还是优先） 而对于大多数低延迟的流处理程，at-least-once 处理效率更高（够用）
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 1.4.4 checkpoint 存储到 HDFS 中
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop1:8020/ecommerce/stream/" + ckAndGroupId);
        // 存储到 JobManager 堆内存中（一般不会存到内存中，而是选择高可用的文件系统）
        // env.getCheckpointConfig().setCheckpointStorage(new JobManagerCheckpointStorage());
        // 允许增量 checkpoint，checkpoint 最大并发必须为 1
        // env.enableChangelogStateBackend(true);

        // 1.4.5 checkpoint 并发数
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 1.4.6 checkpoint 之间的最小间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
        // 1.4.7 checkpoint  的超时时间
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        // 1.4.8 job 取消时 checkpoint 保留策略
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(RETAIN_ON_CANCELLATION);


        // 1.5 从 Kafka 目标主题读取数据，封装为流
        KafkaSource<String> source = FlinkSourceUtil.getKafkaSource(ckAndGroupId, topic);

        DataStreamSource<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka_source");

        // 2. 执行具体的处理逻辑
        handle(env, stream);

        // 3. 执行 Job
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
