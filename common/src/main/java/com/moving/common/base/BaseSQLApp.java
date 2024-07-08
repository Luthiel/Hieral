package com.moving.common.base;

import com.moving.common.constant.Constant;
import com.moving.common.util.SQLUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;


public abstract class BaseSQLApp {
    public abstract void handle(StreamExecutionEnvironment env,
                                StreamTableEnvironment tEnv);

    public void start(int port, int parallelism, String ck) {
        System.setProperty("HADOOP_USER_NAME", "root");

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(parallelism);
        // 设置状态后端
        env.setStateBackend(new HashMapStateBackend());

        // 开启 checkpoint
        env.enableCheckpointing(5000);
        // 设置 checkpoint 模式：精准一次
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // checkpoint 存储
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop1:8020/flink/sql" + ck);
        // checkpoint 并发数
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // checkpoint 之间的最小间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        // checkpoint 的超时时间
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        // job 取消时 checkpoint 的保留策略
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(RETAIN_ON_CANCELLATION);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        handle(env, tEnv);
    }

    // 读取 ods_db 的数据
    public void readOdsDb(StreamTableEnvironment tEnv, String groupId) {
        tEnv.executeSql("create table topic_db (" +
                " `database` string, " +
                " `table` string, " +
                " `type` string, " +
                " `data` map<string, string>, " +
                " `old` map<string, string>, " +
                " `ts` bigint, " +
                " `pt` as proctime(), " +
                " `et` as to_timestamp_ltz(ts, 0), " +
                " `watermark` for `et` as `et` - interval '3' second " + ")" +
                SQLUtil.getKafkaDDLSource(groupId, Constant.TOPIC_DB));
    }

    // 读取 字典表
    public void readBaseDic(StreamTableEnvironment tEnv) {
        tEnv.executeSql("create table base_dic (" +
                // 若字段是原子类型，则表示该字段是 rowKey, 字段随意，其类型随意
                " dic_code string, " +
                // 字段名和 hbase 中的列族名保持一致，类型必须是 row，嵌套在内部的即为列
                " info row<dic_name string>, " +
                // 只能使用 rowKey 作主键
                " primary key (dic_code) not enforced " +
                ") with (" +
                " 'connector' = 'hbase-2.2', " +
                " 'table-name' = 'gmall:dim_base_dic', " +
                " 'zookeeper.quorum' = 'hadoop1:2181, hadoop2:2181, hadoop3:2181', " +
                " 'lookup.cache' = 'PARTIAL', " +
                " 'lookup.async' = 'true', " +
                " 'lookup.partial-cache.max-rows' = '20', " +
                " 'lookup.partial-cache.expire-after-access' = '2 hour' " + ")");
    }
}
