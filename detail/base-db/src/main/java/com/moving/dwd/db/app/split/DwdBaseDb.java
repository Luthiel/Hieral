package com.moving.dwd.db.app.split;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.moving.common.base.BaseApp;
import com.moving.common.bean.TableProcessDwd;
import com.moving.common.constant.Constant;
import com.moving.common.util.FlinkSinkUtil;
import com.moving.common.util.JdbcUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.util.*;

@Slf4j
public class DwdBaseDb extends BaseApp {
    public static void main(String[] args) {
        new DwdBaseDb().start(10019, 4, "dwd_base_db", Constant.TOPIC_DB);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 清洗消费的数据
        SingleOutputStreamOperator<JSONObject> etlStream = etl(stream);
        // 通过 Flink CDC 读取配置表的数据
        SingleOutputStreamOperator<TableProcessDwd> configStream = readTableProcess(env);
        // 数据流 connect 配置流
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> dataWithConfigStream = connect(etlStream, configStream);
        // 删除不需要的字段
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> resultStream = deleteNotNeedColumns(dataWithConfigStream);
        // 写出 Kafka
        writeToKafka(resultStream);
    }

    private void writeToKafka(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> resultStream) {
        resultStream.sinkTo(FlinkSinkUtil.getKafkaSink());
    }

    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> deleteNotNeedColumns(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> dataWithConfigStream) {
        return dataWithConfigStream
                .map(new MapFunction<Tuple2<JSONObject, TableProcessDwd>, Tuple2<JSONObject, TableProcessDwd>>() {
                    @Override
                    public Tuple2<JSONObject, TableProcessDwd> map(Tuple2<JSONObject, TableProcessDwd> dataWithConfig) throws Exception {
                        JSONObject data = dataWithConfig.f0;
                        // 需要的列在配置表中提前定义好
                        List<String> columns = new ArrayList<>(Arrays.asList(dataWithConfig.f1.getSinkColumns().split(",")));
                        data.keySet().removeIf(key -> !columns.contains(key));
                        return dataWithConfig;
                    }
                });
    }


    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> connect(SingleOutputStreamOperator<JSONObject> etlStream, SingleOutputStreamOperator<TableProcessDwd> configStream) {
        // 1. 将配置流转换为广播流
        // key: 表名<sourceTable>:类型<sourceType> -> e.g. user_info : ALL
        // value: TableProcess
        MapStateDescriptor<String, TableProcessDwd> mapStateDescriptor = new MapStateDescriptor<>("table_process_dwd", String.class, TableProcessDwd.class);
        BroadcastStream<TableProcessDwd> broadcastStream = configStream.broadcast(mapStateDescriptor);
        // 2. 数据流 connect 广播流
        return etlStream
                .connect(broadcastStream)
                .process(new BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>() {
                    private HashMap<String, TableProcessDwd> map;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // open 中没法访问状态
                        map = new HashMap<>();
                        // 1. 到 mysql 中查询 table_process 表中所有数据
                        Connection mysqlConn = JdbcUtil.getMysqlConnection();
                        // 将 sql 查询结果以列表的形式封装到 TableProcessDwd 对象中
                        List<TableProcessDwd> tableProcessDwdList = JdbcUtil.queryList(mysqlConn,
                                "SELECT * FROM gmall_config.table_process_dwd",
                                TableProcessDwd.class,
                                true);
                        for (TableProcessDwd tableProcessDwd : tableProcessDwdList) {
                            String key = getKey(tableProcessDwd.getSinkTable(), tableProcessDwd.getSourceType());
                            map.put(key, tableProcessDwd);
                        }

                        // 关闭 JDBC 连接
                        JdbcUtil.closeConnection(mysqlConn);
                    }

                    @Override
                    public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>.ReadOnlyContext readOnlyContext, Collector<Tuple2<JSONObject, TableProcessDwd>> collector) throws Exception {
                        ReadOnlyBroadcastState<String, TableProcessDwd> state = readOnlyContext.getBroadcastState(mapStateDescriptor);
                        String key = getKey(jsonObject.getString("table"), jsonObject.getString("type"));
                        TableProcessDwd tableProcessDwd = state.get(key);
                        if (tableProcessDwd == null) {
                            // 若在状态中没有查到，则转向 map 查找
                            tableProcessDwd = map.get(key);
                            if (tableProcessDwd != null) {
                                log.info("在 map 中查找到 " + key);
                            }
                        } else {
                            log.info("在 状态 中查找到 " + key);
                        }
                        if (tableProcessDwd != null) {
                            // 该数据查找到对应的配置信息
                            JSONObject data = jsonObject.getJSONObject("data");
                            collector.collect(Tuple2.of(data, tableProcessDwd));
                        }
                    }

                    // 处理广播流中的数据：将配置信息存入广播状态中
                    @Override
                    public void processBroadcastElement(TableProcessDwd tableProcessDwd, BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>.Context context, Collector<Tuple2<JSONObject, TableProcessDwd>> collector) throws Exception {
                        BroadcastState<String, TableProcessDwd> state = context.getBroadcastState(mapStateDescriptor);
                        String key = getKey(tableProcessDwd.getSourceTable(), tableProcessDwd.getSourceType());

                        if ("d".equals(tableProcessDwd.getOp())) {
                            // 删除状态
                            state.remove(key);
                            // map 中配置也要删除
                            map.remove(key);
                        } else {
                            // 更新或添加状态
                            state.put(key, tableProcessDwd);
                        }
                    }

                    private String getKey(String table, String type) {
                        return table + ":" + type;
                    }
                });
    }

    private SingleOutputStreamOperator<TableProcessDwd> readTableProcess(StreamExecutionEnvironment env) {
        Properties props = new Properties();
        props.setProperty("useSSL", "false");
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(Constant.MYSQL_HOST)
                .port(Constant.MYSQL_PORT)
                .databaseList("gamll_config")
                // 如果需要使用整个数据库的表，直接设置为 .*
                .tableList("gmall_config.table_process_dwd")
                .username(Constant.MYSQL_USER_NAME)
                .password(Constant.MYSQL_PASSWORD)
                .jdbcProperties(props)
                // 将 SourceRecord 转换为 Json String
                .deserializer(new JsonDebeziumDeserializationSchema())
                // 默认值: initial --> 第一次启动读取所有数据（快照），然后通过 binlog 实时监控变化数据（CDC）
                .startupOptions(StartupOptions.initial()).build();

        return env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "cdc-source")
                .setParallelism(1)
                .map(new MapFunction<String, TableProcessDwd>() {
                    @Override
                    public TableProcessDwd map(String value) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(value);
                        String op = jsonObject.getString("op");
                        TableProcessDwd tableProcessDwd;
                        if ("d".equals(op)) {
                            tableProcessDwd = jsonObject.getObject("before", TableProcessDwd.class);
                        } else {
                            tableProcessDwd = jsonObject.getObject("after", TableProcessDwd.class);
                        }
                        tableProcessDwd.setOp(op);
                        return tableProcessDwd;
                    }
                })
                .setParallelism(1);
    }

    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        return stream
                .filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String value) throws Exception {
                        try {
                            JSONObject jsonObject = JSON.parseObject(value);
                            String database = jsonObject.getString("database");
                            String type = jsonObject.getString("type");
                            String data = jsonObject.getString("data");

                            return "gmall".equals(database)
                                    && ("insert".equals(type) || "update".equals(type))
                                    && data != null
                                    && data.length() > 2;
                        } catch (Exception e) {
                            log.warn(" 不是正确的 json 格式数据：" + value);
                            return false;
                        }
                    }
                })
                .map(JSON::parseObject);
    }
}
