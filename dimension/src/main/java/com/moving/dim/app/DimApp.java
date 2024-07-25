package com.moving.dim.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.moving.common.base.BaseApp;
import com.moving.common.bean.TableProcessDim;
import com.moving.common.constant.Constant;
import com.moving.common.util.HBaseUtil;
import com.moving.common.util.JdbcUtil;
import com.moving.dim.function.HBaseSinkFunction;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
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
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;
import java.util.*;

@Slf4j
public class DimApp extends BaseApp {
    public static void main(String[] args) {
        // 创建 Application，start 最后会调用 handle 进行实际的逻辑实现
        new DimApp().start(10001, 4, "dim_app", Constant.TOPIC_DB);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 清洗 Kafka 消费数据
        SingleOutputStreamOperator<JSONObject> etlStream = etl(stream);
        SingleOutputStreamOperator<TableProcessDim> configStream = readTableProcess(env);
        // 根据配置表的数据, 在 HBase 中建表
        configStream = createHBaseTable(configStream);
        // 主流 connect 配置流，并对关联后的流做处理
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimDataToTpStream = connect(etlStream, configStream);
        // 删除不需要的字段
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> resultStream = deleteNotNeedColumns(dimDataToTpStream);
        // 写出到 HBase 目标表
        writeToHBase(resultStream);
    }

    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        return stream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) {
                try {
                    JSONObject jsonObj = JSON.parseObject(value);
                    String db = jsonObj.getString("database");
                    String type = jsonObj.getString("type");
                    String data = jsonObj.getString("data");

                    // 过滤空值数据 -> 数据库匹配（ecommerce） -> 数据不为空
                    return "ecommerce".equals(db)
                            && ("insert".equals(type)
                            || "update".equals(type)
                            || "delete".equals(type)
                            || "bootstrap-insert".equals(type))
                            && data != null
                            && data.length() > 2;
                } catch (Exception e) {
                    log.warn("非正确的 json 格式数据：" + value);
                    return false;
                }
            }
        }).map(JSON::parseObject); // 处理为 Json 对象
    }

    /**
     * FlinkCDC
     * @param env
     * @return
     */
    private SingleOutputStreamOperator<TableProcessDim> readTableProcess(StreamExecutionEnvironment env) {
        // useSSL=false
        Properties props = new Properties();
        props.setProperty("useSSL", "false");
        props.setProperty("allowPublicKeyRetrieval", "true");
        // JDBC Configuration
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(Constant.MYSQL_HOST)
                .port(Constant.MYSQL_PORT)
                .databaseList("ecommerce_config") // set captured database, If you need to synchronize the whole database, Please set tableList to ".*".
                .tableList("ecommerce_config.table_process_dim") // set captured table
                .username(Constant.MYSQL_USER_NAME)
                .password(Constant.MYSQL_PASSWORD)
                .jdbcProperties(props)
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .startupOptions(StartupOptions.initial()) // 默认值: initial  第一次启动读取所有数据(快照), 然后通过 binlog 实时监控变化数据
                .build();

        return env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "cdc-source")
                .setParallelism(1) // 并行度设置为 1，否则配置信息变更时会出现乱序
                .map(new MapFunction<String, TableProcessDim>() {
                    @Override
                    public TableProcessDim map(String value) throws Exception {
                        JSONObject obj = JSON.parseObject(value); // 流中的每条数据是一个 Json
                        String op = obj.getString("op"); // 配置表操作类型
                        TableProcessDim tableProcessDim;
                        if ("d".equals(op)) {
                            tableProcessDim = obj.getObject("before", TableProcessDim.class);
                        } else {
                            tableProcessDim = obj.getObject("after", TableProcessDim.class);
                        }
                        tableProcessDim.setOp(op);

                        return tableProcessDim;
                    }
                })
                .setParallelism(1);
    }

    private SingleOutputStreamOperator<TableProcessDim> createHBaseTable (SingleOutputStreamOperator<TableProcessDim> tpStream) {
        return tpStream.map(new RichMapFunction<TableProcessDim, TableProcessDim>() {
                    private Connection hbaseConn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 1. 获取到 HBase 的连接
                        hbaseConn = HBaseUtil.getHBaseConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        // 2. 关闭连接
                        HBaseUtil.closeHBaseConn(hbaseConn);
                    }

                    @Override
                    public TableProcessDim map(TableProcessDim tableProcessDim) throws Exception {
                        String op = tableProcessDim.getOp();
                        if ("d".equals(op)) {
                            dropTable(tableProcessDim);
                        } else if ("r".equals(op) || "c".equals(op)) {
                            createTable(tableProcessDim);
                        } else { // 应该先删除表,再建表. 表的历史数据需要重新同步
                            dropTable(tableProcessDim);
                            createTable(tableProcessDim);
                        }
                        return tableProcessDim;
                    }

                    private void createTable(TableProcessDim tableProcessDim) throws IOException {
                        // namespace
                        HBaseUtil.createHBaseTable(hbaseConn,
                                Constant.HBASE_NAMESPACE,
                                tableProcessDim.getSinkTable(),
                                tableProcessDim.getSinkFamily());
                    }

                    private void dropTable(TableProcessDim tableProcessDim) throws IOException {
                        HBaseUtil.dropHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE, tableProcessDim.getSinkTable());
                    }
                })
                .setParallelism(1);
    }

    /**
     * 广播配置流，主流关联广播流，对关联后的流中数据进行处理
     * 对配置表信息进行预加载，避免主流先于配置流到达，导致无法匹配相应状态而丢失数据
     * @param dataStream
     * @param configStream
     * @return
     */
    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> connect(
            SingleOutputStreamOperator<JSONObject> dataStream,
            SingleOutputStreamOperator<TableProcessDim> configStream) {

        // 1. 把配置流做成广播流
        // key: 表名   user_info
        // value: TableProcess
        MapStateDescriptor<String, TableProcessDim> mapStateDescriptor = new MapStateDescriptor<String, TableProcessDim>("table_process_dim", String.class, TableProcessDim.class);
        BroadcastStream<TableProcessDim> broadcastStream = configStream.broadcast(mapStateDescriptor);
        // 2. 数据流去 connect 广播流
        return dataStream
                .connect(broadcastStream)
                .process(new BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>() {
                    private HashMap<String, TableProcessDim> map;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // open 中没有办法访问状态!!!
                        map = new HashMap<>();
                        // 1. 去 mysql 中查询 table_process 表所有数据
                        java.sql.Connection mysqlConn = JdbcUtil.getMysqlConnection();
                        List<TableProcessDim> tableProcessDimList = JdbcUtil.queryList(mysqlConn,
                                "select * from ecommerce_config.table_process_dim",
                                TableProcessDim.class,
                                true
                        );

                        for (TableProcessDim tableProcessDim : tableProcessDimList) {
                            String key = tableProcessDim.getSourceTable();
                            map.put(key, tableProcessDim);
                        }
                        JdbcUtil.closeConnection(mysqlConn);
                    }

                    // 2. 处理广播流中的数据: 把配置信息存入到广播状态中
                    @Override
                    public void processBroadcastElement(TableProcessDim tableProcessDim,
                                                        Context context,
                                                        Collector<Tuple2<JSONObject, TableProcessDim>> out) throws Exception {
                        BroadcastState<String, TableProcessDim> state = context.getBroadcastState(mapStateDescriptor);
                        String key = tableProcessDim.getSourceTable();

                        if ("d".equals(tableProcessDim.getOp())) {
                            // 删除状态
                            state.remove(key);
                            // map中的配置也要删除
                            map.remove(key);
                        } else {
                            // 更新或者添加状态
                            state.put(key, tableProcessDim);
                        }
                    }

                    // 3. 处理数据流中的数据: 从广播状态中读取配置信息
                    @Override
                    public void processElement(JSONObject jsonObj,
                                               ReadOnlyContext context,
                                               Collector<Tuple2<JSONObject, TableProcessDim>> out) throws Exception {
                        ReadOnlyBroadcastState<String, TableProcessDim> state = context.getBroadcastState(mapStateDescriptor);
                        String key = jsonObj.getString("table");
                        TableProcessDim tableProcessDim = state.get(key);

                        if (tableProcessDim == null) {  // 如果状态中没有查到, 则去 map 中查找（processBroadcastElement 预加载进 map 中了）
                            tableProcessDim = map.get(key);
                            if (tableProcessDim != null) {
                                log.info("在 map 中查找到 " + key);
                            }
                        } else {
                            log.info("在状态中查找到 " + key);
                        }
                        if (tableProcessDim != null) { // 这条数据找到了对应的配置信息
                            JSONObject data = jsonObj.getJSONObject("data");
                            data.put("op_type", jsonObj.getString("type"));  // 后期需要
                            out.collect(Tuple2.of(data, tableProcessDim));
                        }
                    }
                });
    }

    /**
     * 删除不需要的字段
     * @param dimDataToTpStream
     * @return
     */
    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> deleteNotNeedColumns(
            SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimDataToTpStream) {
        return dimDataToTpStream
                .map(new MapFunction<Tuple2<JSONObject, TableProcessDim>, Tuple2<JSONObject, TableProcessDim>>() {
                    @Override
                    public Tuple2<JSONObject, TableProcessDim> map(Tuple2<JSONObject, TableProcessDim> dataWithConfig) {
                        JSONObject data = dataWithConfig.f0;
                        List<String> columns = new ArrayList<>(Arrays.asList(dataWithConfig.f1.getSinkColumns().split(",")));
                        columns.add("op_type");

                        data.keySet().removeIf(key -> !columns.contains(key));
                        return dataWithConfig;
                    }
                });
    }

    /**
     * 写入 HBase
     * @param resultStream
     */
    private void writeToHBase(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> resultStream) {
    /*  为什么要自定义 sink
        1. 没有专门的 HBase 连接器
        2. sql 有专门的 HBase 连接器, 由于一次只能写到一个表中, 所以也不能把流转成表再写
     */
        resultStream.addSink(new HBaseSinkFunction());
    }

}
