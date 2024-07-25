package com.moving.dws.app;

import com.moving.common.base.BaseSQLApp;
import com.moving.common.constant.Constant;
import com.moving.common.util.SQLUtil;
import com.moving.dws.function.KWSplit;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwsTrafficSourceKeywordPageViewWindow extends BaseSQLApp {
    public static void main(String[] args) {
        new DwsTrafficSourceKeywordPageViewWindow().start(10021, 4,
                "dws_traffic_source_keyword_page_view_window");
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        // 读取 页面日志
        tEnv.executeSql(
                "CREATE TABLE page_log (" +
                        " page map<string, string>, " +
                        " ts bigint, " +
                        " es AS to_timestamp_ltz(ts, 3), " +
                        " watermark for es as es - interval '5' second) " +
                        SQLUtil.getKafkaDDLSource("dws_traffic_source_keyword_page_view_window", Constant.TOPIC_DWD_TRAFFIC_PAGE)
        );

        // 读取搜索关键词
        Table kwTable = tEnv.sqlQuery(
                "SELECT \n" +
                        "page['item'] kw, \n" +
                        "et \n" +
                        "FROM page_log \n" +
                        "WHERE (page['last_page_id'] = 'search' OR page['last_page_id'] = 'home') " +
                        "AND page['item_type'] = 'keyword' " +
                        "AND page['item'] IS NOT NULL"
        );
        tEnv.createTemporaryView("kw_table", kwTable);

        // 自定义分词函数
        tEnv.createTemporaryFunction("kw_split", KWSplit.class);
        Table keywordTable = tEnv.sqlQuery(
                "SELECT \n" +
                        "keyword, \n" +
                        "et \n" +
                        "FROM kw_table " +
                        "JOIN LATERAL TABLE(split(kw_split(kw))) ON TRUE"
        );
        tEnv.createTemporaryView("keyword_table", keywordTable);

        // 开窗聚合
        Table result = tEnv.sqlQuery(
                "SELECT \n" +
                        "date_format(window_start, 'yyyy-MM-dd' HH:mm:ss) stt, \n" +
                        "date_format(window_end, 'yyyy-MM-dd' HH:mm:ss) edt, \n" +
                        "date_format(window_start, 'yyyy-MM-dd') cur_date, \n" +
                        "keyword, \n" +
                        "count(*) keyword_count \n" +
                        "FROM TABLE(TUMBLE(TABLE keyword_table, DESCRIPTOR(et), INTERVAL '5' SECOND)) \n" +
                        "GROUP BY window_start, window_end, cur_date, keyword"
        );

        // 写出到 Doris
        tEnv.executeSql(
                "CREATE TABLE dws_traffic_source_keyword_page_view_window (\n" +
                        "stt string, \n" +
                        "edt string, \n" +
                        "cur_date string, \n" +
                        "keyword string, \n" +
                        "keyword_count bigint) \n" +
                        "WITH (\n" +
                        "'connector' = 'doris', \n" +
                        "'fenodes' = '" + Constant.DORIS_FE_NODES + "', \n" +
                        "'table_identifier' = '" + Constant.DORIS_DATABASE + ".dws_traffic_source_keyword_page_view_window', \n" +
                        "'username' = 'root', \n" +
                        "'password' = 'doris2024', \n" +
                        "'sink.properties.format' = 'json', \n" +
                        "'sink.buffer-count' = '4', \n" +
                        "'sink.buffer-size' = '4096', \n" +
                        "'sink.enable-2pc' = 'false', \n" + // 测试阶段关闭两阶段提交，便于测试
                        "'sink.properties.read_json_by_line' = 'true')"
        );

        result.executeInsert("dws_traffic_source_keyword_page_view_window");
    }
}
