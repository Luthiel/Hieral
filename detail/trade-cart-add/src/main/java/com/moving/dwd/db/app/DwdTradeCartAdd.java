package com.moving.dwd.db.app;

import com.moving.common.base.BaseSQLApp;
import com.moving.common.constant.Constant;
import com.moving.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdTradeCartAdd extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeCartAdd().start(10013, 4, Constant.TOPIC_DWD_TRADE_CART_ADD);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        // 读取 topic_db 的数据
        readOdsDb(tEnv, Constant.TOPIC_DWD_TRADE_CART_ADD);
        // 过滤出加购数据
        Table cartAdd = tEnv.sqlQuery(
                "SELECT " +
                        " data['id'] id, " +
                        " data['user_id'] user_id, " +
                        " data['sku_id'] sku_id, " +
                        " IF(type = 'insert', CAST(data['sku_num'] AS INT), CAST(old['sku_num'] AS INT)) sku_num, " +
                        " ts, " +
                        " FROM topic_db " +
                        " WHERE database = 'gmall' AND table = 'cart_info' AND (type = 'insert'" +
                        " OR (type = 'update' AND old['sku_num'] IS NOT NULL" +
                        // 数量增加才是加购，加购操作可能的两种方式分别是 新增商品插入（insert） 和 已有商品数量增加（update）
                        " AND CAST(data['sku_num'] AS INT) > CAST(old['sku_num'] AS INT)))"
        );

        // 写出到 kafka
        tEnv.executeSql(
                "CREATE TABLE dwd_trade_cart_add (" +
                        " id STRING, " +
                        " user_id STRING, " +
                        " sku_id STRING, " +
                        " sku_num INT, " +
                        " ts BIGINT )" +
                        SQLUtil.getKafkaDDLSink(Constant.TOPIC_DWD_TRADE_CART_ADD)
        );

        cartAdd.executeInsert(Constant.TOPIC_DWD_TRADE_CART_ADD);
    }
}
