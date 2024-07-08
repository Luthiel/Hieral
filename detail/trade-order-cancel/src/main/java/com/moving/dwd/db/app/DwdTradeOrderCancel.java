package com.moving.dwd.db.app;

import com.moving.common.base.BaseSQLApp;
import com.moving.common.constant.Constant;
import com.moving.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class DwdTradeOrderCancel extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeOrderCancel().start(10015, 4, Constant.TOPIC_DWD_TRADE_ORDER_CANCEL);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        // 设置 ttl
        tEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(15 * 60 + 5));

        // 读取数据 topic_db，创建 kafka 主题
        readOdsDb(tEnv, Constant.TOPIC_DWD_TRADE_ORDER_CANCEL);
        // 读取 dwd 层 order 事务事实表数据
        tEnv.executeSql(
                "CREATE TABLE dwd_trade_order_detail(" +
                        "id string," +
                        "order_id string," +
                        "user_id string," +
                        "sku_id string," +
                        "sku_name string," +
                        "province_id string," +
                        "activity_id string," +
                        "activity_rule_id string," +
                        "coupon_id string," +
                        "date_id string," +
                        "create_time string," +
                        "sku_num string," +
                        "split_original_amount string," +
                        "split_activity_amount string," +
                        "split_coupon_amount string," +
                        "split_total_amount string," +
                        "ts bigint " +
                        ")" +
                        // 传入消费者组名，以及消费的主题
                        SQLUtil.getKafkaDDLSource(Constant.TOPIC_DWD_TRADE_ORDER_CANCEL,
                                Constant.TOPIC_DWD_TRADE_ORDER_DETAIL)
        );

        // 过滤取消订单的数据
        Table orderCancel = tEnv.sqlQuery(
                "SELECT " +
                        " data['id'] id, " +
                        " data['operate_time'] operate_time, " +
                        " ts " +
                        " FROM database = 'gmall' AND table = 'order_info' AND type = 'update' " +
                        " AND old['order_status'] = '1001' AND data['order_status'] = '1003'"
        );
        tEnv.createTemporaryView("orderCancel", orderCancel);

        // 订单取消表和订单明细表 join
        Table result = tEnv.sqlQuery(
                "SELECT " +
                        "od.id," +
                        "od.order_id," +
                        "oi.user_id," +
                        "od.sku_id," +
                        "od.sku_name," +
                        "oi.province_id," +
                        "od.activity_id," +
                        "od.activity_rule_id," +
                        "od.coupon_id," +
                        "date_format(oc.operate_time, 'yyyy-MM-dd') order_cancel_date_id," +
                        // 年月日
                        "oc.operate_time," +
                        "od.sku_num," +
                        "od.split_original_amount," +
                        "od.split_activity_amount," +
                        "od.split_coupon_amount," +
                        "od.split_total_amount," +
                        "oc.ts " +
                        "FROM dwd_trade_order_detail od " +
                        "JOIN order_cancel oc ON od.order_id = oc.id "
        );

        // 写出数据
        tEnv.executeSql(
                "CREATE TABLE dwd_trade_order_cancel(" +
                        " id string, " +
                        " order_id string, " +
                        " user_id string, " +
                        " sku_id string, " +
                        " sku_name string, " +
                        " province_id string, " +
                        " activity_id string, " +
                        " activity_rule_id string, " +
                        " coupon_id string, " +
                        " date_id string, " +
                        " cancel_time string, " +
                        " sku_num string, " +
                        " split_original_amount string," +
                        " split_activity_amount string," +
                        " split_coupon_amount string," +
                        " split_total_amount string," +
                        " ts bigint )" +
                        SQLUtil.getKafkaDDLSink(Constant.TOPIC_DWD_TRADE_ORDER_CANCEL)
        );

        result.executeInsert(Constant.TOPIC_DWD_TRADE_ORDER_CANCEL);
    }
}
