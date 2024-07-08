package com.moving.dwd.db.app;

import com.moving.common.base.BaseSQLApp;
import com.moving.common.constant.Constant;
import com.moving.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdTradeOrderPaymentSuc extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeOrderPaymentSuc().start(10016, 4, Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        // 1. 读取下单事务事实表
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
                        "ts bigint," +
                        // ltz --> local time zone，将字符串转换为带有时区信息的 TIMESTAMP_LTZ 类型的函数
                        "et as to_timestamp_ltz(ts, 0), " +
                        "watermark for et as et - interval '3' second)" +
                        // 支付成功的数据从订单明细表中取出
                        SQLUtil.getKafkaDDLSource(Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS,
                                Constant.TOPIC_DWD_TRADE_ORDER_DETAIL)
        );

        // 2. 读取 topic_db
        readOdsDb(tEnv,
                Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS);
        // 3. 读取 字典表
        readBaseDic(tEnv);
        // 4. 从 topic_db 中过滤 payment_info
        Table paymentInfo = tEnv.sqlQuery(
                "SELECT " +
                        "data['user_id'] user_id," +
                        "data['order_id'] order_id," +
                        "data['payment_type'] payment_type," +
                        "data['callback_time'] callback_time," +
                        "pt," +
                        "ts, " +
                        "et " +
                        "FROM topic_db " +
                        "WHERE database = 'gmall' AND table = 'payment_info' AND type = 'update'" +
                        "AND old['payment_status'] IS NOT NULL AND data['payment_status'] = '1602'"
        );
        tEnv.createTemporaryView("payment_info", paymentInfo);

        // 5. 3 张 join: interval join 无需设置 ttl
        Table result = tEnv.sqlQuery(
                "SELECT " +
                        "od.id order_detail_id," +
                        "od.order_id," +
                        "od.user_id," +
                        "od.sku_id," +
                        "od.sku_name," +
                        "od.province_id," +
                        "od.activity_id," +
                        "od.activity_rule_id," +
                        "od.coupon_id," +
                        "pi.payment_type payment_type_code," +
                        "dic.dic_name payment_type_name," +
                        "pi.callback_time," +
                        "od.sku_num," +
                        "od.split_original_amount," +
                        "od.split_activity_amount," +
                        "od.split_coupon_amount," +
                        "od.split_total_amount split_payment_amount," +
                        "pi.ts " +
                        "FROM payment_info pi " +
                        "JOIN dwd_trade_order_detail od " +
                        "ON pi.order_id=od.order_id " +
                        // et 就是 event time
                        "AND od.et >= pi.et - interval '30' minute " +
                        "AND od.et <= pi.et + interval '5' second " +
                        // pt 就是 process time
                        "JOIN base_dic for system_time as of pi.pt as dic " +
                        "ON pi.payment_type = dic.dic_code "
        );

        // 6. 写出到 kafka 中
        tEnv.executeSql(
                "CREATE TABLE dwd_trade_order_payment_success(" +
                        "order_detail_id string," +
                        "order_id string," +
                        "user_id string," +
                        "sku_id string," +
                        "sku_name string," +
                        "province_id string," +
                        "activity_id string," +
                        "activity_rule_id string," +
                        "coupon_id string," +
                        "payment_type_code string," +
                        "payment_type_name string," +
                        "callback_time string," +
                        "sku_num string," +
                        "split_original_amount string," +
                        "split_activity_amount string," +
                        "split_coupon_amount string," +
                        "split_payment_amount string," +
                        "ts bigint )" +
                        SQLUtil.getKafkaDDLSink(Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS)
        );

        result.executeInsert(Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS);
    }
}
