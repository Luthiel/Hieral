package com.moving.dwd.db.app;

import com.moving.common.base.BaseSQLApp;
import com.moving.common.constant.Constant;
import com.moving.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class DwdTradeOrderDetail extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeOrderDetail().start(10014, 4, Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        // 涉及 join：默认所有表的数据都会一直存储在内存中，因此要添加 ttl，防止内存过载，及时删除不需要的 状态
        // 设置 5s
        tEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5));

        // 读取数据并过滤
        // 1. order_detail
        readOdsDb(tEnv, Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);
        Table orderDetail = tEnv.sqlQuery(
                "SELECT " +
                        " data['id'] id, " +
                        " data['order_id'] order_id, " +
                        " data['sku_id'] sku_id, " +
                        " data['sku_name'] sku_name, " +
                        " data['sku_num'] sku_num, " +
                        " data['create_time'] create_time, " +
                        " data['source_id'] source_id, " +
                        " data['source_type'] source_type, " +
                        // 分摊原始总金额
                        " CAST(CAST(data['sku_num'] AS DECIMAL(16, 2)) * CAST(data['order_price'] AS DECIMAL(16, 2)) AS STRING) split_original_amount, " +
                        // 分摊总金额
                        " data['split_total_amount'] split_total_amount, " +
                        // 分摊活动金额
                        " data['split_activity_amount'] split_activity_amount, " +
                        " data['split_coupon_amount'] split_coupon_amount, " +
                        " ts " +
                        " FROM topic_db " +
                        " WHERE database = 'gmall' AND table = 'order_detail' AND type = 'insert'"
        );
        tEnv.createTemporaryView("order_detail", orderDetail);

        // 2. order_info
        Table orderInfo = tEnv.sqlQuery(
                " SELECT " +
                        " data['id'] id, " +
                        " data['user_id'] user_id, " +
                        " data['province_id'] province_id " +
                        " FROM topic_db " +
                        " WHERE database = 'gmall' AND table = 'order_info' AND type = 'insert'"
        );
        tEnv.createTemporaryView("order_info", orderInfo);

        // 3. order_detail_activity
        Table orderDetailActivity = tEnv.sqlQuery(
                "SELECT " +
                        " data['order_detail_id'] order_detail_id, " +
                        " data['activity_id'] activity_id, " +
                        " data['activity_rule_id'] activity_rule_id " +
                        " FROM topic_db " +
                        " WHERE database = 'gmall' AND table = 'order_detail_activity' AND type = 'insert'"
        );
        tEnv.createTemporaryView("order_detail_activity", orderDetailActivity);

        // 4. order_detail_coupon
        Table orderDetailCoupon = tEnv.sqlQuery(
                "SELECT " +
                        " data['order_detail_id'] order_detail_id, " +
                        " data['coupon_id'] coupon_id " +
                        " FORM topic_db " +
                        " WHERE database = 'gmall' AND table = 'order_detail_coupon' AND type = 'insert'"
        );
        tEnv.createTemporaryView("order_detail_coupon", orderDetailCoupon);

        // join 各表
        Table result = tEnv.sqlQuery(
                "SELECT " +
                        "od.id," +
                        "od.order_id," +
                        "oi.user_id," +
                        "od.sku_id," +
                        "od.sku_name," +
                        "oi.province_id," +
                        "act.activity_id," +
                        "act.activity_rule_id," +
                        "cou.coupon_id," +
                        "date_format(od.create_time, 'yyyy-MM-dd') date_id," +
                        // 年月日
                        "od.create_time," +
                        "od.sku_num," +
                        "od.split_original_amount," +
                        "od.split_activity_amount," +
                        "od.split_coupon_amount," +
                        "od.split_total_amount," +
                        "od.ts " +
                        "FROM order_detail od " +
                        "JOIN order_info oi ON od.order_id = oi.id " +
                        "LEFT JOIN order_detail_activity act ON od.id=act.order_detail_id " +
                        "LEFT JOIN order_detail_coupon cou ON od.id=cou.order_detail_id "
        );

        // 写出到 kafka
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
                        "primary key(id) not enforced " +
                        ")" +
                        SQLUtil.getUpsertKakfaDDL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL)
        );

        result.executeInsert(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);
    }
}
