package com.moving.dwd.db.app;

import com.moving.common.base.BaseSQLApp;
import com.moving.common.constant.Constant;
import com.moving.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class DwdTradeOrderRefund extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeOrderRefund().start(10017, 4, Constant.TOPIC_DWD_TRADE_ORDER_REFUND);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        tEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5));

        // 读取 topic_db
        readOdsDb(tEnv, Constant.TOPIC_DWD_TRADE_ORDER_REFUND);
        // 读取 字典表 for look up
        readBaseDic(tEnv);

        // 筛选退单表数据 order_refund_info insert
        Table orderRefundInfo = tEnv.sqlQuery(
                "SELECT " +
                        " data['id'] id, " +
                        " data['user_id'] user_id, " +
                        " data['order_id'] order_id, " +
                        " data['sku_id'] sku_id, " +
                        " data['refund_type'] refund_type, " +
                        " data['refund_num'] refund_num, " +
                        " data['refund_amount'] refund_amount, " +
                        " data['refund_reason_type'] refund_reason_type, " +
                        " data['refund_reason_txt'] refund_reason_txt, " +
                        " data['create_time'] create_time, " +
                        " pt, " +
                        " ts " +
                        " FROM topic_db " +
                        " WHERE database = 'gmall' AND table = order_refund_info AND type = 'insert'"
        );
        tEnv.createTemporaryView("order_refund_info", orderRefundInfo);

        // 过滤订单表中的退单数据
        Table orderInfo = tEnv.sqlQuery(
                "SELECT " +
                        "data['id'] id, " +
                        "data['province_id'] province_id, " +
                        "old " +
                        "FROM topic_db " +
                        "WHERE database = 'gmall' AND type = 'update' AND old['order_status'] IS NOT NULL " +
                        "AND data['order_status'] = '1005'"
        );
        tEnv.createTemporaryView("order_info", orderInfo);

        // common join && lookup join
        Table result = tEnv.sqlQuery(
                "SELECT " +
                        " ri.id, " +
                        " ri.user_id, " +
                        " ri.order_id, " +
                        " ri.sku_id, " +
                        " oi.province_id, " +
                        " date_format(ri.create_time, 'yyyy-MM-dd') date_id, " +
                        " ri.create_time, " +
                        " ri.refund_type, " +
                        " dic1.info.dic_name, " +
                        " ri.refund_reason_type, " +
                        " dic2.info.dic_name, " +
                        " ri.refund_num, " +
                        " ri.refund_amount, " +
                        " ri.ts " +
                        " FROM order_refund_info ri JOIN order_info oi " +
                        " ON ri.order_id = oi.id " +
                        " JOIN base_dic FOR SYSTEM_TIME AS OF ri.pt AS dic1" +
                        " ON ri.refund_type = dic1.dic_code " +
                        " JOIN base_dic FOR SYSTEM_TIME AS OF ri.pt AS dic2 " +
                        " ON ri.refund_reason_type = dic2.dic_code "
        );

        // 写出到 Kafka
        tEnv.executeSql(
                " CREATE TABLE dwd_trade_order_refund(" +
                        " id string, " +
                        " user_id string, " +
                        " order_id string, " +
                        " sku_id string, " +
                        " province_id string, " +
                        " date_id string, " +
                        " create_time string, " +
                        " refund_type_code string, " +
                        " refund_type_name string, " +
                        " refund_reason_type string, " +
                        " refund_reason_type_name string, " +
                        " refund_reason_txt string, " +
                        " refund_num string, " +
                        " refund_amount string, " +
                        " ts bigint)" +
                        SQLUtil.getKafkaDDLSink(Constant.TOPIC_DWD_TRADE_ORDER_REFUND)
        );

        result.executeInsert(Constant.TOPIC_DWD_TRADE_ORDER_REFUND);
    }
}
