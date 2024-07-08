package com.moving.dwd.db.app;

import com.moving.common.base.BaseSQLApp;
import com.moving.common.constant.Constant;
import com.moving.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class DwdTradeRefundPaymentSuc extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeRefundPaymentSuc().start(10018, 4, Constant.TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        // set ttl，考虑数据乱序
        tEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5));

        // 1.读取 topic_db
        // 消费 Kafka 中 topic_db 主题下的数据，消费者组 ID 为 Constant.TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS
        // 在上下文环境中创建 topic_db 增量表，载入相应的 Kafka 消费数据：从 latest-offset 读取
        readOdsDb(tEnv, Constant.TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS);
        // 2.读取 字典表
        readBaseDic(tEnv);
        // 3.过滤退款成功表的数据
        Table refundPayment = tEnv.sqlQuery(
                " SELECT " +
                        " data['id'] id, " +
                        " data['order_id'] order_id, " +
                        " data['sku_id'] sku_id, " +
                        " data['payment_type'] payment_type, " +
                        " data['callback_time'] callback_time, " +
                        " data['total_amount'] total_amount, " +
                        " pt, " + // process_time，创建 topic_db 表时由 proctime() 函数得到
                        " ts " +
                        " FROM topic_db " +
                        " WHERE database = 'gmall' AND table = 'refund_payment' AND type = 'update' " +
                        " AND old['refund_status'] IS NOT NULL AND data['refund_status'] = '1602'"
        );
        tEnv.createTemporaryView("refund_payment", refundPayment);

        // 4.过滤退单表中退单成功的数据
        Table orderRefundInfo = tEnv.sqlQuery(
                " SELECT " +
                        " data['order_id'] order_id, " +
                        " data['sku_id'] sku_id, " +
                        " data['refund_num'] refund_num, " +
                        " FROM topic_db " +
                        " WHERE database = 'gmall' AND table = 'order_refund_info' AND type = 'update' " +
                        " AND old['refund_status'] IS NOT NULL AND data['refund_status'] = '0705'"
        );
        tEnv.createTemporaryView("order_refund_info", orderRefundInfo);

        // 5.过滤订单表中退款成功的数据
        Table orderInfo = tEnv.sqlQuery(
                " SELECT " +
                        " data['id'] id, " +
                        " data['user_id'] user_id, " +
                        " data['province_id'] province_id, " +
                        " FROM topic_db " +
                        " WHERE database = 'gmall' AND table = 'order_info' AND type = 'update' " +
                        " AND old['refund_status'] IS NOT NULL AND data['refund_status'] = '1006'"
        );
        tEnv.createTemporaryView("order_info", orderInfo);

        // 6.连接 4 张表
        Table result = tEnv.sqlQuery(
                "SELECT *" +
                        " rp.id, " +
                        " oi.user_id, " +
                        " rp.order_id, " +
                        " rp.sku_id, " +
                        " oi.province_id, " +
                        " rp.payment_type, " +
                        " dic.info.dic_name payment_type_name, " +
                        " DATE_FORMAT(rp.callback_time, 'yyyy-MM-dd') date_id, " +
                        " rp.callback_time, " +
                        " ori.refund_num, " +
                        " rp.total_amount, " +
                        " rp.ts " +
                        " FROM refund_payment rp " +
                        " JOIN order_refund_info ori ON rp.order_id = ori.order_id AND rp.sku_id = ori.sku_id " +
                        " JOIN order_info oi ON rp.order_id = oi.id " +
                        " JOIN base_dic FOR SYSTEM_TIME AS OF rp.pt AS dic ON rp.payment_type = dic.dic_code"
        );

        // 7.结果表 DDL
        tEnv.executeSql(
                "CREATE TABLE dwd_trade_refund_payment_success (" +
                        " id string, " +
                        " user_id string, " +
                        " order_id string, " +
                        " sku_id string, " +
                        " province_id string, " +
                        " payment_type_code string, " +
                        " payment_type_name string, " +
                        " date_id string, " +
                        " callback_time string, " +
                        " refund_num string, " +
                        " refund_amount string, " +
                        " ts bigint) " +
                        SQLUtil.getKafkaDDLSink(Constant.TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS)
        );

        // 写入 Kafka，result 写入 结果表
        result.executeInsert(Constant.TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS);
    }
}
