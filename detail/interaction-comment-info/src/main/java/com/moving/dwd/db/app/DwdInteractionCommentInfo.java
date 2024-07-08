package com.moving.dwd.db.app;

import com.moving.common.base.BaseSQLApp;
import com.moving.common.constant.Constant;
import com.moving.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdInteractionCommentInfo extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdInteractionCommentInfo().start(10012, 4, Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        // 通过 DDL 的方式建立动态表：从 topic_db (kafka) 读取数据（source）
        readOdsDb(tEnv, Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);
        // 过滤出评论表数据
        Table commentInfo = tEnv.sqlQuery(
                "select " +
                        " data['id'] id, " +
                        " data['user_id'] user_id, " +
                        " data['sku_id'] sku_id, " +
                        " data['appraise'] appraise, " +
                        " data['comment_txt'] comment_txt, " +
                        " data['create_time'] create_time, " +
                        " ts, " +
                        " pt " +
                        " from topic_db " +
                        " where database = 'gmall' and table = 'comment_info' and type = 'insert' "
        );
        // 将返回结果创建为临时视图
        tEnv.createTemporaryView("comment_info", commentInfo);

        // 通过 DDL 建表：base_dic (hbase) 中的维度表
        readBaseDic(tEnv);
        // 事实表 join 维度表：lookup join
        Table result = tEnv.sqlQuery(
                "SELECT " +
                        " ci.id, " +
                        " ci.user_id, " +
                        " ci.sku_id, " +
                        " ci.appraise, " +
                        " dic.info.dic_name appraise_name, " +
                        " ci.comment_txt, " +
                        " ci.ts " +
                        " FROM comment_info ci " +
                        // 创建动态表（dynamic tables），它们可以随着新数据的到来而变化。使用for system_time as of语法，
                        // 可以查询这些表在特定时间点的快照，这在处理数据流时非常有用，尤其是在需要根据历史数据状态进行计算和分析时
                        " JOIN base_dic FOR SYSTEM_TIME AS OF ci.pt AS dic " +
                        " ON ci.appraise = dic.dic_code"
        );

        // 通过 DDL 方式建表：kafka 的 topic 管理（sink）
        tEnv.sqlQuery(
                " CREATE TABLE dwd_interaction_comment_info (" +
                        " id STRING, " +
                        " user_id STRING, " +
                        " sku_id STRING, " +
                        " appraise STRING, " +
                        "comment_txt STRING, " +
                        " ts BIGINT)" +
                        SQLUtil.getKafkaDDLSink(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO)
        );

        // 将 join 的结果放入 sink 表中
        result.executeInsert(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);
    }
}
