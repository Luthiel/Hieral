package com.moving.pojo;

import lombok.Data;

@Data
public class MetricsConfPO {

    /* **********************
     *
     * 注意：
     *
     * 1.
     * 属性名称必须和 Mysql 表 metric_attr 的字段名称一致
     *
     * 2.
     * 为了后面反射赋值的方便,
     * 属性数据类型都是 String
     *
     *
     * *********************/



    /**
     * 指标名称
     */
    private String metric_name;
    /**
     * 指标编码
     */
    private String metric_code;
    /**
     * 主维度
     */
    private String main_dim;
    /**
     * flink窗口大小
     */
    private String window_size;
    /**
     * flink窗口大小
     */
    private String window_step;
    /**
     * flink窗口类型
     */
    private String window_type;
    /**
     * flink筛选
     */
    private String flink_filter;
    /**
     * flink分组
     */
    private String flink_keyby;
    /**
     * flink 聚合计算方式
     */
    private String flink_aggregate;
    /**
     * flink 窗口开始时间 (默认值是0)
     */
    private String window_start_time;
    /**
     * flink 窗口结束时间 (默认值是0)
     */
    private String window_end_time;
    /**
     * flink 窗口持续天数 (默认值是0)
     */
    private String window_days;
    /**
     * flink 累加器计算方法
     */
    private String acc_aggregate;
}