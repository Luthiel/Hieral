package com.moving.pojo;

import lombok.Data;

@Data
//@AllArgsConstructor
public class EventPO {

    /* **********************
     * 注意：
     * 属性名称要和写入到Mysql的属性名称一致
     * *********************/

    /**
     * 用户id
     */
    private Integer user_id_int;

    /**
     * 事件发生时间
     */
    private String event_time;


    /**
     * 事件目标名称
     */
    private String event_target_name;

    /**
     * 事件名称
     */
    private String event_name;
    /**
     * 事件类型
     */
    private String event_type;
    /**
     * 事件上下文
     */
    private EventContextPO event_context;

    /**
     * 指标各配置参数
     */
    private MetricsConfPO metrics_conf;

    public EventPO() {
    }

    public EventPO(
            Integer user_id_int,
            String event_time,
            String event_target_name,
            String event_name,
            String event_type,
            EventContextPO event_context) {
        this.user_id_int = user_id_int;
        this.event_time = event_time;
        this.event_target_name = event_target_name;
        this.event_name = event_name;
        this.event_type = event_type;
        this.event_context = event_context;
    }

}