package com.moving.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.moving.common.base.BaseApp;
import com.moving.common.bean.UserRegisterBean;
import com.moving.common.constant.Constant;
import com.moving.common.function.DorisMapFunction;
import com.moving.common.util.DateFormatUtil;
import com.moving.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DwsUserRegisterWindow extends BaseApp {
    public static void main(String[] args){
        new DwsUserRegisterWindow().start(
                10025,
                4,
                "dws_user_user_register_window",
                Constant.TOPIC_DWD_USER_REGISTER
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // TODO parse string to jsonObject
        // 因为之前没有实现 dwd_user_register
        // 因为只是统计注册用户数，因此不需要状态判断，而是直接从 create_time 获取注册时间
        // （这个需求是不是应该由离线仓实现？或者直接使用 FlinkSQL？）

        stream.map(JSON::parseObject).assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        // fastjson 自动将 datetime 转换为 ts
                        .withTimestampAssigner((obj, ts) -> obj.getLong("create_time"))
                        .withIdleness(Duration.ofSeconds(120)))
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .aggregate(new AggregateFunction<JSONObject, Long, Long>() {
                    @Override
                    public Long createAccumulator() {
                        return 0L;
                    }

                    @Override
                    public Long add(JSONObject obj, Long acc) {
                        return acc + 1;
                    }

                    @Override
                    public Long getResult(Long acc) {
                        return acc;
                    }

                    @Override
                    public Long merge(Long acc1, Long acc2) {
                        return acc1 + acc2;
                    }
                }, new ProcessAllWindowFunction<Long, UserRegisterBean, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<Long> elements, Collector<UserRegisterBean> collector) throws Exception {
                        Long result = elements.iterator().next();
                        collector.collect(new UserRegisterBean(DateFormatUtil.tsToDateTime(context.window().getStart()),
                                DateFormatUtil.tsToDateTime(context.window().getStart()),
                                DateFormatUtil.tsToDateForPartition(context.window().getEnd()), result));
                    }
                })
                .map(new DorisMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink(Constant.DORIS_DATABASE + ".dws_user_user_register_window",
                        "dws_user_user_register_window"));
    }
}
