package com.moving.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.moving.common.base.BaseApp;
import com.moving.common.bean.TrafficHomeDetailPageViewBean;
import com.moving.common.constant.Constant;
import com.moving.common.function.DorisMapFunction;
import com.moving.common.util.DateFormatUtil;
import com.moving.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DwsTrafficHomeDetailPageViewWindow extends BaseApp {
    public static void main(String[] args){
        new DwsTrafficHomeDetailPageViewWindow().start(
                10023,
                4,

                "dws_traffic_home_detail_page_view_window",
                Constant.TOPIC_DWD_TRAFFIC_PAGE
                );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 解析 jsonObject 并封装为 Java Bean
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> beanStream = parseToBean(stream);

        // 开窗聚合
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> result = windowAndAgg(beanStream);

        // 写出到 Doris
        writeToDoris(result);
    }

    private static void writeToDoris(SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> result) {
        result.map(new DorisMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink(
                        Constant.DORIS_DATABASE +
                        ".dws_traffic_home_detail_page_view_window", "dws_traffic_home_detail_page_view_window"));
    }

    private static SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> windowAndAgg(SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> beanStream) {
        return beanStream
                .assignTimestampsAndWatermarks(WatermarkStrategy.<TrafficHomeDetailPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((bean, ts) -> bean.getTs())
                        .withIdleness(Duration.ofSeconds(120)))
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<TrafficHomeDetailPageViewBean>() {
                    @Override
                    public TrafficHomeDetailPageViewBean reduce(TrafficHomeDetailPageViewBean bean1, TrafficHomeDetailPageViewBean bean2) throws Exception {
                        bean1.setHomeUvCt(bean1.getHomeUvCt() + bean2.getHomeUvCt());
                        bean1.setGoodDetailUvCt(bean1.getGoodDetailUvCt() + bean2.getGoodDetailUvCt());
                        return bean1;
                    }
                }, new ProcessAllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<TrafficHomeDetailPageViewBean> elements, Collector<TrafficHomeDetailPageViewBean> collector) throws Exception {
                        TrafficHomeDetailPageViewBean bean = elements.iterator().next();
                        bean.setStt(DateFormatUtil.tsToDateTime(context.window().getStart()));
                        bean.setEdt(DateFormatUtil.tsToDateTime(context.window().getEnd()));
                        bean.setCurDate(DateFormatUtil.tsToDate(context.window().getStart()));

                        collector.collect(bean);
                    }
                });
    }

    private SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> parseToBean(DataStreamSource<String> stream) {
        return stream.map(JSON::parseObject)
                .keyBy(obj -> obj.getJSONObject("common").getString("mid"))
                .process(new KeyedProcessFunction<String, JSONObject, TrafficHomeDetailPageViewBean>() {
                    private ValueState<String> homeState;
                    private ValueState<String> goodDetailState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> homeDesc = new ValueStateDescriptor<>("home", String.class);
                        ValueStateDescriptor<String> goodDetailDesc = new ValueStateDescriptor<>("goodDetail", String.class);

                        homeState = getRuntimeContext().getState(homeDesc);
                        goodDetailState = getRuntimeContext().getState(goodDetailDesc);
                    }

                    @Override
                    public void processElement(JSONObject obj, Context context, Collector<TrafficHomeDetailPageViewBean> collector) throws Exception {
                        String pageId = obj.getJSONObject("page").getString("page_id");
                        Long ts = obj.getLong("ts");
                        String today = DateFormatUtil.tsToDate(ts);

                        String lastHomeDate = homeState.value();
                        String lastGoodDetailDate = goodDetailState.value();
                        Long homeCt = 0L;
                        Long goodDetailCt = 0L;

                        if ("home".equals(pageId) && !today.equals(lastHomeDate)) {
                            homeCt = 1L;
                            homeState.update(today);
                        } else if ("good_detail".equals(pageId) && !today.equals(lastGoodDetailDate)) {
                            goodDetailCt = 1L;
                            goodDetailState.update(today);
                        }

                        if (homeCt + goodDetailCt == 1) {
                            collector.collect(new TrafficHomeDetailPageViewBean("", "", "", homeCt, goodDetailCt, ts));
                        }
                    }
                });
    }
}


