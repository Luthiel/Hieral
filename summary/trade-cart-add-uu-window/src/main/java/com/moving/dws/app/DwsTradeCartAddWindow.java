package com.moving.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.moving.common.base.BaseApp;
import com.moving.common.bean.CartAddUuBean;
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
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DwsTradeCartAddWindow extends BaseApp {
    public static void main(String[] args){
        new DwsTradeCartAddWindow().start(
                10026,
                4,
                "dws_trade_cart_add_window",
                Constant.TOPIC_DWD_TRADE_CART_ADD
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        stream.map(JSON::parseObject)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        // 传入的是 SerializableTimestampAssigner 继承自 TimestampAssigner 接口
                        // 需要实现的方法是 extractTimestamp(T var1, long var2), 即抽取出时间戳
                        .withTimestampAssigner((obj, ts) -> obj.getLong("ts") * 1000)
                        .withIdleness(Duration.ofSeconds(120)))
                .keyBy(obj -> obj.getString("user_id"))
                .process(new KeyedProcessFunction<String, JSONObject, CartAddUuBean>() {
                    private ValueState<String> lastCartAddDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        lastCartAddDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("lastCartAddDate", String.class));
                    }

                    @Override
                    public void processElement(JSONObject obj, Context context, Collector<CartAddUuBean> collector) throws Exception {
                        String lastCartAddDate = lastCartAddDateState.value();
                        Long ts = obj.getLong("ts") * 1000;
                        String curDate = DateFormatUtil.tsToDate(ts);

                        if (!curDate.equals(lastCartAddDate)) {
                            lastCartAddDateState.update(curDate);
                            collector.collect(new CartAddUuBean("", "", curDate, 1L));
                        }
                    }
                })
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<CartAddUuBean>() {
                    @Override
                    public CartAddUuBean reduce(CartAddUuBean bean1, CartAddUuBean bean2) throws Exception {
                        bean1.setCartAddUuCt(bean1.getCartAddUuCt() + bean2.getCartAddUuCt());
                        return bean1;
                    }
                }, new ProcessAllWindowFunction<CartAddUuBean, CartAddUuBean, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<CartAddUuBean> elements, Collector<CartAddUuBean> collector) throws Exception {
                        CartAddUuBean bean = elements.iterator().next();
                        bean.setStt(DateFormatUtil.tsToDateTime(context.window().getStart()));
                        bean.setEdt(DateFormatUtil.tsToDateTime(context.window().getEnd()));
                        bean.setCurDate(DateFormatUtil.tsToDate(context.window().getStart()));

                        collector.collect(bean);
                    }
                })
                .map(new DorisMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink(Constant.DORIS_DATABASE + ".dws_trade_cart_add_uu_window", "dws_trade_cart_add_uu_window"));
    }
}
