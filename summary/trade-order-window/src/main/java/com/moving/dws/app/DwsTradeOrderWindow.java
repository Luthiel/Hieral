package com.moving.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.moving.common.base.BaseApp;
import com.moving.common.bean.TradeOrderBean;
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
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

// Source: Kafka_topic -> dwd_order_detail
public class DwsTradeOrderWindow extends BaseApp {
  public static void main(String[] args) {
    new DwsTradeOrderWindow()
        .start(10028, 4, "dws_trade_order_window", Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);
  }

  @Override
  public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
    stream
        .map(JSON::parseObject)
        .assignTimestampsAndWatermarks(
            WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((obj, ts) -> obj.getLong("ts") * 1000)
                .withIdleness(Duration.ofSeconds(10)))
        .keyBy(obj -> obj.getString("user_id"))
        .process(
            new ProcessFunction<JSONObject, TradeOrderBean>() {
              private ValueState<String> lastOrderDateState;

              @Override
              public void open(Configuration parameters) throws Exception {
                lastOrderDateState =
                    getRuntimeContext()
                        .getState(new ValueStateDescriptor<String>("lastOrderDate", String.class));
              }

              @Override
              public void processElement(
                  JSONObject obj, Context context, Collector<TradeOrderBean> collector)
                  throws Exception {
                String lastOrderDate = lastOrderDateState.value();
                Long ts = obj.getLong("ts") * 1000;
                String curDate = DateFormatUtil.tsToDate(ts);

                Long orderUniqueUserCount = 0L;
                Long orderNewUserCount = 0L;

                if (!curDate.equals(lastOrderDate)) {
                  orderUniqueUserCount = 1L;
                  lastOrderDateState.update(curDate);

                  if (lastOrderDate == null) {
                    orderNewUserCount = 1L;
                  }
                }

                // 因为只统计有效的用户，即今日首次下单用户才放入下游
                if (orderUniqueUserCount == 1) {
                  collector.collect(
                      new TradeOrderBean("", "", "", orderUniqueUserCount, orderNewUserCount, ts));
                }
              }
            })
        .windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
        .reduce(
            new ReduceFunction<TradeOrderBean>() {
              @Override
              public TradeOrderBean reduce(TradeOrderBean bean1, TradeOrderBean bean2)
                  throws Exception {
                bean1.setOrderUniqueUserCount(
                    bean1.getOrderNewUserCount() + bean2.getOrderNewUserCount());
                bean1.setOrderNewUserCount(
                    bean1.getOrderNewUserCount() + bean2.getOrderNewUserCount());

                return bean1;
              }
            },
            new ProcessAllWindowFunction<TradeOrderBean, TradeOrderBean, TimeWindow>() {
              @Override
              public void process(
                  Context context,
                  Iterable<TradeOrderBean> elements,
                  Collector<TradeOrderBean> collector)
                  throws Exception {
                TradeOrderBean bean = elements.iterator().next();
                bean.setStt(DateFormatUtil.tsToDateTime(context.window().getStart()));
                bean.setEdt(DateFormatUtil.tsToDateTime(context.window().getEnd()));
                bean.setCurDate(DateFormatUtil.tsToDateTime(context.window().getStart()));

                collector.collect(bean);
              }
            })
        .map(new DorisMapFunction<>())
        .sinkTo(
            FlinkSinkUtil.getDorisSink(
                Constant.DORIS_DATABASE + ".dws_trade_order_window", "dws_trade_order_window"));
  }
}
