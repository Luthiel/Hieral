package com.moving.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.moving.common.base.BaseApp;
import com.moving.common.bean.TradePaymentBean;
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

// from kafka topic: dwd_trade_payment_suc
// 统计独立支付人数和新增支付人数
/*
 * 若末次支付日期为 null，则将首次支付用户数和支付独立用户数均置为 1；否则首次支
 * 付用户数置为 0，判断末次支付日期是否为当日，如果不是当日则支付独立用户数置为 1，否则置为 0
 * */
public class DwsTradePaymentSucWindow extends BaseApp {
  public static void main(String[] args) {
    new DwsTradePaymentSucWindow()
        .start(
            10027,
            4,
            "dws_trade_payment_suc_window",
            Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS);
  }

  @Override
  public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
    stream
        .map(JSON::parseObject)
        // 水位线只要在开窗前设置好即可
        .assignTimestampsAndWatermarks(
            WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((obj, ts) -> obj.getLong("ts") * 1000)
                .withIdleness(Duration.ofSeconds(120)))
        .keyBy(obj -> obj.getString("user_id"))
        .process(
            new ProcessFunction<JSONObject, TradePaymentBean>() {
              private ValueState<String> lastPayDateState;

              @Override
              public void open(Configuration parameters) throws Exception {
                lastPayDateState =
                    getRuntimeContext()
                        .getState(new ValueStateDescriptor<String>("lastPayDate", String.class));
              }

              @Override
              public void processElement(
                  JSONObject obj, Context context, Collector<TradePaymentBean> collector)
                  throws Exception {
                String lastPayDate = lastPayDateState.value();
                Long ts = obj.getLong("ts") * 1000;
                String curDate = DateFormatUtil.tsToDate(ts);

                Long paymentSucUniqueUserCount = 0L;
                Long paymentSucNewUserCount = 0L;

                if (!curDate.equals(lastPayDate)) {
                  paymentSucUniqueUserCount = 1L;
                  lastPayDateState.update(curDate);

                  if (lastPayDate == null) {
                    paymentSucNewUserCount = 1L;
                  }
                }

                if (paymentSucUniqueUserCount == 1) {
                  collector.collect(
                      new TradePaymentBean(
                          "", "", "", paymentSucUniqueUserCount, paymentSucNewUserCount, ts));
                }
              }
            })
        .windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
        .reduce(
            new ReduceFunction<TradePaymentBean>() {
              @Override
              public TradePaymentBean reduce(TradePaymentBean bean1, TradePaymentBean bean2)
                  throws Exception {
                bean1.setPaymentSucNewUserCount(
                    bean1.getPaymentSucNewUserCount() + bean2.getPaymentSucNewUserCount());
                bean1.setPaymentSucUniqueUserCount(
                    bean1.getPaymentSucUniqueUserCount() + bean2.getPaymentSucUniqueUserCount());

                return bean1;
              }
            },
            new ProcessAllWindowFunction<TradePaymentBean, TradePaymentBean, TimeWindow>() {
              @Override
              public void process(
                  Context context,
                  Iterable<TradePaymentBean> elements,
                  Collector<TradePaymentBean> collector)
                  throws Exception {
                TradePaymentBean bean = elements.iterator().next();
                bean.setStt(DateFormatUtil.tsToDateTime(context.window().getStart()));
                bean.setEdt(DateFormatUtil.tsToDateTime(context.window().getEnd()));
                bean.setCurDate(DateFormatUtil.tsToDate(context.window().getStart()));

                collector.collect(bean);
              }
            })
        .map(new DorisMapFunction<>())
        .sinkTo(
            FlinkSinkUtil.getDorisSink(
                Constant.DORIS_DATABASE + ".dws_trade_payment_suc_window",
                "dws_trade_payment_suc_window"));
  }
}
