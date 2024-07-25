package com.moving.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.moving.common.base.BaseApp;
import com.moving.common.bean.UserLoginBean;
import com.moving.common.constant.Constant;
import com.moving.common.function.DorisMapFunction;
import com.moving.common.util.DateFormatUtil;
import com.moving.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

// 统计 7 日回流用户和当日独立用户数
// source: Kafka page view log
public class DwsUserLoginWindow extends BaseApp {
  public static void main(String[] args) {
    new DwsUserLoginWindow()
        .start(10024, 4, "dws_user_user_login_window", Constant.TOPIC_DWD_TRAFFIC_PAGE);
  }

  @Override
  public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
    // TODO parseToPojo string -> jsonObject -> pojo
    // 1. 先过滤出所有的 登录 数据
    SingleOutputStreamOperator<JSONObject> loginLogStream =
        stream
            .map(JSON::parseObject)
            .filter(
                new FilterFunction<JSONObject>() {
                  @Override
                  public boolean filter(JSONObject obj) throws Exception {
                    String uid = obj.getJSONObject("common").getString("uid");
                    String lastPageId = obj.getJSONObject("page").getString("last_page_id");

                    return uid != null && "login".equals(lastPageId);
                  }
                });

    // 2. 解析并封装到 pojo
    SingleOutputStreamOperator<UserLoginBean> beanStream =
        loginLogStream
            .keyBy(obj -> obj.getJSONObject("common").getString("uid"))
            .process(
                new KeyedProcessFunction<String, JSONObject, UserLoginBean>() {
                  private ValueState<String> lastLoginDateState;

                  @Override
                  public void open(Configuration parameters) throws Exception {
                    lastLoginDateState =
                        getRuntimeContext()
                            .getState(
                                new ValueStateDescriptor<String>("lastLoginDate", String.class));
                  }

                  @Override
                  public void processElement(
                      JSONObject obj, Context context, Collector<UserLoginBean> collector)
                      throws Exception {
                    Long ts = obj.getLong("ts");
                    String curDate = DateFormatUtil.tsToDate(ts);
                    String lastLoginDate = lastLoginDateState.value();
                    Long uuCt = 0L, backCt = 0L;

                    if (!curDate.equals(lastLoginDate)) {
                      // 今日首次
                      uuCt = 1L;
                      lastLoginDateState.update(curDate);

                      // 计算回流
                      if (lastLoginDate != null) {
                        Long lastLoginTs = DateFormatUtil.dateTimeToTs(lastLoginDate);
                        if ((ts - lastLoginTs) >= 7 * 24 * 60 * 60 * 1000) {
                          backCt = 1L;
                        }
                      }
                    }

                    if (uuCt == 1) {
                      collector.collect(new UserLoginBean("", "", "", backCt, uuCt, ts));
                    }
                  }
                });

    // TODO windowAndAgg keyBy uid -> record the last login state of user
    SingleOutputStreamOperator<UserLoginBean> resultStream =
        beanStream
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<UserLoginBean>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                    .withTimestampAssigner((bean, ts) -> bean.getTs())
                    .withIdleness(Duration.ofSeconds(10)))
            .windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
            .reduce(
                new ReduceFunction<UserLoginBean>() {
                  @Override
                  public UserLoginBean reduce(UserLoginBean bean1, UserLoginBean bean2)
                      throws Exception {
                    bean1.setUuCt(bean1.getUuCt() + bean2.getUuCt());
                    bean1.setBackCt(bean1.getBackCt() + bean2.getBackCt());
                    return bean1;
                  }
                },
                new AllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>() {
                  @Override
                  public void apply(
                      TimeWindow timeWindow,
                      Iterable<UserLoginBean> values,
                      Collector<UserLoginBean> collector)
                      throws Exception {
                    UserLoginBean bean = values.iterator().next();
                    bean.setStt(DateFormatUtil.tsToDateTime(timeWindow.getStart()));
                    bean.setEdt(DateFormatUtil.tsToDateTime(timeWindow.getEnd()));
                    bean.setCurDate(DateFormatUtil.tsToDate(timeWindow.getStart()));

                    collector.collect(bean);
                  }
                });

    // TODO sinkToDoris
    resultStream
        .map(new DorisMapFunction<>())
        .sinkTo(
            FlinkSinkUtil.getDorisSink(
                Constant.DORIS_DATABASE + ".dws_user_user_login_window",
                "dws_user_user_login_window"));
  }
}
