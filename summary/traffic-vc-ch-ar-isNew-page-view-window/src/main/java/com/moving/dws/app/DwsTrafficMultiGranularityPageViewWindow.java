package com.moving.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import com.moving.common.base.BaseApp;
import com.moving.common.bean.TrafficPageViewBean;
import com.moving.common.constant.Constant;
import com.moving.common.function.DorisMapFunction;
import com.moving.common.util.DateFormatUtil;
import com.moving.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

import static org.apache.flink.api.common.state.StateTtlConfig.StateVisibility.NeverReturnExpired;
import static org.apache.flink.api.common.state.StateTtlConfig.UpdateType.OnCreateAndWrite;

/** 流量域 版本-渠道-地区-用户类别粒度 页面浏览窗口汇总 dws_traffic_multi_granularity_page_view_window */
public class DwsTrafficMultiGranularityPageViewWindow extends BaseApp {
  public static void main(String[] args) {
    new DwsTrafficMultiGranularityPageViewWindow()
        .start(
            10022,
            4,
            "dws_traffic_multi_granularity_page_view_window",
            Constant.TOPIC_DWD_TRAFFIC_PAGE);
  }

  @Override
  public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
    // 解析 jsonObject，将解析结果封装到 TrafficPageViewBean 中
    SingleOutputStreamOperator<TrafficPageViewBean> beanStream = parseToPojo(stream);

    // 开窗聚合
    SingleOutputStreamOperator<TrafficPageViewBean> result = windowAndAgg(beanStream);

    writeToDoris(result);
  }

  private static void writeToDoris(SingleOutputStreamOperator<TrafficPageViewBean> result) {
    result
        .map(new DorisMapFunction<TrafficPageViewBean>())
        .sinkTo(
            FlinkSinkUtil.getDorisSink(
                Constant.DORIS_DATABASE + ".dws_traffic_page_view_window",
                "dws_traffic_page_view_window"));
  }

  private static SingleOutputStreamOperator<TrafficPageViewBean> windowAndAgg(
      SingleOutputStreamOperator<TrafficPageViewBean> beanStream) {
    return beanStream
        .assignTimestampsAndWatermarks(
            WatermarkStrategy.<TrafficPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                // 将时间戳提取出来，并生成水印
                .withTimestampAssigner((bean, ts) -> bean.getTs())
                // withIdleness 表示检测空闲输入并将其标记为空闲状态，推动下游生成 watermark
                .withIdleness(Duration.ofSeconds(120)))
        .keyBy(
            bean -> bean.getVc() + "_" + bean.getCh() + "_" + bean.getAr() + "_" + bean.getIsNew())
        // 键控流使用 window，非键控流使用 windowAll
        .window(
            TumblingEventTimeWindows.of(
                org.apache.flink.streaming.api.windowing.time.Time.seconds(5)))
        .reduce(
            new ReduceFunction<TrafficPageViewBean>() {
              // reduce 实现的是规约聚合，第一个值用于保存每次的聚合结果
              @Override
              public TrafficPageViewBean reduce(
                  TrafficPageViewBean bean1, TrafficPageViewBean bean2) throws Exception {
                bean1.setPvCt(bean1.getPvCt() + bean2.getPvCt());
                bean1.setUvCt(bean1.getUvCt() + bean2.getUvCt());
                bean1.setSvCt(bean1.getSvCt() + bean2.getSvCt());
                bean1.setDurSum(bean1.getDurSum() + bean2.getDurSum());

                return bean1;
              }
            },
            new ProcessWindowFunction<
                TrafficPageViewBean, TrafficPageViewBean, String, TimeWindow>() {
              @Override
              public void process(
                  String key,
                  Context context,
                  Iterable<TrafficPageViewBean> elements,
                  Collector<TrafficPageViewBean> collector)
                  throws Exception {
                // elements 有且仅有一个值，即前面聚合的最终结果
                TrafficPageViewBean bean = elements.iterator().next();
                bean.setStt(DateFormatUtil.tsToDateTime(context.window().getStart()));
                bean.setEdt(DateFormatUtil.tsToDateTime(context.window().getEnd()));
                bean.setCur_date(DateFormatUtil.tsToDateForPartition(context.window().getStart()));
                collector.collect(bean);
              }
            });
  }

  private SingleOutputStreamOperator<TrafficPageViewBean> parseToPojo(
      DataStreamSource<String> stream) {
    return stream
        .map(JSON::parseObject)
        .keyBy(obj -> obj.getJSONObject("common").getString("mid"))
        .process(
            new KeyedProcessFunction<String, JSONObject, TrafficPageViewBean>() {
              private ValueState<String> lastVisitDateState;

              // 在 open 方法中获取 Descriptor
              @Override
              public void open(Configuration parameters) throws Exception {
                lastVisitDateState =
                    getRuntimeContext()
                        .getState(new ValueStateDescriptor<String>("lastVisitDate", String.class));
              }

              @Override
              public void processElement(
                  JSONObject obj,
                  // <key_type, in_type, out_type>
                  KeyedProcessFunction<String, JSONObject, TrafficPageViewBean>.Context context,
                  Collector<TrafficPageViewBean> collector)
                  throws Exception {
                JSONObject page = obj.getJSONObject("page");
                JSONObject common = obj.getJSONObject("common");
                Long ts = obj.getLong("ts");
                String today = DateFormatUtil.tsToDate(ts);

                Long pv = 1L;
                Long duration = page.getLong("during_time");
                String lastVisitDate = lastVisitDateState.value();
                Long uv = 0L;
                if (!today.equals(lastVisitDate)) {
                  // first time visit today
                  uv = 1L;
                  lastVisitDateState.update(today);
                }

                TrafficPageViewBean bean = new TrafficPageViewBean();
                bean.setVc(common.getString("vc"));
                bean.setCh(common.getString("ch"));
                bean.setAr(common.getString("ar"));
                bean.setIsNew(common.getString("is_new"));
                bean.setSid(obj.getString("sid"));

                bean.setPvCt(pv);
                bean.setUvCt(uv);
                bean.setDurSum(duration);
                bean.setTs(ts);

                // push to downstream
                collector.collect(bean);
              }
            })
        .keyBy(TrafficPageViewBean::getSid) // 按照 session_id 分组，用于计算 sv
        .process(
            new KeyedProcessFunction<String, TrafficPageViewBean, TrafficPageViewBean>() {
              private ValueState<Boolean> isFirstState;

              @Override
              public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<Boolean> descriptor =
                    new ValueStateDescriptor<>("isFirst", Boolean.class);
                StateTtlConfig conf =
                    new StateTtlConfig.Builder(Time.hours(1))
                        .setStateVisibility(NeverReturnExpired)
                        .setUpdateType(OnCreateAndWrite)
                        .useProcessingTime()
                        .build();
                descriptor.enableTimeToLive(conf);
                isFirstState = getRuntimeContext().getState(descriptor);
              }

              @Override
              public void processElement(
                  TrafficPageViewBean bean,
                  Context context,
                  Collector<TrafficPageViewBean> collector)
                  throws Exception {
                if (isFirstState.value() == null) {
                  bean.setSvCt(1L);
                  isFirstState.update(true);
                } else {
                  bean.setSvCt(0L);
                }
                collector.collect(bean);
              }
            });
  }
}
