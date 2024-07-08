package com.moving.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.moving.common.base.BaseApp;
import com.moving.common.constant.Constant;
import com.moving.common.util.DateFormatUtil;
import com.moving.common.util.FlinkSinkUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class DwdBaseLog extends BaseApp {
    /*
    * 该模块处理的三件事：
    * 1. 过滤不满足的 Json 格式的数据，视为脏数据将其放入测输出流
    * 2. 修复 is_new 标签，保障用户属性的正确性
    * 3.
    * */
    private final String START = "start";
    private final String ERR = "error";
    private final String DISPLAY = "display";
    private final String ACTION = "action";
    private final String PAGE = "page";

    public static void main(String[] args) {
        new DwdBaseLog().start(10011, 4, "dwd_base_log", Constant.TOPIC_LOG);
    }


    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 1. ETL
        SingleOutputStreamOperator<JSONObject> etlStream = etl(stream);
        // 2. 纠正新老用户属性
        SingleOutputStreamOperator<JSONObject> validateStream = validateNewOrOld(etlStream);
        // 3. 分流
        Map<String, DataStream<JSONObject>> streams = splitStream(validateStream);
        // 4. 不同六写出到不同的 topic
        writeToKafka(streams);
    }

    private void writeToKafka(Map<String, DataStream<JSONObject>> streams) {
        streams.get(START)
                .map(JSONAware::toJSONString)
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_START));

        streams.get(ERR)
                .map(JSONAware::toJSONString)
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ERR));

        streams.get(DISPLAY)
                .map(JSONAware::toJSONString)
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_DISPLAY));

        streams.get(ACTION)
                .map(JSONAware::toJSONString)
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ACTION));

        streams.get(PAGE)
                .map(JSONAware::toJSONString)
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_PAGE));
    }

    private Map<String, DataStream<JSONObject>> splitStream(SingleOutputStreamOperator<JSONObject> validateStream) {
        OutputTag<JSONObject> displayTag = new OutputTag<>("display");
        OutputTag<JSONObject> actionTag = new OutputTag<>("action");
        OutputTag<JSONObject> errTag = new OutputTag<>("err");
        OutputTag<JSONObject> pageTag = new OutputTag<>("page");

        SingleOutputStreamOperator<JSONObject> startStream = validateStream.process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject jsonObject,
                                       ProcessFunction<JSONObject, JSONObject>.Context context,
                                       Collector<JSONObject> collector) throws Exception {
                JSONObject common = jsonObject.getJSONObject("common");
                Long ts = jsonObject.getLong("ts");
                // 1. 启动日志
                JSONObject start = jsonObject.getJSONObject("start");
                if (start != null) {
                    collector.collect(jsonObject);
                }

                // 2. 曝光日志
                JSONArray displays = jsonObject.getJSONArray("displays");
                if (displays != null) {
                    for (int i = 0; i < displays.size(); i++) {
                        JSONObject display = displays.getJSONObject(i);
                        display.putAll(common);
                        display.put("ts", ts);
                        context.output(displayTag, display);
                    }
                    // 删除 display
                    jsonObject.remove("displays");
                }

                //3. 活动日志
                JSONArray actions = jsonObject.getJSONArray("actions");
                if (actions != null) {
                    for (int i = 0; i < actions.size(); i++) {
                        JSONObject action = actions.getJSONObject(i);
                        action.putAll(common);
                        context.output(actionTag, action);
                    }
                    // 删除 actions
                    jsonObject.remove("actions");
                }

                // 4. 错误日志
                JSONObject err = jsonObject.getJSONObject("err");
                if (err != null) {
                    context.output(errTag, jsonObject);
                    jsonObject.remove("err");
                }

                // 5. 页面访问日志
                JSONObject page = jsonObject.getJSONObject("page");
                if (page != null) {
                    context.output(pageTag, jsonObject);
                }
            }
        });

        Map<String, DataStream<JSONObject>> streams = new HashMap<>();

        // 每个侧输出流有一个 Tag 标记该侧输出流的信息
        streams.put(START, startStream);
        streams.put(DISPLAY, startStream.getSideOutput(displayTag));
        streams.put(ERR, startStream.getSideOutput(errTag));
        streams.put(PAGE, startStream.getSideOutput(pageTag));
        streams.put(ACTION, startStream.getSideOutput(actionTag));

        return streams;
    }

    private SingleOutputStreamOperator<JSONObject> validateNewOrOld(SingleOutputStreamOperator<JSONObject> etlStream) {
        return etlStream.keyBy(obj -> obj.getJSONObject("common").getString("mid"))
                .process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    private ValueState<String> firstVisitDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        firstVisitDateState = getRuntimeContext().getState(
                                new ValueStateDescriptor<String>("firstVisitDate", String.class));
                    }

                    @Override
                    public void processElement(JSONObject jsonObject,
                                               KeyedProcessFunction<String, JSONObject, JSONObject>.Context context,
                                               Collector<JSONObject> collector) throws Exception {
                        JSONObject common = jsonObject.getJSONObject("common");
                        String isNew = common.getString("is_new");
                        Long ts = jsonObject.getLong("ts");
                        String today = DateFormatUtil.tsToDate(ts);

                        // 从状态中获取首次访问的日志，用于后续对比，如果和已记录状态不一致则说明需要对其状态进行修正
                        String firstVisitDate = firstVisitDateState.value();
                        if ("1".equals(isNew)) {
                            if (firstVisitDate == null) {
                                // 该设备 mid 的首次访问
                                firstVisitDateState.update(today);
                            } else if (!today.equals(firstVisitDateState)){
                                // 今日和首日访问不一致
                                common.put("is_new", 0); // 将新用户修改为老用户
                            } else {
                                if (firstVisitDate == null) {
                                    // 若一个老用户首次访问日志为 null，则将其首次访问日期设置为昨日
                                    firstVisitDateState.update(DateFormatUtil.tsToDate(ts - 24 * 60 * 1000));
                                }
                            }
                        }
                        collector.collect(jsonObject);
                    }
                });
    }

    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        return stream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                try {
                    JSON.parseObject(s);
                    return true;
                } catch (Exception e) {
                    log.error("日志格式不是正确的 JSON 格式：" + s);
                    return false;
                }
            }
        }).map(JSON::parseObject);
    }
}
