package com.moving.metric;

import com.moving.pojo.EventPO;
import com.moving.utils.DateUtil;
import com.moving.utils.KafkaUtil;
import com.moving.utils.ParameterUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.time.LocalDateTime;

public class MetricsKafka {
    public static void main(String[] args) throws Exception {
        // 消费 Kafka
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 读取配置文件，将 flink 的参数配置为全局
        ParameterTool parameters = ParameterUtil.getParameters(args);
        env.getConfig().setGlobalJobParameters(parameters);

        // 将 Kafka 数据转换为 EventPO 对象
        DataStream<EventPO> eventStream = KafkaUtil.read(parameters);

        // 乱序数据 提取时间字段
        SerializableTimestampAssigner<EventPO> serializableTimestampAssigner = new SerializableTimestampAssigner<EventPO>() {
            @Override
            public long extractTimestamp(EventPO pojo, long l) {
                // String -> LocalDateTime
                LocalDateTime localDateTime = DateUtil.convertStr2Datetime(pojo.getEvent_time());
                // LocalDateTime -> Timestamp
                return DateUtil.convertLocalDateTime2Timestamp(localDateTime);
            }
        };

        // 设置水位线
        /*
        * forBoundedOutOfOrderness(): 允许乱序数据（一定范围内）；周期性生成 Watermark
        * forMonotonousTimestamps(): 单调递增的策略，要求数据按升序到达
        * noWatermark()：不生成 watermark
        *
        * */
        SingleOutputStreamOperator<EventPO> streamWithWatermark = eventStream.assignTimestampsAndWatermarks(WatermarkStrategy.<EventPO>forBoundedOutOfOrderness(Duration.ofMillis(1000L))
                .withTimestampAssigner(serializableTimestampAssigner));
        // 可以直接写成 .withTimestampAssigner((obj, ts) -> DateUtil.convertStr2Timestamp(DateUtil.convertStr2Datetime(obj.getEvent_time())))

        // 每五分钟计算用户近一小时的登录次数 (flume 采集数据后发送到 Kafka 存在小时级别的延迟)
//        SingleOutputStreamOperator<EventPO> filterStream = streamWithWatermark.filter(new FilterFunction<EventPO>() {
//            @Override
//            public boolean filter(EventPO eventPO) throws Exception {
//                if (eventPO.getEvent_name().equals("LOGIN_SUCCESS")) {
//                    return true;
//                }
//                return false;
//            }
//        });
        SingleOutputStreamOperator<EventPO> filterStream = streamWithWatermark.filter(obj -> obj.getEvent_name().equals("LOGIN_SUCCESS"));

        /* **********************
         *开窗的逻辑:滑动窗口
         * 窗口的大小 每五分钟计算用户近一小时的登录次数   1小时
         * 窗口的滑动步长  每五分钟计算一次
         * 窗口是左闭右开的
         * [ 6 - 7 )  [ 6.05 - 7.05 )   [ 6.10 - 7.10 )
         *
         * 两种窗口：计数窗口、时间窗口
         * CountWindow 和时间没有关系  根据数据的数量 来生成窗口
         * TimeWindow  时间窗口  根据时间来生成窗口
         * 滚动窗口   滑动窗口   会话窗口  都是属于 TimeWindow
         * SlidingEventTimeWindows 生成滑动窗口
         * TumblingEventTimeWindows  生成滚动窗口
         *
         * flink 针对于 上述两种窗口  提供了 四种聚合函数  （window function）
         * 1、 ReduceFunction
         * 2、 AggregateFunction
         * 3、 ProcessWindowFunction
         * 4、 sum、 min、 max
         *
         * 上述四种聚合函数 又分两类
         * 1、增量聚合函数 ReduceFunction、AggregateFunction
         *  性能高、不需要缓存数据，基于算子的中间状态进行计算
         * 2、全量聚合函数 ProcessWindowFunction、sum、 min、 max
         * 性能低、需要缓存数据，都是基于进入窗口的全部数据进行计算
         * 场景 全局TOP N
         *
         * ReduceFunction、AggregateFunction 结合ProcessWindowFunction 做增量计算
         * ProcessWindowFunction  中可以补充窗口信息  窗口的开始  结束  等
         *
         * Time的导包import org.apache.flink.streaming.api.windowing.time.Time;
         * *********************/

        // 可以单独封装成一个方法
        SingleOutputStreamOperator<Tuple2<Integer, Integer>> result = filterStream.keyBy(obj -> obj.getUser_id_int())
                // 每五分钟统计近一小时的数据，采用滑动窗口
                .window(SlidingEventTimeWindows.of(Time.hours(1L), Time.minutes(5L)))
                .aggregate(new AggregateFunction<EventPO, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> createAccumulator() {
                        return new Tuple2<>(0, 0);
                    }

                    @Override
                    public Tuple2<Integer, Integer> add(EventPO eventPO, Tuple2<Integer, Integer> tuple) {
                        tuple.f0 = eventPO.getUser_id_int();
                        tuple.f1 += 1;
                        return tuple;
                    }

                    @Override
                    public Tuple2<Integer, Integer> getResult(Tuple2<Integer, Integer> tuple) {
                        return tuple;
                    }

                    @Override
                    public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> integerIntegerTuple2, Tuple2<Integer, Integer> acc1) {
                        return null;
                    }
                });

        env.execute();
    }
}
