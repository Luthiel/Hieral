package com.moving.metric;

import com.moving.function.UserEventDeserializationSchema;
import com.moving.pojo.Alert;
import com.moving.pojo.UserEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Map;

// 一分钟抓出薅羊毛的用户
/* **********************
数据源格式——kafka
-- 用户登录
{
  "eventType": "login",
  "userId": "112233",
  "loginTime": "2024-01-17T08:00:00Z",
  "ipAddress": "192.168.10.1"
}

-- 领取优惠券
{
"eventType": "couponRedemption",
"userId": "112233",
"redemptionTime": "2024-01-17T08:02:00Z",
"couponCode": "DISCOUNT20",
"ipAddress": "192.168.10.1"
}

-- 快速浏览商品
{
  "eventType": "quickBrowse",
  "userId": "112233",
  "browseTime": "2024-01-17T08:03:00Z",
  "productId": "A123",
  "timeSpent": 10, // 浏览时间（秒）
  "ipAddress": "192.168.10.1"
}

-- 立即下单
{
"eventType": "quickPurchase",
"userId": "112233",
"purchaseTime": "2024-01-17T08:04:00Z",
"productId": "A123",
"quantity": 1,
"totalAmount": 80.00, // 使用优惠券后的价格
"originalPrice": 100.00, // 原价
"couponCode": "DISCOUNT20",
"ipAddress": "192.168.10.1"
}
 *
 * *********************/
// 薅羊毛的用户 -> 用户在短时间内频繁领取优惠券，快速浏览/或未浏览商品并立即下单
public class EcommerceFraudDetection {
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // 其实建议将 KafkaUtil 中获取 KafkaSource 的方法封装为泛型方法
    KafkaSource<UserEvent> kafkaSource =
        KafkaSource.<UserEvent>builder()
            .setBootstrapServers("localhost:9092")
            .setTopics("ecommerce-events")
            .setGroupId("ecommerce-group")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setDeserializer(
                KafkaRecordDeserializationSchema.of(new UserEventDeserializationSchema()))
            .build();

    DataStreamSource<UserEvent> eventStream =
        env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Ecommerce-Events");

    // 薅羊毛路径：登录 -> 领券 -> 下单
    // eventType == login -> eventType == couponRedemption -> eventType == quickPurchase [eventType
    // == quickBrowse]
    // 通过 模式 Pattern 判断
    // 一分钟内领券、购买事件连续，即没有浏览
    Pattern<UserEvent, UserEvent> couponPattern =
        Pattern.<UserEvent>begin("first")
            .where(
                new SimpleCondition<UserEvent>() {
                  @Override
                  public boolean filter(UserEvent userEvent) throws Exception {
                    return userEvent.getType().equals("couponRedemption");
                  }
                })
            .next("second")
            .where(
                new SimpleCondition<UserEvent>() {
                  @Override
                  public boolean filter(UserEvent userEvent) throws Exception {
                    return userEvent.getType().equals("quickPurchase");
                  }
                })
            .within(Time.minutes(1));

    PatternStream<UserEvent> patternStream = CEP.pattern(eventStream.keyBy(UserEvent::getUserId), couponPattern);
    SingleOutputStreamOperator<Alert> result =
        patternStream.select(
            new PatternSelectFunction<UserEvent, Alert>() {
              @Override
              public Alert select(Map<String, List<UserEvent>> map) throws Exception {
                UserEvent first = map.get("first").get(0);
                UserEvent second = map.get("second").get(0);

                // Within 1 minute
                if (first.getUserId().equals(second.getUserId())) {
                  return new Alert(first.getUserId(), "Suspicious behavior detected");
                }
                return new Alert();
              }
            });

    result.print();
    env.execute();
  }
}
