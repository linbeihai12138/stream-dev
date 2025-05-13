package com.zsf.retail_v1.realtime.damoplate;
import java.sql.Timestamp;
import java.time.LocalDate;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.zsf.realtime.common.constant.Constant;
import com.zsf.realtime.common.func.FilterBloomDeduplicatorFunc;
import com.zsf.realtime.common.util.KafkaUtil;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * @Package com.zsf.retail_v1.realtime.damoplate.userlabel2kafka
 * @Author zhao.shuai.fei
 * @Date 2025/5/12 11:17
 * @description:
 */
public class label2kafka {

    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStreamSource<String> kafkaSource = KafkaUtil.getKafkaSource(env, "topic_db_001", "label2kafka");

        SingleOutputStreamOperator<JSONObject> kafkaJson = kafkaSource
                .map(JSON::parseObject)
                .filter(data -> !data.isEmpty())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject jsonObject, long l) {
                        return jsonObject.getLong("ts_ms");
                    }
                }));


        // 获取用户数据
        SingleOutputStreamOperator<JSONObject> dbJsonDS1 = kafkaJson
                .filter(json->json.getJSONObject("source").getString("table").equals("user_info"))
                .map(new MapFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject map(JSONObject jsonObject) throws Exception {

                        JSONObject result = new JSONObject();
                        if (jsonObject.containsKey("after") && jsonObject.getJSONObject("after") != null) {
                            JSONObject after = jsonObject.getJSONObject("after");
                            result.put("id", after.getString("id"));
                            result.put("uname", after.getString("name"));
                            result.put("user_level", after.getString("user_level"));
                            result.put("login_name", after.getString("login_name"));
                            result.put("phone_num", after.getString("phone_num"));
                            result.put("email", after.getString("email"));
                            result.put("gender", after.getString("gender") != null ? after.getString("gender") : "home");
                            result.put("birthday", after.getString("birthday"));
                            result.put("ts_ms", jsonObject.getLongValue("ts_ms"));
                            Integer birthdayStr = after.getInteger("birthday");
                            if (birthdayStr != null) {
                                try {
                                    LocalDate date = LocalDate.ofEpochDay(birthdayStr);
                                    result.put("birthday", date.format(DateTimeFormatter.ISO_DATE));
                                    String birthdayStr1 = result.getString("birthday");
                                    String substring = birthdayStr1.substring(0,3);
                                    // 获取年代
                                    result.put("decade", substring + "0");

                                    // 获取年龄
                                    LocalDate birthday = LocalDate.parse(birthdayStr1, DateTimeFormatter.ISO_DATE);
                                    LocalDate currentDate = LocalDate.now(ZoneId.of("Asia/Shanghai"));

                                    // 获取性别
                                    int age = calculateAge(birthday, currentDate);
                                    result.put("age", age);
                                    String zodiac = getZodiacSign(birthday);
                                    result.put("zodiac_sign", zodiac);
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }
                        }

                        return result;
                    }
                });

        // 获取用户详情数据
        SingleOutputStreamOperator<JSONObject> dbJsonDS2 = kafkaJson
                .filter(json->json.getJSONObject("source").getString("table").equals("user_info_sup_msg"))
                .map(new MapFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject map(JSONObject jsonObject) throws Exception {
                        JSONObject result = new JSONObject();
                        if (jsonObject.containsKey("after") && jsonObject.getJSONObject("after") != null) {
                            JSONObject after = jsonObject.getJSONObject("after");
                            result.put("uid", after.getString("uid"));
                            result.put("unit_height", after.getString("unit_height"));
                            result.put("create_ts", after.getLong("create_ts"));
                            result.put("weight", after.getString("weight"));
                            result.put("unit_weight", after.getString("unit_weight"));
                            result.put("height", after.getString("height"));
                            result.put("ts_ms", jsonObject.getLong("ts_ms"));
                            return result;
                        }
                        return null;
                    }
                });


        // intervalJoin
        SingleOutputStreamOperator<JSONObject> userInfo = dbJsonDS1
                .keyBy(o->o.getString("id"))
                .intervalJoin(dbJsonDS2.keyBy(o->o.getString("uid")))
                .between(Time.minutes(-30), Time.minutes(30))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject jsonObject1, JSONObject jsonObject2, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        JSONObject result = new JSONObject();
                        if (jsonObject1.getString("id").equals(jsonObject2.getString("uid"))){
                            result.putAll(jsonObject1);
                            result.put("height",jsonObject2.getString("height"));
                            result.put("unit_height",jsonObject2.getString("unit_height"));
                            result.put("weight",jsonObject2.getString("weight"));
                            result.put("unit_weight",jsonObject2.getString("unit_weight"));
                        }
                        collector.collect(result);

                    }
                });

        // 读取日志数据
        DataStreamSource<String> logSource = KafkaUtil.getKafkaSource(env, "topic_log_001", "groupId002");

        SingleOutputStreamOperator<JSONObject> logJsonDs = logSource.map(JSON::parseObject)
                .filter(json -> json.containsKey("page") && !json.getString("page").isEmpty())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject jsonObject, long l) {
                        return jsonObject.getLongValue("ts");
                    }
                }));

        SingleOutputStreamOperator<JSONObject> logPageJsonDs = logJsonDs.map(new MapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                JSONObject json = new JSONObject();
                JSONObject page = jsonObject.getJSONObject("page");
                JSONObject common = jsonObject.getJSONObject("common");
                json.put("uid", common.getString("uid") == null ? "-1" : common.getString("uid"));
                json.put("os", common.getString("os").split(" ")[0]);
                json.put("ts", jsonObject.getLongValue("ts"));
                json.put("item", page.getString("item"));
                json.put("item_type", page.getString("item_type"));
                return json;
            }
        });
        // 对日志数据进行去重
        SingleOutputStreamOperator<JSONObject> logPageDuplicateDs = logPageJsonDs.keyBy(json -> json.getString("uid"))
                .process(new LatestLogDeduplicator());

        SingleOutputStreamOperator<JSONObject> userInfoLog = userInfo
                .keyBy(o->o.getString("id"))
                .intervalJoin(logPageDuplicateDs.keyBy(o->o.getString("uid")))
                .between(Time.days(-1), Time.days(1))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject jsonObject1, JSONObject jsonObject2, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        JSONObject result = new JSONObject();
                        if (jsonObject1.getString("id").equals(jsonObject2.getString("uid"))){
                            result.putAll(jsonObject1);
                            result.put("os",jsonObject2.getString("os"));
                            result.put("item",jsonObject2.getString("item"));
                        }
                        collector.collect(result);

                    }
                });

        // 读取订单数据
        SingleOutputStreamOperator<JSONObject> orderInfoJsonDs = kafkaJson
                .filter(json -> json.getJSONObject("source").getString("table").equals("order_info"))
                .map(new MapFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject map(JSONObject jsonObject) throws Exception {
                        JSONObject result = new JSONObject();
                        if (jsonObject.containsKey("after") && jsonObject.getJSONObject("after") != null) {
                            JSONObject after = jsonObject.getJSONObject("after");
                            result.put("order_id", after.getString("id"));
                            result.put("total_amount", after.getString("total_amount"));
                            result.put("uid", after.getString("user_id"));
                        }
                        return result;
                    }
                });
        orderInfoJsonDs.print();
        env.execute();

    }
    private static int calculateAge(LocalDate birthDate, LocalDate currentDate) {
        return Period.between(birthDate, currentDate).getYears();
    }
    private static String getZodiacSign(LocalDate birthDate) {
        int month = birthDate.getMonthValue();
        int day = birthDate.getDayOfMonth();

        // 星座日期范围定义
        if ((month == 12 && day >= 22) || (month == 1 && day <= 19)) return "摩羯座";
        else if (month == 1 || month == 2 && day <= 18) return "水瓶座";
        else if (month == 2 || month == 3 && day <= 20) return "双鱼座";
        else if (month == 3 || month == 4 && day <= 19) return "白羊座";
        else if (month == 4 || month == 5 && day <= 20) return "金牛座";
        else if (month == 5 || month == 6 && day <= 21) return "双子座";
        else if (month == 6 || month == 7 && day <= 22) return "巨蟹座";
        else if (month == 7 || month == 8 && day <= 22) return "狮子座";
        else if (month == 8 || month == 9 && day <= 22) return "处女座";
        else if (month == 9 || month == 10 && day <= 23) return "天秤座";
        else if (month == 10 || month == 11 && day <= 22) return "天蝎座";
        else return "射手座";
    }
    public static class LatestLogDeduplicator extends KeyedProcessFunction<String, JSONObject, JSONObject> {
        private transient ValueState<JSONObject> latestLogState;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<JSONObject> descriptor = new ValueStateDescriptor<>(
                    "latestLog",
                    JSONObject.class
            );
            latestLogState = getRuntimeContext().getState(descriptor);
        }
        @Override
        public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
            // 从 JSON 中获取长整型时间戳
            long timestampLong = jsonObject.getLong("ts");
            // 手动转换为 Timestamp 对象
            Timestamp currentTimestamp = new Timestamp(timestampLong);
            JSONObject latest = latestLogState.value();
            // 比较时使用已转换的 Timestamp 对象
            if (latest == null || currentTimestamp.after(new Timestamp(latest.getLong("ts")))) {
                latestLogState.update(jsonObject);
                out.collect(jsonObject);
            }
        }
    }
}
