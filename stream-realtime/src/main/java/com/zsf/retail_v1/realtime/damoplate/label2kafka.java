package com.zsf.retail_v1.realtime.damoplate;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.text.DecimalFormat;
import java.time.LocalDate;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.zsf.realtime.common.bean.DimBaseCategory;
import com.zsf.realtime.common.bean.DimBaseCategory2;
import com.zsf.realtime.common.constant.Constant;
import com.zsf.realtime.common.func.FilterBloomDeduplicatorFunc;
import com.zsf.realtime.common.util.KafkaUtil;
import com.zsf.realtime.common.utils.JdbcUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import java.time.Period;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @Package com.zsf.retail_v1.realtime.damoplate.userlabel2kafka
 * @Author zhao.shuai.fei
 * @Date 2025/5/12 11:17
 * @description:
 */
public class label2kafka {
    private static final List<DimBaseCategory> dim_base_categories;
    private static final List<DimBaseCategory2> dim_base_categories2;
    private static final Connection connection;
    private static final double device_rate_weight_coefficient = 0.1; // 设备权重系数
    private static final double search_rate_weight_coefficient = 0.15; // 搜索权重系数

    static {
        try {
            connection = JdbcUtils.getMySQLConnection(
                    Constant.MYSQL_URL,
                    Constant.MYSQL_USER_NAME,
                    Constant.MYSQL_PASSWORD);
            String sql = "select b3.id,                          \n" +
                    "            b3.name as b3name,              \n" +
                    "            b2.name as b2name,              \n" +
                    "            b1.name as b1name               \n" +
                    "     from sx_004.base_category3 as b3  \n" +
                    "     join sx_004.base_category2 as b2  \n" +
                    "     on b3.category2_id = b2.id             \n" +
                    "     join sx_004.base_category1 as b1  \n" +
                    "     on b2.category1_id = b1.id";
            dim_base_categories = JdbcUtils.queryList2(connection, sql, DimBaseCategory.class, false);

            String sql2 = "select\n" +
                    "    ki.id as id,\n" +
                    "    kpd.base_category_name bcname,\n" +
                    "    kpd.base_trademark_name as btname\n" +
                    "from sx_004.sku_info ki\n" +
                    "left join sx_003.hbase_kpb kpd\n" +
                    "on ki.category3_id=kpd.base_category_id;";
            dim_base_categories2 = JdbcUtils.queryList2(connection, sql2, DimBaseCategory2.class, false);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
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


        // 获取用户数据 对字段进行筛选
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

        // 获取用户详情数据 对字段进行筛选
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


        // user_info join user_info_sup_msg 对user_info 字段进行补充
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

        // 对日志数据中 字段进行 筛选： 设备信息 + 关键词搜索
        SingleOutputStreamOperator<JSONObject> logPageJsonDs = logJsonDs.map(new MapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                JSONObject result = new JSONObject();
                if (jsonObject.containsKey("common")){
                    JSONObject common = jsonObject.getJSONObject("common");
                    result.put("uid",common.getString("uid") != null ? common.getString("uid") : "-1");
                    result.put("ts",jsonObject.getLongValue("ts"));
                    JSONObject deviceInfo = new JSONObject();
                    common.remove("sid");
                    common.remove("mid");
                    common.remove("is_new");
                    deviceInfo.putAll(common);
                    result.put("deviceInfo",deviceInfo);
                    if(jsonObject.containsKey("page") && !jsonObject.getJSONObject("page").isEmpty()){
                        JSONObject pageInfo = jsonObject.getJSONObject("page");
                        if (pageInfo.containsKey("item_type") && pageInfo.getString("item_type").equals("keyword")){
                            String item = pageInfo.getString("item");
                            result.put("search_item",item);
                        }
                    }
                }
                JSONObject deviceInfo = result.getJSONObject("deviceInfo");
                String os = deviceInfo.getString("os").split(" ")[0];
                deviceInfo.put("os",os);

                return result;
            }
        });

        //  对日志数据进行分组
        KeyedStream<JSONObject, String> logPageKeyByJsonDs = logPageJsonDs.keyBy(json -> json.getString("uid"));

//         通过ProcessFunction进行数据去重 或者通过布隆过滤器也可以实现
        SingleOutputStreamOperator<JSONObject> logPageDuplicateJsonDs = logPageKeyByJsonDs.process(new ProcessFilterRepeatTsDataFunc());


//         1 min 分钟窗口 实现了窗口内数据的去重，只保留最新状态
        SingleOutputStreamOperator<JSONObject> LogPageReduceDuplicateJsonDs = logPageDuplicateJsonDs.keyBy(json -> json.getString("uid"))
                .process(new AggregateUserDataProcessFunction())
                .keyBy(json -> json.getString("uid"))
                .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
                .reduce((value1, value2) -> value2);


        // 设备打分模型
        SingleOutputStreamOperator<JSONObject> logs = LogPageReduceDuplicateJsonDs.map(new MapDeviceAndSearchMarkModelFunc(dim_base_categories, device_rate_weight_coefficient, search_rate_weight_coefficient));


        // 订单数据
        SingleOutputStreamOperator<JSONObject> orderInfoDs = kafkaJson
                .filter(json -> json.getJSONObject("source").getString("table").equals("order_info"));

        SingleOutputStreamOperator<JSONObject> orderInfoUpdDs = orderInfoDs.map(new MapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                String op = jsonObject.getString("op");
                JSONObject json = new JSONObject();
                if (!op.equals("d")) {
                    JSONObject after = jsonObject.getJSONObject("after");
                    json.put("op", op);
                    json.put("order_id", after.getString("id"));
                    json.put("create_time", after.getString("create_time"));
                    json.put("total_amount", after.getString("total_amount"));
                    json.put("uid", after.getString("user_id"));
                    json.put("ts_ms", jsonObject.getString("ts_ms"));
                    return json;

                }
                return null;
            }
        });

        SingleOutputStreamOperator<JSONObject> orderDetailDs = kafkaJson
                .filter(json -> json.getJSONObject("source").getString("table").equals("order_detail"));

        SingleOutputStreamOperator<JSONObject> orderDetailUpdDs = orderDetailDs.map(new MapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                String op = jsonObject.getString("op");
                JSONObject json = new JSONObject();
                if (!op.equals("d")) {
                    JSONObject after = jsonObject.getJSONObject("after");
                    json.put("op", op);
                    json.put("order_id", after.getString("order_id"));
                    json.put("ts_ms", jsonObject.getString("ts_ms"));
                    json.put("sku_id", after.getString("sku_id"));
                    return json;

                }
                return null;
            }
        });

        SingleOutputStreamOperator<JSONObject> orderDetail2 = orderInfoUpdDs
                .keyBy(o->o.getString("order_id"))
                .intervalJoin(orderDetailUpdDs.keyBy(o->o.getString("order_id")))
                .between(Time.minutes(-30), Time.minutes(30))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject jsonObject1, JSONObject jsonObject2, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        jsonObject1.put("sku_id",jsonObject2.getString("sku_id"));

                        collector.collect(jsonObject1);
                    }
                });

        //通过布隆过滤器，去掉重复数据
        SingleOutputStreamOperator<JSONObject> orderDetail1 = orderDetail2.keyBy(data -> data.getLong("order_id"))
                .filter(new FilterBloomDeduplicatorFunc(1000000, 0.01));


        SingleOutputStreamOperator<JSONObject> orderInfos = orderDetail1.map(new MapDeviceAndSearchMarkModelFuncApi(dim_base_categories2, device_rate_weight_coefficient, search_rate_weight_coefficient));

//        orderInfos.print();
//        logs.print();
//        userInfo.print();

        SingleOutputStreamOperator<JSONObject> user = userInfo
                .keyBy(o->o.getString("id"))
                .intervalJoin(orderInfos.keyBy(o->o.getString("uid")))
                .between(Time.minutes(-30), Time.minutes(30))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject jsonObject1, JSONObject jsonObject2, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        jsonObject1.put("sum_25~29",jsonObject2.getString("sum_25~29"));
                        jsonObject1.put("sum_40~49",jsonObject2.getString("sum_40~49"));
                        jsonObject1.put("sum_50",jsonObject2.getString("sum_50"));
                        jsonObject1.put("sum_18~24",jsonObject2.getString("sum_18~24"));
                        jsonObject1.put("sum_35~39",jsonObject2.getString("sum_35~39"));
                        jsonObject1.put("sum_30~34",jsonObject2.getString("sum_30~34"));

                        collector.collect(jsonObject1);
                    }
                });


        SingleOutputStreamOperator<JSONObject> logs1 = logs.map(new MapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                String uid = jsonObject.getString("uid");
                jsonObject.put("uid", uid.substring(0, 1));
                jsonObject.put("ts_ms", jsonObject.getString("ts"));
                return jsonObject;
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject jsonObject, long l) {
                return jsonObject.getLongValue("ts_ms");
            }
        }));

        SingleOutputStreamOperator<JSONObject> user2 = user
                .keyBy(o->o.getString("id"))
                .intervalJoin(logs1.keyBy(o->o.getString("uid")))
                .between(Time.hours(-12), Time.hours(12))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject jsonObject1, JSONObject jsonObject2, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        jsonObject1.put("sum_25~29",jsonObject1.getDoubleValue("sum_25~29")+ jsonObject2.getDoubleValue("search_18_24") + jsonObject2.getDoubleValue("device_18_24"));
                        jsonObject1.put("sum_40~49",jsonObject1.getDoubleValue("sum_40~49")+ jsonObject2.getDoubleValue("search_40_49") + jsonObject2.getDoubleValue("device_40_49"));
                        jsonObject1.put("sum_50",jsonObject1.getDoubleValue("sum_50") + jsonObject2.getDoubleValue("search_50") + jsonObject2.getDoubleValue("device_50"));
                        jsonObject1.put("sum_18~24",jsonObject1.getDoubleValue("sum_18~24") + jsonObject2.getDoubleValue("device_18_24") + + jsonObject2.getDoubleValue("search_18_24"));
                        jsonObject1.put("sum_35~39",jsonObject1.getDoubleValue("sum_35~39") + jsonObject2.getDoubleValue("search_35_39") + jsonObject2.getDoubleValue("device_35_39"));
                        jsonObject1.put("sum_30~34",jsonObject1.getDoubleValue("sum_30~34") + jsonObject2.getDoubleValue("search_30_34") + jsonObject2.getDoubleValue("device_30_34"));
                        collector.collect(jsonObject1);
                    }
                });
//        user2.print();

        user2.map(new MapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                long sum_18_24 = jsonObject.getLongValue("sum_18~24");
                long sum_35_39 = jsonObject.getLongValue("sum_35~39");
                long sum_25_29 = jsonObject.getLongValue("sum_25~29");
                long sum_40_49 = jsonObject.getLongValue("sum_40~49");
                long sum_50 = jsonObject.getLongValue("sum_50");
                long sum_30_34 = jsonObject.getLongValue("sum_30~34");

                double sum18_24 = roundToPrecision(sum_18_24, 3);
                double sum35_39 = roundToPrecision(sum_35_39, 3);
                double sum25_29 = roundToPrecision(sum_25_29, 3);
                double sum40_49 = roundToPrecision(sum_40_49, 3);
                double sum50 = roundToPrecision(sum_50, 3);
                double sum30_34 = roundToPrecision(sum_30_34, 3);

                ArrayList<String> list = new ArrayList<>();
                list.add("18-24");
                list.add("25-29");
                list.add("30-34");
                list.add("35-39");
                list.add("40-49");
                list.add("50以上");

                ArrayList<Double> list2 = new ArrayList<>();
                list2.add(sum18_24);
                list2.add(sum25_29);
                list2.add(sum30_34);
                list2.add(sum35_39);
                list2.add(sum40_49);
                list2.add(sum50);

                double maxValue2 = Collections.max(list2);

                int index2 = list2.indexOf(maxValue2);

                jsonObject.put("max_age", list.get(index2));
                jsonObject.remove("sum_18~24");
                jsonObject.remove("sum_35~39");
                jsonObject.remove("sum_25~29");
                jsonObject.remove("sum_40~49");
                jsonObject.remove("sum_50");
                jsonObject.remove("sum_30~34");

                return jsonObject;
            }
        }).print();
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
    public static double roundToPrecision(double num, int precision) {
        if (precision < 0) {
            throw new IllegalArgumentException("精度不能为负数");
        }

        // 使用 Double.toString 避免浮点数精度问题
        BigDecimal bd = new BigDecimal(Double.toString(num));
        // 四舍五入到指定小数位数
        bd = bd.setScale(precision, RoundingMode.HALF_UP);
        return bd.doubleValue();
    }
}
