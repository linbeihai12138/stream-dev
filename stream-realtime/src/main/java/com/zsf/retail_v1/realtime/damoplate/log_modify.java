package com.zsf.retail_v1.realtime.damoplate;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.zsf.realtime.common.util.KafkaUtil;
import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.security.SecureRandom;

/**
 * @Package com.zsf.realtime.common.damoplate.log_modify
 * @Author zhao.shuai.fei
 * @Date 2025/5/12 10:03
 * @description: 对日志数据进行修改
 */
public class log_modify {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        //读取kafka数据
        DataStreamSource<String> kafkaSource = KafkaUtil.getKafkaSource(env, "topic_log_001", "groupId");

        SingleOutputStreamOperator<JSONObject> logJsonDB = kafkaSource.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String s) throws Exception {
                boolean b = JSON.isValid(s);
                if (b){
                    JSONObject jsonObject = JSON.parseObject(s);
                    JSONObject common = jsonObject.getJSONObject("common");
                    if(common!=null && common.getString("uid")!=null){
                        SecureRandom secureRandom = new SecureRandom();
                        int randomNum = secureRandom.nextInt(10) + 1;
                        common.put("uid",randomNum);
                    }
                    return jsonObject;
                }
                return null;
            }
        }).uid("kafka_log_map_logJsonDB").name("kafka_log_map_logJsonDB");

        SingleOutputStreamOperator<String> map = logJsonDB.map(js -> js.toJSONString());
        map.addSink(KafkaUtil.getKafkaSink("topic_log_002"));

        env.execute();
    }
}
