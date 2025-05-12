package com.zsf.retail_v1.realtime.damoplate;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.zsf.realtime.common.util.KafkaUtil;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * @Package com.zsf.retail_v1.realtime.damoplate.dbmysql2kafka
 * @Author zhao.shuai.fei
 * @Date 2025/5/12 11:27
 * @description: 业务数据采集到kafka
 */
public class dbmysql2kafka {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        Properties properties = new Properties();
        properties.setProperty("decimal.handling.mode","double");
        properties.setProperty("time.precision.mode","connect");
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("cdh03")
                .port(3306)
                .databaseList("sx_004") // 设置捕获的数据库， 如果需要同步整个数据库，请将 tableList 设置为 ".*".
                .tableList("sx_004.*") // 设置捕获的表
                .username("root")
                .password("root")
                .debeziumProperties(properties)
                .startupOptions(StartupOptions.earliest()) //全量
//                .startupOptions(StartupOptions.latest()) //增量
                .deserializer(new JsonDebeziumDeserializationSchema()) // 将 SourceRecord 转换为 JSON 字符串
                .build();

        DataStreamSource<String> dbStrDS = env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");

        // 数据清洗
        SingleOutputStreamOperator<String> dbJsonDS = dbStrDS.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                boolean b = JSON.isValid(s);
                if (!b) {
                    return false;
                }
                return JSON.parseObject(s).getString("after") != null;
            }
        });

        dbJsonDS.addSink(KafkaUtil.getKafkaSink("topic_db_001"));

        env.execute();
    }
}
