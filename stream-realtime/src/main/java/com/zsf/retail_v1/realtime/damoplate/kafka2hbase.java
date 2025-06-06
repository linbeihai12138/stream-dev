package com.zsf.retail_v1.realtime.damoplate;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.zsf.realtime.common.constant.Constant;
import com.zsf.realtime.common.util.HbaseUtils;
import com.zsf.realtime.common.util.KafkaUtil;
import com.zsf.retail_v1.realtime.dim.TableProcessFunction;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;

import java.util.*;

/**
 * @Package com.zsf.retail_v1.realtime.damoplate.kafka2hbase
 * @Author zhao.shuai.fei
 * @Date 2025/5/12 13:45
 * @description: 将维度数据写入到kafka
 */
public class kafka2hbase {
    private static void deleteNotNeedColumns(JSONObject dataJsonObj, String sinkColumns) {
        List<String> columnList = Arrays.asList(sinkColumns.split(","));

        Set<Map.Entry<String, Object>> entrySet = dataJsonObj.entrySet();

        entrySet.removeIf(entry-> !columnList.contains(entry.getKey()));

    }
    private static SingleOutputStreamOperator<JSONObject> createHBaseTable(SingleOutputStreamOperator<JSONObject> tpDS){
        tpDS = tpDS.map(new MapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject tp) throws Exception {
                HbaseUtils hbaseUtils = new HbaseUtils("cdh01,cdh02,cdh03");

                if (hbaseUtils.isConnect()) {
                    System.out.println("HBase 连接正常");
                } else {
                    System.out.println("HBase 连接失败");
                }

                //获取配置表：操作类型
                String op = tp.getString("op");

                //获取配置表：hbase维度表表名
                String sinkTable = tp.getString("sink_table");
                //获取配置表：hbase维度表表中列祖
                String[] sinkFamily = tp.getString("sink_family").split(",");

                if ("d".equals(op)){
                    //从配置表：删除一条数据，hbase将对应的表删除
                    boolean tableDeleted = hbaseUtils.deleteTable(sinkTable);

                } else if("r".equals(op)||"c".equals(op)){
                    //从配置表：添加一条数据，hbase将对应的表添加
                    boolean tableCreated = hbaseUtils.createTable("sx_003", sinkTable, sinkFamily);

                }else {
                    //从配置表：修改一条数据，hbase将对应的表修改：先删除后添加
                    boolean tableCreated = hbaseUtils.createTable("sx_003", sinkTable, sinkFamily);
                    boolean tableDeleted = hbaseUtils.deleteTable(sinkTable);

                }
                return tp;
            }
        }).setParallelism(1);
        return tpDS;
    }
    @SneakyThrows
    public static void main(String[] args) {
        // 环境准备：流处理，
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 并行度，
        env.setParallelism(4);

        // 业务数据
        DataStreamSource<String> dbSource = KafkaUtil.getKafkaSource(env, "topic_db_001", "groupId001");

        SingleOutputStreamOperator<JSONObject> dbJsonDS = dbSource.map(JSONObject::parseObject);
        // 读取维度表信息
        Properties properties = new Properties();
        properties.setProperty("decimal.handling.mode","double");
        properties.setProperty("time.precision.mode","connect");
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("cdh03")
                .port(3306)
                .databaseList("sx_004_v2") // 设置捕获的数据库， 如果需要同步整个数据库，请将 tableList 设置为 ".*".
                .tableList("sx_004_v2.*") // 设置捕获的表
                .username("root")
                .password("root")
                .debeziumProperties(properties)
                .startupOptions(StartupOptions.earliest()) //全量
//                .startupOptions(StartupOptions.latest()) //增量
                .deserializer(new JsonDebeziumDeserializationSchema()) // 将 SourceRecord 转换为 JSON 字符串
                .build();

        DataStreamSource<String> dbDimStrDS = env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");

        //维度信息过滤
        SingleOutputStreamOperator<JSONObject> dbDimTabDS = dbDimStrDS.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String s) throws Exception {
                JSONObject jsonObj = JSONObject.parseObject(s);
                String op = jsonObj.getString("op");
                JSONObject after = null;
                if ("d".equals(op)) {
                    //如果是删除获取删除前数据before
                    after = jsonObj.getObject("before", JSONObject.class);
                } else {
                    //非删除获取更新后的数据after
                    after = jsonObj.getObject("after", JSONObject.class);
                }

                after.put("op",op);
                return after;
            }
        });

//        调用方法 把维度表信息写入到 hbase
//        createHBaseTable(dbDimTabDS);

        MapStateDescriptor<String, JSONObject> mapStateDescriptor = new MapStateDescriptor<>("state", String.class, JSONObject.class);

        BroadcastStream<JSONObject> broadcast = dbDimTabDS.broadcast(mapStateDescriptor);
        //主流connect广播流
        BroadcastConnectedStream<JSONObject, JSONObject> connect = dbJsonDS.connect(broadcast);

        SingleOutputStreamOperator<Tuple2<JSONObject, JSONObject>> dimDS = connect.process(new TableProcessFunction(mapStateDescriptor));

        dimDS.addSink(new SinkFunction<Tuple2<JSONObject, JSONObject>>() {
            @Override
            public void invoke(Tuple2<JSONObject, JSONObject> value, Context context) throws Exception {
                JSONObject jsonObj = value.f0;
                JSONObject jsonObject = value.f1;
                String type = jsonObj.getString("type");
                jsonObj.remove("type");

                //获取操作的HBase表的表名
                String sinkTable = jsonObject.getString("sinkTable");
                if (sinkTable==null){
                    sinkTable = jsonObject.getString("sink_table");
                }

                //获取rowkey
                String rowKey = jsonObject.getString("sinkRowKey");
                if (rowKey==null){
                    rowKey = jsonObject.getString("sink_row_key");
                }

                //判断对业务数据库维度表进行了什么操作
                HbaseUtils hbaseUtils = new HbaseUtils("cdh01,cdh02,cdh03");
                if("d".equals(type)){
                    //从业务数据库维度表中做了删除操作  需要将HBase维度表中对应的记录也删除掉
//                    HbaseUtils.delRow(hbaseConn, Constant.HBASE_NAMESPACE,sinkTable,rowKey);
                }else{
                    //如果不是delete，可能的类型有insert、update、bootstrap-insert，上述操作对应的都是向HBase表中put数据
                    String sinkFamily = jsonObject.getString("sinkFamily");
//                    HbaseUtils.putRow(hbaseConn,Constant.HBASE_NAMESPACE,sinkTable,rowKey,sinkFamily,jsonObj);
                    // 2. 配置要写入的表名、行键和数据
                    String tableName = "sx_003:"+sinkTable;

                    // 3. 创建 BufferedMutator，用于批量写入数据
                    BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf(tableName));
                    BufferedMutator mutator = hbaseUtils.getConnection().getBufferedMutator(params);

                    // 4. 调用 HbaseUtils 的 put 方法写入数据
                    HbaseUtils.put(rowKey, jsonObj, mutator);

                    // 5. 刷新缓冲区并关闭 BufferedMutator
                    mutator.flush();
                    mutator.close();

                    // 6. 关闭 HBase 连接（可选，根据实际情况决定是否关闭）
                    hbaseUtils.getConnection().close();

                    System.out.println("数据已成功写入 HBase 表：" + tableName);
                }

            }
        });


        env.execute();
    }

}
