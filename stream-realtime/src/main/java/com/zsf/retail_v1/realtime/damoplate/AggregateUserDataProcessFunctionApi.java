package com.zsf.retail_v1.realtime.damoplate;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * @Package com.zsf.retail_v1.realtime.damoplate.AggregateUserDataProcessFunctionApi
 * @Author zhao.shuai.fei
 * @Date 2025/5/14 18:56
 * @description:
 */
public class AggregateUserDataProcessFunctionApi extends KeyedProcessFunction<String, JSONObject,JSONObject> {
    private transient ValueState<Long> pvState;
    private transient MapState<String, Set<String>> fieldsState;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 初始化PV状态
        pvState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("pv-state", Long.class)
        );

        // 初始化字段集合状态（使用TypeHint保留泛型信息）
        MapStateDescriptor<String, Set<String>> fieldsDescriptor =
                new MapStateDescriptor<>(
                        "fields-state",
                        Types.STRING,
                        TypeInformation.of(new TypeHint<Set<String>>() {})
                );

        fieldsState = getRuntimeContext().getMapState(fieldsDescriptor);
    }
    @Override
    public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {
        // 更新PV
        Long pv = pvState.value() == null ? 1L : pvState.value() + 1;
        pvState.update(pv);

        // 提取设备信息和搜索词
        String uid = value.getString("uid");
        String create_time = value.getString("create_time");
        String total_amount = value.getString("total_amount");
        String order_id = value.getString("order_id");
        String ts_ms = value.getString("ts_ms");


        // 更新字段集合
        updateField("uid", uid);
        updateField("create_time", create_time);
        updateField("total_amount", total_amount);
        updateField("order_id", order_id);
        updateField("ts_ms", ts_ms);

        // 构建输出JSON
        JSONObject output = new JSONObject();
        output.put("uid", value.getString("uid"));
        output.put("pv", pv);
        output.put("create_time", String.join(",", getField("create_time")));
        output.put("total_amount", String.join(",", getField("total_amount")));
        output.put("order_id", String.join(",", getField("order_id")));
        output.put("ts_ms", String.join(",", getField("ts_ms")));

        out.collect(output);

    }
    private void updateField(String field, String value) throws Exception {
        Set<String> set = fieldsState.get(field) == null ? new HashSet<>() : fieldsState.get(field);
        set.add(value);
        fieldsState.put(field, set);
    }

    // 辅助方法：获取字段集合
    private Set<String> getField(String field) throws Exception {
        return fieldsState.get(field) == null ? Collections.emptySet() : fieldsState.get(field);
    }
}
