package com.zsf.retail_v1.realtime.damoplate;

import com.alibaba.fastjson.JSONObject;
import com.zsf.realtime.common.bean.DimBaseCategory2;
import com.zsf.realtime.common.bean.bean.DimCategoryCompare;
import com.zsf.realtime.common.bean.bean.DimCategoryCompare2;
import com.zsf.realtime.common.constant.Constant;
import com.zsf.realtime.common.utils.JdbcUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Package com.zsf.retail_v1.realtime.damoplate.MapDeviceAndSearchMarkModelFuncApi
 * @Author zhao.shuai.fei
 * @Date 2025/5/14 19:16
 * @description:
 */
public class MapDeviceAndSearchMarkModelFuncApi extends RichMapFunction<JSONObject,JSONObject> {
    private final double deviceRate;
    private final double searchRate;
    private final Map<String, DimBaseCategory2> categoryMap;
    private List<DimCategoryCompare2> dimCategoryCompares;
    private Connection connection;
    public MapDeviceAndSearchMarkModelFuncApi(List<DimBaseCategory2> dimBaseCategories, double deviceRate, double searchRate) {
        this.deviceRate = deviceRate;
        this.searchRate = searchRate;
        this.categoryMap = new HashMap<String, DimBaseCategory2>();
        // 将 DimBaseCategory 对象存储到 Map中  加快查询
        for (DimBaseCategory2 category : dimBaseCategories) {
            categoryMap.put(category.getId(), category);
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = JdbcUtils.getMySQLConnection(
                Constant.MYSQL_URL,
                Constant.MYSQL_USER_NAME,
                Constant.MYSQL_PASSWORD);
        String sql = "select order_id ,base_category_name,base_trademark_name from sx_003.cct;";
        dimCategoryCompares = JdbcUtils.queryList2(connection, sql, DimCategoryCompare2.class, true);
        super.open(parameters);
    }

    @Override
    public JSONObject map(JSONObject jsonObject) throws Exception {

        // 映射
        String uId = jsonObject.getString("sku_id");
        if (uId != null && !uId.isEmpty()) {
            // 通过订单ID查找对应的订单分类信息（需替换为实际的订单映射逻辑）
            DimBaseCategory2 category = categoryMap.get(uId);
            if (category != null) {
                // 将订单对应的分类信息存入JSON（例如：商品类别、消费金额等）
                jsonObject.put("btname", category.getBtname());
                jsonObject.put("bcname", category.getBcname());
            }
        }
        String orderId2 = jsonObject.getString("sku_id");
        // 将订单分类映射到搜索类别（或其他目标类别）

        if (orderId2 != null && !orderId2.isEmpty()) {
            // 遍历订单分类映射表（需替换为实际的映射数据结构）
            for (DimCategoryCompare2 mapping : dimCategoryCompares) {
                if (orderId2.equals(mapping.getId())) {
                    // 将订单分类映射为目标类别（如搜索类别、用户画像标签等）
                    jsonObject.put("btname", mapping.getBase_trademark_name());
                    jsonObject.put("bcname", mapping.getBase_category_name());
                    break;
                }
            }
        }

        JSONObject json = new JSONObject();

        double price = jsonObject.getDoubleValue("total_amount");

        if (price<=1000 && price>=0){
            json.put("price_18~24", round(0.8 * 0.15));
            json.put("price_25~29", round(0.6 * 0.15));
            json.put("price_30~34", round(0.4 * 0.15));
            json.put("price_35~39", round(0.3 * 0.15));
            json.put("price_40~49", round(0.2 * 0.15));
            json.put("price_50", round(0.1 * 0.15));
        }else if(price<=4000 && price>1000){
            json.put("price_18~24", round(0.2 * 0.15));
            json.put("price_25~29", round(0.4 * 0.15));
            json.put("price_30~34", round(0.6 * 0.15));
            json.put("price_35~39", round(0.7 * 0.15));
            json.put("price_40~49", round(0.8 * 0.15));
            json.put("price_50", round(0.7 * 0.15));
        }else{
            json.put("price_18~24", round(0.1 * 0.15));
            json.put("price_25~29", round(0.2 * 0.15));
            json.put("price_30~34", round(0.3 * 0.15));
            json.put("price_35~39", round(0.4 * 0.15));
            json.put("price_40~49", round(0.5 * 0.15));
            json.put("price_50", round(0.6 * 0.15));
        }

        String btname = jsonObject.getString("btname");

        switch (btname) {
            case "苹果":
                json.put("search_18~24", round(0.9 * searchRate));
                json.put("search_25~29", round(0.7 * searchRate));
                json.put("search_30~34", round(0.5 * searchRate));
                json.put("search_35~39", round(0.3 * searchRate));
                json.put("search_40~49", round(0.2 * searchRate));
                json.put("search_50", round(0.1    * searchRate));
                break;
            case "香奈儿":
                json.put("search_18~24", round(0.2 * searchRate));
                json.put("search_25~29", round(0.4 * searchRate));
                json.put("search_30~34", round(0.6 * searchRate));
                json.put("search_35~39", round(0.7 * searchRate));
                json.put("search_40~49", round(0.8 * searchRate));
                json.put("search_50", round(0.8    * searchRate));
                break;

            case "家庭与育儿":
                json.put("search_18~24", round(0.1 * searchRate));
                json.put("search_25~29", round(0.2 * searchRate));
                json.put("search_30~34", round(0.4 * searchRate));
                json.put("search_35~39", round(0.6 * searchRate));
                json.put("search_40~49", round(0.8 * searchRate));
                json.put("search_50", round(0.7    * searchRate));
                break;
            case "科技与数码":
                json.put("search_18~24", round(0.8 * searchRate));
                json.put("search_25~29", round(0.6 * searchRate));
                json.put("search_30~34", round(0.4 * searchRate));
                json.put("search_35~39", round(0.3 * searchRate));
                json.put("search_40~49", round(0.2 * searchRate));
                json.put("search_50", round(0.1    * searchRate));
                break;
            default:
                json.put("search_18~24", round(0.4 * searchRate));
                json.put("search_25~29", round(0.5 * searchRate));
                json.put("search_30~34", round(0.6 * searchRate));
                json.put("search_35~39", round(0.7 * searchRate));
                json.put("search_40~49", round(0.8 * searchRate));
                json.put("search_50", round(0.7    * searchRate));
                break;

        }
        String bcname = jsonObject.getString("bcname");

        switch (bcname) {
            case "时尚与潮流":
                json.put("search1_18~24", round(0.9 * searchRate));
                json.put("search1_25~29", round(0.7 * searchRate));
                json.put("search1_30~34", round(0.5 * searchRate));
                json.put("search1_35~39", round(0.3 * searchRate));
                json.put("search1_40~49", round(0.2 * searchRate));
                json.put("search1_50", round(0.1    * searchRate));
                break;
            case "性价比":
                json.put("search1_18~24", round(0.2 * searchRate));
                json.put("search1_25~29", round(0.4 * searchRate));
                json.put("search1_30~34", round(0.6 * searchRate));
                json.put("search1_35~39", round(0.7 * searchRate));
                json.put("search1_40~49", round(0.8 * searchRate));
                json.put("search1_50", round(0.8    * searchRate));
                break;
            case "健康与养生":
            case "家庭与育儿":
                json.put("search1_18~24", round(0.1 * searchRate));
                json.put("search1_25~29", round(0.2 * searchRate));
                json.put("search1_30~34", round(0.4 * searchRate));
                json.put("search1_35~39", round(0.6 * searchRate));
                json.put("search1_40~49", round(0.8 * searchRate));
                json.put("search1_50", round(0.7    * searchRate));
                break;
            case "科技与数码":
                json.put("search1_18~24", round(0.8 * searchRate));
                json.put("search1_25~29", round(0.6 * searchRate));
                json.put("search1_30~34", round(0.4 * searchRate));
                json.put("search1_35~39", round(0.3 * searchRate));
                json.put("search1_40~49", round(0.2 * searchRate));
                json.put("search1_50", round(0.1    * searchRate));
                break;
            case "学习与发展":
                json.put("search1_18~24", round(0.4 * searchRate));
                json.put("search1_25~29", round(0.5 * searchRate));
                json.put("search1_30~34", round(0.6 * searchRate));
                json.put("search1_35~39", round(0.7 * searchRate));
                json.put("search1_40~49", round(0.8 * searchRate));
                json.put("search1_50", round(0.7    * searchRate));
                break;
            default:
                json.put("search1_18~24", 0);
                json.put("search1_25~29", 0);
                json.put("search1_30~34", 0);
                json.put("search1_35~39", 0);
                json.put("search1_40~49", 0);
                json.put("search1_50", 0);
        }
        
        jsonObject.put("sum_18~24",json.getDouble("search_18~24") + json.getDouble("search1_18~24") + json.getDouble("price_25~29"));
        jsonObject.put("sum_25~29",json.getDouble("search_25~29") + json.getDouble("search1_25~29") + json.getDouble("price_25~29"));
        jsonObject.put("sum_30~34",json.getDouble("search_30~34") + json.getDouble("search1_30~34") + json.getDouble("price_25~29"));
        jsonObject.put("sum_35~39",json.getDouble("search_35~39") + json.getDouble("search1_35~39") + json.getDouble("price_25~29"));
        jsonObject.put("sum_40~49",json.getDouble("search_40~49") + json.getDouble("search1_40~49") + json.getDouble("price_25~29"));
        jsonObject.put("sum_50",json.getDouble("search_50") + json.getDouble("search1_50") + json.getDouble("price_50"));



        return jsonObject;
    }



    private static double round(double value) {
        return BigDecimal.valueOf(value)
                .setScale(3, RoundingMode.HALF_UP)
                .doubleValue();
    }


    @Override
    public void close() throws Exception {
        super.close();
        connection.close();
    }
}
