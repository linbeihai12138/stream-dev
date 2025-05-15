package com.zsf.realtime.common.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @Package com.zsf.realtime.common.bean.DimBaseCategory
 * @Author zhao.shuai.fei
 * @Date 2025/5/14 15:23
 * @description:
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class DimBaseCategory2 implements Serializable {

    private String id;
    private String bcname;
    private String btname;
}
