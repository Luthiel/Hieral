package com.moving.pojo;

import lombok.Data;

@Data
public class EventContextPO {

    /**
     * 设备POJO
     */
    private DevicePO device;
    /**
     * 用户信息POJO
     */
    private ProfilePO profile;
    /**
     * 商品信息POJO
     */
    private ProductPO product;
}