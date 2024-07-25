package com.moving.utils;

import com.alibaba.fastjson.JSON;

public class JsonUtil {
  public static <T> T strToJsonObject(String value, Class<T> clazz) {
    return JSON.parseObject(value, clazz);
  }
}
