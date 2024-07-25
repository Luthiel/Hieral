package com.moving.common.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import org.apache.flink.api.common.functions.MapFunction;

public class DorisMapFunction<T> implements MapFunction<T, String> {
    @Override
    public String map(T bean) throws Exception {
        SerializeConfig conf = new SerializeConfig();
        // 驼峰命名规范 转 下划线命名规范
        conf.setPropertyNamingStrategy(PropertyNamingStrategy.SnakeCase);
        return JSON.toJSONString(bean, conf);
    }
}
