package com.moving.common.bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserLoginBean {
    // 起始时间
    String stt;
    // 结束时间
    String edt;
    // 当前日期
    String curDate;
    // 回流用户数
    Long backCt;
    // 独立用户数
    Long uuCt;
    @JSONField(serialize = false)
    Long ts;
}
