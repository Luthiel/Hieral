package com.moving.common.bean;

import lombok.Data;

@Data
public class TableProcessDwd {
    String sourceTable;
    String sourceType;
    String sinkTable;
    String sinkColumns;
    // 配置表操作类型
    String op;
}
