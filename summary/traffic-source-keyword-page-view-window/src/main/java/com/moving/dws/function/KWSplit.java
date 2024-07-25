package com.moving.dws.function;


import com.moving.dws.util.IKUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.Set;

@FunctionHint(output = @DataTypeHint("Row<keyword String>"))
public class KWSplit extends TableFunction<Row> {
    public void eval(String keyword) {
        if (keyword == null) {
            return;
        }

        Set<String> keywords = IKUtil.split(keyword);
        for (String kw : keywords) {
            collect(Row.of(kw));
        }
    }
}
