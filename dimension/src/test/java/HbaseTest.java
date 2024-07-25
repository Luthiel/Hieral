import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

public class HbaseTest {
    public static void main(String[] args) throws IOException {
        // 配置 HBase
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "hadoop1");
        config.set("hbase.zookeeper.property.clientPort", "2181");

        // 获取连接
        try (Connection connection = ConnectionFactory.createConnection(config);
             Table table = connection.getTable(TableName.valueOf("dim_base_dic"))) {
            // 创建Scan对象
            Scan scan = new Scan();
            // 设置行键范围
            scan.setStartRow("dic_code".getBytes());
            scan.setStopRow("dic_code".getBytes());
            // 执行Scan操作
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                // 处理结果
                System.out.println(result);
            }
            // 关闭Scanner
            scanner.close();
        }
    }
}
