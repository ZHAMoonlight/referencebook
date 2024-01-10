package com.mt.hbase.chpt06.clientapi.filter;

import com.mt.hbase.chpt06.clientapi.BaseDemo;
import com.mt.hbase.connection.HBaseConnectionFactory;
import java.io.IOException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;

/**
 * @author pengxu
 */
public class KeyOnlyFilterDemo extends BaseDemo {

    public static void main(String[] args) throws Exception {
        scanWithKeyOnlyFilter();
    }
    private static void scanWithKeyOnlyFilter() throws IOException {
        Scan scan = new Scan();
        Connection connection = null;
        Table table = null;
        ResultScanner resultScanner = null;
        try {
            /**
             * 查询表s_behavior所有数据的行键
             */
            scan.setFilter(new KeyOnlyFilter());

            connection = HBaseConnectionFactory.getConnection();
            table = connection.getTable(TableName.valueOf(TABLE));
            resultScanner = table.getScanner(scan);

            printResult(resultScanner);
            /**
             输出结果：
             rowkey=54321000000000000000092233703937313212871
             qualifier=o,value=
             qualifier=v,value=
             rowkey=54321000000000000000092233703937313212872
             qualifier=o,value=
             qualifier=v,value=
             rowkey=54321000000000000000092233703937313212873
             qualifier=v,value=
             rowkey=54321000000000000000092233703937313212880
             qualifier=v,value=
             rowkey=54321000000000000000092233703937313240881
             qualifier=v,value=
             rowkey=54321000000000000000092233703937313240882
             qualifier=o,value=
             qualifier=v,value=
             rowkey=54321000000000000000092233703937313240883
             qualifier=v,value=
             rowkey=54321000000000000000092233703937313242830
             qualifier=v,value=
             */
        } finally {
            if (null != resultScanner) {
                resultScanner.close();
            }
            if (null != table) {
                table.close();
            }
            if (null != connection) {
                connection.close();
            }
        }
    }

}
