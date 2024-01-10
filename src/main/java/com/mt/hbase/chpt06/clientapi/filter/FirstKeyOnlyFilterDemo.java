package com.mt.hbase.chpt06.clientapi.filter;

import com.mt.hbase.chpt06.clientapi.BaseDemo;
import com.mt.hbase.connection.HBaseConnectionFactory;
import com.mt.hbase.constants.Constants;
import java.io.IOException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;

/**
 * @author pengxu
 */
public class FirstKeyOnlyFilterDemo extends BaseDemo {

    public static void main(String[] args) throws Exception {
        scanWithFirstKeyOnlyFilter();
    }

    private static void scanWithFirstKeyOnlyFilter() throws IOException {
        Scan scan = new Scan();
        Connection connection = null;
        Table table = null;
        ResultScanner resultScanner = null;
        try {

            /**
             * 查询表s_behavior所有数据，每行只返回第一列，通常与KeyOnlyFilter一起使用
             */
            scan.setFilter(new FirstKeyOnlyFilter());
            connection = HBaseConnectionFactory.getConnection();
            table = connection.getTable(TableName.valueOf(Constants.TABLE));
            resultScanner = table.getScanner(scan);

            printResult(resultScanner);
            /**
             输出结果：
             rowkey=54321000000000000000092233703937313212871
             qualifier=o,value=1002
             rowkey=54321000000000000000092233703937313212872
             qualifier=o,value=1004
             rowkey=54321000000000000000092233703937313212873
             qualifier=v,value=1009
             rowkey=54321000000000000000092233703937313212880
             qualifier=v,value=1001
             rowkey=54321000000000000000092233703937313240881
             qualifier=v,value=1002
             rowkey=54321000000000000000092233703937313240882
             qualifier=o,value=1004
             rowkey=54321000000000000000092233703937313240883
             qualifier=v,value=1009
             rowkey=54321000000000000000092233703937313242830
             qualifier=v,value=1001
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
