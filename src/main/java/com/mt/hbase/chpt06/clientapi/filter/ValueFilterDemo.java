package com.mt.hbase.chpt06.clientapi.filter;

import com.mt.hbase.chpt06.clientapi.BaseDemo;
import com.mt.hbase.connection.HBaseConnectionFactory;
import java.io.IOException;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author pengxu
 */
public class ValueFilterDemo extends BaseDemo {

    public static void main(String[] args) throws Exception {
        scanWithValueFilter();
    }

    private static void scanWithValueFilter() throws IOException {
        Scan scan = new Scan();
        Connection connection = null;
        Table table = null;
        ResultScanner resultScanner = null;
        try {
            /**
             * 查询表s_behavior包含商品1001记录的数据
             * 注意如果一行有多列数据，只有值等于1001数据的一列会返回
             */
            ValueFilter valueFilter = new ValueFilter(CompareOperator.EQUAL, new BinaryComparator(
                Bytes.toBytes("1001")));
            scan.setFilter(valueFilter);

            connection = HBaseConnectionFactory.getConnection();
            table = connection.getTable(TableName.valueOf(TABLE));
            resultScanner = table.getScanner(scan);

            printResult(resultScanner);
            /**
             输出结果：
             rowkey=54321000000000000000092233703937313212880
             qualifier=v,value=1001
             rowkey=54321000000000000000092233703937313242830
             qualifier=v,value=1001
             */
        }finally {
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
