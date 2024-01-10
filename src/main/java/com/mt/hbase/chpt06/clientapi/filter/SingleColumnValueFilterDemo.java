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
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author pengxu
 */
public class SingleColumnValueFilterDemo extends BaseDemo {

    public static void main(String[] args) throws Exception {
        doScanWithSingleColumnValueFilter();
    }

    private static void doScanWithSingleColumnValueFilter() throws IOException {
        Scan scan = new Scan();
        Connection connection = null;
        Table table = null;
        ResultScanner resultScanner = null;
        try {
            /**
             *  查询表s_behavior 列限定符'o'，值为1004的数据
             */
            SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(
                Bytes.toBytes(CF_PC), Bytes.toBytes(COLUMN_ORDER),
                CompareOperator.EQUAL, Bytes.toBytes("1004"));
            /**
             * 如果数据行不包含列限定符'o' 则不返回该行
             * 如果设置为false，则会返回不包含列限定符'o'的数据行
             */
            singleColumnValueFilter.setFilterIfMissing(true);

            scan.setFilter(singleColumnValueFilter);
            connection = HBaseConnectionFactory.getConnection();
            table = connection.getTable(TableName.valueOf(TABLE));
            resultScanner = table.getScanner(scan);
            printResult(resultScanner);
            /**
             输出结果：
             rowkey=54321000000000000000092233703937313212872
             qualifier=o,value=1004
             qualifier=v,value=1004
             rowkey=54321000000000000000092233703937313240882
             qualifier=o,value=1004
             qualifier=v,value=1004
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
