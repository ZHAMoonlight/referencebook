package com.mt.hbase.chpt06.clientapi.filter;

import com.mt.hbase.chpt05.rowkeydesign.RowKeyUtil;
import com.mt.hbase.chpt06.clientapi.BaseDemo;
import com.mt.hbase.connection.HBaseConnectionFactory;
import java.io.IOException;
import java.text.ParseException;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author pengxu
 */
public class RowFilterDemo extends BaseDemo {

    public static void main(String[] args) throws Exception {
        scanWithRowFilter();
    }

    private static void scanWithRowFilter() throws ParseException, IOException {
        RowKeyUtil rowKeyUtil = new RowKeyUtil();
        Scan scan = new Scan();
        Connection connection = null;
        Table table = null;
        ResultScanner resultScanner = null;
        try {
            /**
             * 查询用户ID12345，2018-01-01之后的数据
             */
            String startS = "2018-01-01";
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            Date startDate = dateFormat.parse(startS);

            String rowKey =
                rowKeyUtil.formatUserId(12345) + rowKeyUtil.formatTimeStamp(startDate.getTime());

            RowFilter rowFilter = new RowFilter(CompareOperator.LESS, new BinaryComparator(
                Bytes.toBytes(rowKey)));
            scan.setFilter(rowFilter);
            connection = HBaseConnectionFactory.getConnection();
            table = connection.getTable(TableName.valueOf(TABLE));
            resultScanner = table.getScanner(scan);

            printResult(resultScanner);
            /**
             输出结果：
             rowkey=54321000000000000000092233703937313212871
             qualifier=o,value=1002
             qualifier=v,value=1002
             rowkey=54321000000000000000092233703937313212872
             qualifier=o,value=1004
             qualifier=v,value=1004
             rowkey=54321000000000000000092233703937313212873
             qualifier=v,value=1009
             rowkey=54321000000000000000092233703937313212880
             qualifier=v,value=1001
             rowkey=54321000000000000000092233703937313240881
             qualifier=v,value=1002
             rowkey=54321000000000000000092233703937313240882
             qualifier=o,value=1004
             qualifier=v,value=1004
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
