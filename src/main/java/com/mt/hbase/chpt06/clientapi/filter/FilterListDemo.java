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
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;

/**
 * @author pengxu
 */
public class FilterListDemo extends BaseDemo {

    public static void main(String[] args) throws Exception {
        doScanWithFilterList();
    }

    private static void doScanWithFilterList() throws IOException {
        Connection connection = null;
        Table table = null;
        ResultScanner resultScanner1 = null;
        ResultScanner resultScanner2 = null;
        try {
            connection = HBaseConnectionFactory.getConnection();
            table = connection.getTable(TableName.valueOf(TABLE));
            /**
             * 查询表s_behavior满足如下条件的数据行
             * 1.列限定符 pc:o 值等于 1004
             * 2.列数据时间戳等于1643123455499L
             * 3.只返回数据的行键，不返回列数据值
             */
            List<Filter> filters = new ArrayList<Filter>();
            SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(
                Bytes.toBytes(CF_PC), Bytes.toBytes(COLUMN_ORDER),
                CompareOperator.EQUAL, Bytes.toBytes("1004"));
            singleColumnValueFilter.setFilterIfMissing(true);
            List<Long> timeStampList = new ArrayList<Long>();
            timeStampList.add(1643123455499L);
            TimestampsFilter timestampsFilter = new TimestampsFilter(timeStampList);
            filters.add(singleColumnValueFilter);
            filters.add(timestampsFilter);
            filters.add(new KeyOnlyFilter());
            FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, filters);
            Scan scanOnlyKey = new Scan();
            scanOnlyKey.setFilter(filterList);
            resultScanner1 = table.getScanner(scanOnlyKey);
            System.out.println("start print result1");
            printResult(resultScanner1);
            /**
             输出结果：
             start print result1
             start print result1
             rowkey=54321000000000000000092233703937313212872
             qualifier=o,value=
             qualifier=v,value=
             */
            filters.clear();

            /**
             * 查询表s_behavior满足如下条件的数据行
             * 1.列限定符 pc:o 值等于 1004 或者 1002
             * 2.列数据时间戳等于1643123455499L
             * 3.只返回数据的行键，不返回列数据值
             */
            SingleColumnValueFilter singleColumnValueFilter2 = new SingleColumnValueFilter(
                Bytes.toBytes(CF_PC), Bytes.toBytes(COLUMN_ORDER),
                CompareOperator.EQUAL, Bytes.toBytes("1002"));

            singleColumnValueFilter2.setFilterIfMissing(true);
            filters.add(singleColumnValueFilter);
            filters.add(singleColumnValueFilter2);
            FilterList filterListAll = new FilterList(FilterList.Operator.MUST_PASS_ALL,
                timestampsFilter,
                new FilterList(FilterList.Operator.MUST_PASS_ONE, filters));
            Scan scanWithValue = new Scan();
            scanWithValue.setFilter(filterListAll);

            table = connection.getTable(TableName.valueOf(TABLE));
            resultScanner2 = table.getScanner(scanWithValue);
            System.out.println("start print result2");
            printResult(resultScanner2);
            /**
             输出结果：
             start print result2
             rowkey=54321000000000000000092233703937313212871
             qualifier=o,value=1002
             qualifier=v,value=1002
             rowkey=54321000000000000000092233703937313212872
             qualifier=o,value=1004
             qualifier=v,value=1004
             */
        } finally {
            if (null != resultScanner1) {
                resultScanner1.close();
            }
            if (null != resultScanner2) {
                resultScanner2.close();
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
