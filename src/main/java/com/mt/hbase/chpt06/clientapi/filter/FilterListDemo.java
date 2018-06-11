package com.mt.hbase.chpt06.clientapi.filter;

import com.mt.hbase.chpt06.clientapi.BaseDemo;
import com.mt.hbase.connection.HBaseConnectionFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;

public class FilterListDemo extends BaseDemo {

    public static void main(String[] args) throws Exception {

        Table table = HBaseConnectionFactory.getConnection().getTable(TableName.valueOf(TABLE));

        /**
         * 查询表s_behavior满足如下条件的数据行
         * 1.列限定符 pc:o 值等于 1004
         * 2.列数据时间戳等于1523541994550L
         * 3.只返回数据的行键，不返回列数据值
         */
        List<Filter> filters = new ArrayList<Filter>();
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(Bytes.toBytes(CF_PC), Bytes.toBytes(COLUMN_ORDER),
                CompareFilter.CompareOp.EQUAL, Bytes.toBytes("1004"));
        List<Long> timeStampList = new ArrayList<Long>();
        timeStampList.add(1523541994550L);
        TimestampsFilter timestampsFilter = new TimestampsFilter(timeStampList);
        filters.add(singleColumnValueFilter);
        filters.add(timestampsFilter);
        filters.add(new KeyOnlyFilter());
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, filters);
        Scan scanOnlyKey = new Scan();
        scanOnlyKey.setFilter(filterList);
        ResultScanner resultScanner = table.getScanner(scanOnlyKey);
        printResult(resultScanner);
        /**
         输出结果：
         rowkey=54321000000000000000092233705133122008862
         qualifier=o,value=
         qualifier=v,value=
         */
        filters.clear();

        /**
         * 查询表s_behavior满足如下条件的数据行
         * 1.列限定符 pc:o 值等于 1004 或者 1002
         * 2.列数据时间戳等于1523541994550L
         * 3.只返回数据的行键，不返回列数据值
         */
        SingleColumnValueFilter singleColumnValueFilter2 = new SingleColumnValueFilter(Bytes.toBytes(CF_PC), Bytes.toBytes(COLUMN_ORDER),
                CompareFilter.CompareOp.EQUAL, Bytes.toBytes("1002"));
        filters.add(singleColumnValueFilter);
        filters.add(singleColumnValueFilter2);
        FilterList filterListAll = new FilterList(FilterList.Operator.MUST_PASS_ALL,
                timestampsFilter,
                new FilterList(FilterList.Operator.MUST_PASS_ONE, filters),
                new KeyOnlyFilter());
        Scan scanWithValue = new Scan();
        scanWithValue.setFilter(filterListAll);
        resultScanner = table.getScanner(scanWithValue);
        printResult(resultScanner);
        /**
         输出结果：
         rowkey=54321000000000000000092233705133122008861
         qualifier=v,value=1002
         rowkey=54321000000000000000092233705133122008862
         qualifier=o,value=1004
         qualifier=v,value=1004
         */
    }

}
