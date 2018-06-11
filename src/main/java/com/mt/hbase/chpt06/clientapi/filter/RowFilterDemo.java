package com.mt.hbase.chpt06.clientapi.filter;

import com.mt.hbase.chpt05.rowkeydesign.RowKeyUtil;
import com.mt.hbase.chpt06.clientapi.BaseDemo;
import com.mt.hbase.connection.HBaseConnectionFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.text.SimpleDateFormat;
import java.util.Date;

public class RowFilterDemo extends BaseDemo {

    public static void main(String[] args) throws Exception {

        RowKeyUtil rowKeyUtil = new RowKeyUtil();
        Scan scan = new Scan();
        /**
         * 查询用户ID12345，2018-01-01之后的数据
         */
        String startS = "2018-01-01";
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Date startDate = dateFormat.parse(startS);

        String rowKey = rowKeyUtil.formatUserId(12345)+rowKeyUtil.formatTimeStamp(startDate.getTime());

        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.LESS, new BinaryComparator(
                Bytes.toBytes(rowKey)));
        scan.setFilter(rowFilter);

        Table table = HBaseConnectionFactory.getConnection().getTable(TableName.valueOf(TABLE));
        ResultScanner resultScanner = table.getScanner(scan);

        printResult(resultScanner);
        /**
         输出结果：
         rowkey=54321000000000000000092233705133122008861
         qualifier=v,value=1002
         rowkey=54321000000000000000092233705133122008862
         qualifier=o,value=1004
         qualifier=v,value=1004
         rowkey=54321000000000000000092233705133122008863
         qualifier=o,value=1009
         qualifier=v,value=1009
         rowkey=54321000000000000000092233705133122009210
         qualifier=o,value=1001
         qualifier=v,value=1001
         */
    }

}
