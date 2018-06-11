package com.mt.hbase.chpt06.clientapi.filter;

import com.mt.hbase.chpt06.clientapi.BaseDemo;
import com.mt.hbase.connection.HBaseConnectionFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.TimestampsFilter;

import java.util.ArrayList;
import java.util.List;

public class TimestampsFilterDemo extends BaseDemo {


    public static void main(String[] args) throws Exception {

        Scan scan = new Scan();

        /**
         *  查询表s_behavior hbase时间戳为1523541994550的数据
         */
        List<Long> timeStampList = new ArrayList<Long>();
        timeStampList.add(1523541994550L);
        TimestampsFilter timestampsFilter = new TimestampsFilter(timeStampList);

        scan.setFilter(timestampsFilter);

        /**
         也可以使用如下命令替换
         scan.setTimeStamp(1523541994550L);
         scan.setTimeRange(1523541994550L,1524550412164L);
         */

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
