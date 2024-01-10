package com.mt.hbase.chpt06.clientapi.filter;

import com.mt.hbase.chpt06.clientapi.BaseDemo;
import com.mt.hbase.connection.HBaseConnectionFactory;
import java.io.IOException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.TimestampsFilter;

import java.util.ArrayList;
import java.util.List;

/**
 * @author pengxu
 */
public class TimestampsFilterDemo extends BaseDemo {


    public static void main(String[] args) throws Exception {
        doScanWithTimestampFilter();
    }

    private static void doScanWithTimestampFilter() throws IOException {
        Scan scan = new Scan();
        Connection connection = null;
        Table table = null;
        ResultScanner resultScanner = null;
        try {

            /**
             *  查询表s_behavior hbase时间戳为1643123455499的数据
             */
            List<Long> timeStampList = new ArrayList<Long>();
            timeStampList.add(1643123455499L);
            TimestampsFilter timestampsFilter = new TimestampsFilter(timeStampList);
            scan.setFilter(timestampsFilter);
            /**
             也可以使用如下命令替换
             scan.setTimestamp(1643123455499L);
             scan.setTimeRange(1643123455499L,1643123455500L);
             */
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
