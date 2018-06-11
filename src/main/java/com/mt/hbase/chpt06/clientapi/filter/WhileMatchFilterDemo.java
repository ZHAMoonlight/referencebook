package com.mt.hbase.chpt06.clientapi.filter;

import com.mt.hbase.chpt06.clientapi.BaseDemo;
import com.mt.hbase.connection.HBaseConnectionFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

public class WhileMatchFilterDemo extends BaseDemo {

    public static void main(String[] args) throws Exception {

        Scan scan = new Scan();
        /**
         *  查询表s_behavior
         *  相当于while执行，当条件不满足了即返回结果
         *  如下过滤条件相当于遇到1009的数据内容就返回
         */
        WhileMatchFilter whileMatchFilter = new WhileMatchFilter(new ValueFilter(CompareFilter.CompareOp.NOT_EQUAL,
                new BinaryComparator(Bytes.toBytes("1009"))));

        scan.setFilter(whileMatchFilter);
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
         */
    }
}
