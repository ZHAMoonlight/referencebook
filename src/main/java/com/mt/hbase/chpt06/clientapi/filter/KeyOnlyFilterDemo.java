package com.mt.hbase.chpt06.clientapi.filter;

import com.mt.hbase.chpt06.clientapi.BaseDemo;
import com.mt.hbase.connection.HBaseConnectionFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;

public class KeyOnlyFilterDemo extends BaseDemo {


    public static void main(String[] args) throws Exception {

        Scan scan = new Scan();

        /**
         * 查询表s_behavior所有数据的行键
         */
        scan.setFilter(new KeyOnlyFilter());

        Table table = HBaseConnectionFactory.getConnection().getTable(TableName.valueOf(TABLE));
        ResultScanner resultScanner = table.getScanner(scan);

        printResult(resultScanner);
        /**
         输出结果：
         rowkey=54321000000000000000092233705133122008861
         qualifier=v,value=
         rowkey=54321000000000000000092233705133122008862
         qualifier=o,value=
         qualifier=v,value=
         rowkey=54321000000000000000092233705133122008863
         qualifier=o,value=
         qualifier=v,value=
         rowkey=54321000000000000000092233705133122009210
         qualifier=o,value=
         qualifier=v,value=
         */
    }


}
