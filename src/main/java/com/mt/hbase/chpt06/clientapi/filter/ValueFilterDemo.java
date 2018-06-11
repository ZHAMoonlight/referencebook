package com.mt.hbase.chpt06.clientapi.filter;

import com.mt.hbase.chpt06.clientapi.BaseDemo;
import com.mt.hbase.connection.HBaseConnectionFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class ValueFilterDemo extends BaseDemo {

    private static final String TABLE = "s_behavior";

    public static void main(String[] args) throws Exception {

        Scan scan = new Scan();

        /**
         * 查询表s_behavior包含商品1001记录的数据
         * 注意如果一行有多列数据，只有值等于1001数据的一列会返回
         */
        ValueFilter valueFilter = new ValueFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(
                Bytes.toBytes("1001")));
        scan.setFilter(valueFilter);

        Table table = HBaseConnectionFactory.getConnection().getTable(TableName.valueOf(TABLE));
        ResultScanner resultScanner = table.getScanner(scan);

        printResult(resultScanner);
        /**
         输出结果：
         rowkey=54321000000000000000092233705133122009210
         qualifier=o,value=1001
         qualifier=v,value=1001
         */
    }
}
