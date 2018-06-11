package com.mt.hbase.chpt06.clientapi.filter;

import com.mt.hbase.chpt05.rowkeydesign.RowKeyUtil;
import com.mt.hbase.chpt06.clientapi.BaseDemo;
import com.mt.hbase.connection.HBaseConnectionFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class PrefixFilterDemo extends BaseDemo {


    private static final String TABLE = "s_behavior";

    public static void main(String[] args) throws Exception {
        Scan scan = new Scan();
        RowKeyUtil rowKeyUtil = new RowKeyUtil();

        /**
         * 查询用户12345的所有数据
         */
        PrefixFilter prefixFilter = new PrefixFilter(Bytes.toBytes(rowKeyUtil.formatUserId(12345)));
        scan.setFilter(prefixFilter);

        Table table = HBaseConnectionFactory.getConnection().getTable(TableName.valueOf(TABLE));
        ResultScanner resultScanner = table.getScanner(scan);

        printResult(resultScanner);
    }

}
