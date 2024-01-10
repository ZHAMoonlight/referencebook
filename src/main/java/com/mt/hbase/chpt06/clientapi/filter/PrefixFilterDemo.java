package com.mt.hbase.chpt06.clientapi.filter;

import com.mt.hbase.chpt05.rowkeydesign.RowKeyUtil;
import com.mt.hbase.chpt06.clientapi.BaseDemo;
import com.mt.hbase.connection.HBaseConnectionFactory;
import java.io.IOException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author pengxu
 */
public class PrefixFilterDemo extends BaseDemo {

    public static void main(String[] args) throws Exception {
        ScanWithPrefixFilter();
    }

    private static void ScanWithPrefixFilter() throws IOException {
        Scan scan = new Scan();
        Connection connection = null;
        Table table = null;
        ResultScanner resultScanner = null;
        try {
            RowKeyUtil rowKeyUtil = new RowKeyUtil();
            /**
             * 查询用户12345的所有数据
             */
            PrefixFilter prefixFilter = new PrefixFilter(
                Bytes.toBytes(rowKeyUtil.formatUserId(12345)));
            scan.setFilter(prefixFilter);
            connection = HBaseConnectionFactory.getConnection();
            table = connection.getTable(TableName.valueOf(TABLE));
            resultScanner = table.getScanner(scan);

            printResult(resultScanner);
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
