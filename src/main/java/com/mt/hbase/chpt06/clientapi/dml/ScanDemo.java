package com.mt.hbase.chpt06.clientapi.dml;

import com.mt.hbase.chpt05.rowkeydesign.RowKeyUtil;
import com.mt.hbase.chpt06.clientapi.BaseDemo;
import com.mt.hbase.connection.HBaseConnectionFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;


public class ScanDemo extends BaseDemo{

    private static final long userId = 12345;

    public final static String MIN_TIME = "0000000000000000000";
    public final static String MAX_TIME = "9999999999999999999";

    private static RowKeyUtil rowKeyUtil = new RowKeyUtil();


    public static void main(String[] args) throws IOException, InterruptedException,
            ParseException {
        Scan scan = new Scan();
        //设置需要scan的数据列族
        scan.addFamily(Bytes.toBytes(CF_PC));
        //设置需要scan的数据列
        //scan.addColumn(Bytes.toBytes(CF_PHONE),Bytes.toBytes(COLUMN_ORDER));)
        //设置small scan以提高性能，如果扫描的数据在一个数据块内，则应该设置为true
        scan.setSmall(true);
        //设置扫描开始行键，结果包含开始行
        scan.setStartRow(Bytes.toBytes(rowKeyUtil.formatUserId(userId) + MIN_TIME));
        //设置扫描结束行键，结果不包含结束行
        scan.setStopRow(Bytes.toBytes(rowKeyUtil.formatUserId(userId) + MAX_TIME));
        //设置事务隔离级别
        scan.setIsolationLevel(IsolationLevel.READ_COMMITTED);
        //设置每次RPC请求读取数据行
        scan.setCaching(100);
        //设置scan的数据时间范围为2018-01-01到现在
        String startS = "2018-01-01";
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Date startDate = dateFormat.parse(startS);
        scan.setTimeRange(startDate.getTime(),System.currentTimeMillis());
        //设置scan的数据版本为2
        scan.setMaxVersions(2);
        //设置是否缓存读取的数据块，如果数据会被多次读取则应该设置为true，如果数据仅会被读取一次则应该设置为false
        scan.setCacheBlocks(false);

        Table table = HBaseConnectionFactory.getConnection().getTable(TableName.valueOf(TABLE));
        ResultScanner resultScanner = table.getScanner(scan);

        printResult(resultScanner);
    }

}
