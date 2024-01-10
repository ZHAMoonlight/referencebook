package com.mt.hbase.chpt06.clientapi.dml;

import com.mt.hbase.chpt05.rowkeydesign.RowKeyUtil;
import com.mt.hbase.chpt06.clientapi.BaseDemo;
import com.mt.hbase.connection.HBaseConnectionFactory;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.IsolationLevel;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author pengxu
 */
public class ScanDemo extends BaseDemo {

    private static final long userId = 12345;

    public final static String MIN_TIME = "0000000000000000000";
    public final static String MAX_TIME = "9999999999999999999";

    private static RowKeyUtil rowKeyUtil = new RowKeyUtil();


    public static void main(String[] args) throws IOException, InterruptedException,
        ParseException {
        doScanTable();
    }

    private static void doScanTable() throws ParseException, IOException {
        Connection connection = null;
        Table table = null;
        ResultScanner resultScanner = null;
        try {
            Scan scan = new Scan();
            //设置需要scan的数据列族
            scan.addFamily(Bytes.toBytes(CF_PC));
            //设置需要scan的数据列
            scan.addColumn(Bytes.toBytes(CF_PC), Bytes.toBytes(COLUMN_ORDER));
            //设置扫描开始行键，结果包含开始行
            scan.withStartRow(Bytes.toBytes(rowKeyUtil.formatUserId(userId) + MIN_TIME));
            //设置扫描结束行键，结果不包含结束行
            scan.withStopRow(Bytes.toBytes(rowKeyUtil.formatUserId(userId) + MAX_TIME));
            //设置事务隔离级别
            scan.setIsolationLevel(IsolationLevel.READ_COMMITTED);
            //设置每次RPC请求读取数据行
            scan.setCaching(500);
            //设置请求返回的行数
            scan.setLimit(800);
            //设置scan的数据时间范围为2018-01-01到现在
            String startS = "2018-01-01";
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            Date startDate = dateFormat.parse(startS);
            scan.setTimeRange(startDate.getTime(), System.currentTimeMillis());
            //设置scan获取2个版本的数据
            scan.readVersions(2);
            //设置是否缓存读取的数据块，如果数据会被多次读取则应该设置为true，如果数据仅会被读取一次则应该设置为false
            scan.setCacheBlocks(false);

            connection = HBaseConnectionFactory.getConnection();
            table = connection.getTable(TableName.valueOf(TABLE));
            resultScanner = table.getScanner(scan);

            printResult(resultScanner);
            resultScanner.close();
            table.close();
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

    /**
     * 输出结果为：
     *
     * rowkey=54321000000000000000092233703936883853351
     * qualifier=v,value=1002
     * rowkey=54321000000000000000092233703936883853352
     * qualifier=v,value=1004
     * rowkey=54321000000000000000092233703936883853353
     * qualifier=o,value=1009
     * qualifier=v,value=1009
     * rowkey=54321000000000000000092233703936883853480
     * qualifier=o,value=1001
     * qualifier=v,value=1001
     * rowkey=54321000000000000000092233703937313212871
     * qualifier=o,value=1002
     * qualifier=v,value=1002
     * rowkey=54321000000000000000092233703937313212872
     * qualifier=o,value=1004
     * qualifier=v,value=1004
     * rowkey=54321000000000000000092233703937313212873
     * qualifier=v,value=1009
     * rowkey=54321000000000000000092233703937313212880
     * qualifier=v,value=1001
     * rowkey=54321000000000000000092233703937313240881
     * qualifier=v,value=1002
     * qualifier=v,value=1002
     * rowkey=54321000000000000000092233703937313240882
     * qualifier=o,value=1004
     * qualifier=o,value=1004
     * qualifier=v,value=1004
     * qualifier=v,value=1004
     * rowkey=54321000000000000000092233703937313240883
     * qualifier=v,value=1009
     * qualifier=v,value=1009
     * rowkey=54321000000000000000092233703937313242830
     * qualifier=v,value=1001
     * qualifier=v,value=1001
     *
     */
}
