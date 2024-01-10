package com.mt.hbase.chpt06.clientapi.dml;

import com.mt.hbase.chpt05.rowkeydesign.RowKeyUtil;
import com.mt.hbase.chpt06.clientapi.BaseDemo;
import com.mt.hbase.connection.HBaseConnectionFactory;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AdvancedScanResultConsumer;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.IsolationLevel;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author pengxu
 */
public class AsyncScanDemo extends BaseDemo {

    private static final long userId = 12345;

    public final static String MIN_TIME = "0000000000000000000";
    public final static String MAX_TIME = "9999999999999999999";

    private static RowKeyUtil rowKeyUtil = new RowKeyUtil();


    public static void main(String[] args) throws IOException, InterruptedException,
        ParseException {
        doScanTable();
    }

    private static void doScanTable() throws ParseException, IOException {
        AsyncConnection asyncConnection = null;
        AsyncTable<AdvancedScanResultConsumer> asyncTable = null;
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
            scan.setCaching(100);
            //设置请求返回的行数
            scan.setLimit(1000);
            //设置scan的数据时间范围为2018-01-01到现在
            String startS = "2018-01-01";
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            Date startDate = dateFormat.parse(startS);
            scan.setTimeRange(startDate.getTime(), System.currentTimeMillis());
            //设置scan获取2个版本的数据
            scan.readVersions(2);
            //设置是否缓存读取的数据块，如果数据会被多次读取则应该设置为true，如果数据仅会被读取一次则应该设置为false
            scan.setCacheBlocks(false);

            asyncConnection = HBaseConnectionFactory.getAsycConnection();
            asyncTable = asyncConnection.getTable(TableName.valueOf(TABLE));
            //针对小批量数据一次性scan读取
            //asyncTable.scanAll(scan);
//            resultScanner = asyncTable.getScanner(scan);
            AdvancedScanResultConsumer consumer = new AdvancedScanResultConsumer() {
                @Override
                public void onError(Throwable error) {
                    error.printStackTrace();
                }

                @Override
                public void onComplete() {

                }

                @Override
                public void onNext(Result[] results, ScanController controller) {
                    System.out.println("onNext: results.len=" + results.length);
                    for (Result result : results) {
                        Cell[] cells = result.rawCells();
                        System.out.println("rowkey=" + Bytes.toString(result.getRow()));
                        for (Cell cell : cells) {
                            String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                            String value = Bytes.toString(CellUtil.cloneValue(cell));
                            System.out.println("qualifier=" + qualifier + ",value=" + value);
                        }
                    }
                }
            };
            asyncTable.scan(scan, consumer);
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            if (null != resultScanner) {
                resultScanner.close();
            }
            if (null != asyncConnection) {
                asyncConnection.close();
            }

        }
    }
}
