package com.mt.hbase.chpt06.clientapi;

import com.mt.hbase.chpt05.rowkeydesign.RowKeyUtil;
import com.mt.hbase.connection.HBaseConnectionFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by pengxu on 2018/1/22.
 */
public class ScanDemo {

    private static final String TABLE = "s_behavior";

    private static final String CF_PC    = "pc";
    private static final String CF_PHONE = "ph";

    private static final String COLUMN_VIEW = "v";

    private static final String COLUMN_ORDER = "o";

    private static final long userId = 12345;

    public final static String MIN_TIME = "0000000000000000000";
    public final static String MAX_TIME = "9999999999999999999";

    private static RowKeyUtil rowKeyUtil = new RowKeyUtil();


    public static void main(String[] args) throws IOException, InterruptedException {
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(rowKeyUtil.formatUserId(userId) + MIN_TIME));

        scan.setStopRow(Bytes.toBytes(rowKeyUtil.formatUserId(userId) + MAX_TIME));
        scan.setCaching(100);

        Table table = HBaseConnectionFactory.getConnection().getTable(TableName.valueOf(TABLE));
        ResultScanner resultScanner = table.getScanner(scan);

        Result result = null;
        while ((result = resultScanner.next()) != null) {
            if (result.getRow() == null) {
                continue;// keyvalues=NONE
            }
            Cell[] cells = result.rawCells();
            System.out.println("rowkey=" + Bytes.toString(result.getRow()));
            for (Cell cell : cells) {
                String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                System.out.println("qualifier=" + qualifier + ",value=" + value);
            }
        }
    }

}
