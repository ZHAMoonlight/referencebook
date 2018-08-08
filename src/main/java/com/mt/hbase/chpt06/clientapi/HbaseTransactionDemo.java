package com.mt.hbase.chpt06.clientapi;

import com.mt.hbase.connection.HBaseConnectionFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by pengxu on 2017/10/10.
 */
public class HbaseTransactionDemo {

    private static final String TABLE         = "s_test";
    private static final String COLUMN_FAMILY = "cf";


    private void doPut(String valuePrefix) throws Exception {
        List<Put> actions = new ArrayList<Put>();
        for (int i = 0; i < 1000; i++) {
            Put put = new Put(Bytes.toBytes("rowkey" + i));
            put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("a"),
                    Bytes.toBytes(valuePrefix + i));
            actions.add(put);
        }
        Table table = HBaseConnectionFactory.getConnection().getTable(TableName.valueOf(TABLE));
        Object[] results = new Object[actions.size()];
        table.batch(actions, results);
        System.out.println("put done");
    }


    private void doScan() throws Exception {
        Scan scan = new Scan();
        scan.setCaching(1);//表示一个RPC请求只取一条数据
        scan.setIsolationLevel(IsolationLevel.READ_COMMITTED);//设置事务隔离级别为 读已提交
        Table table = HBaseConnectionFactory.getConnection().getTable(TableName.valueOf(TABLE));
        ResultScanner resultScanner = table.getScanner(scan);
        Result result = null;
        while ((result = resultScanner.next()) != null) {
            if (result.getRow() == null) {
                continue;// keyvalues=NONE
            }
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                System.out.println(
                        Bytes.toString(result.getRow()) + "[" + qualifier + "=" + value + "]");
            }
        }
    }

    public static void main(String[] args) throws Exception {
        final HbaseTransactionDemo hbaseTransactionTest = new HbaseTransactionDemo();
        hbaseTransactionTest.doPut("t1");

        Thread putThread = new Thread(new Runnable() {
            @Override public void run() {
                try {
                    hbaseTransactionTest.doPut("t2");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        putThread.start();

        Thread scanThread = new Thread(new Runnable() {
            @Override public void run() {
                try {
                    hbaseTransactionTest.doScan();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        scanThread.start();
    }

}
