package com.mt.hbase.chpt06.clientapi;

import com.mt.hbase.connection.HBaseConnectionFactory;
import java.util.Calendar;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;

/**
 * @author pengxu
 * @Date 2022/01/31.
 */
public class TransactionDemo {

    private static final String TABLE = "s_transaction_test";
    private static final String COLUMN_FAMILY = "cf";
    private static final int PUT_COUNT = 1000;


    private void doPut(String valuePrefix) throws Exception {
        Table table = null;
        try {
            List<Put> actions = new ArrayList<Put>();
            for (int i = 0; i < PUT_COUNT; i++) {
                Put put = new Put(Bytes.toBytes("RowKey" + i));
                put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("a"),
                    Bytes.toBytes(valuePrefix + i));
                actions.add(put);
            }
            table = HBaseConnectionFactory.getConnection().getTable(TableName.valueOf(TABLE));
            Object[] results = new Object[actions.size()];
            table.batch(actions, results);
            table.close();
            System.out.println("put done");
        } finally {
            if (null != table) {
                table.close();
            }
        }
    }


    private void doScan() throws Exception {
        Table table = null;
        try {
            Scan scan = new Scan();
            /**
             * 表示一个RPC请求只取一条数据
             */
            scan.setCaching(1);
            /**
             * 设置事务隔离级别为 读已提交
             */
            scan.setIsolationLevel(IsolationLevel.READ_COMMITTED);
            table = HBaseConnectionFactory.getConnection().getTable(TableName.valueOf(TABLE));
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
                    System.out.println(System.currentTimeMillis()+
                        Bytes.toString(result.getRow()) + "[" + qualifier + "=" + value + "]");
                }
            }
        } finally {
            if (null != table) {
                table.close();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        final TransactionDemo hbaseTransactionTest = new TransactionDemo();
        hbaseTransactionTest.doPut("t1");

        Thread putThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    hbaseTransactionTest.doPut("t2");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        putThread.start();

        Thread scanThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    hbaseTransactionTest.doScan();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        scanThread.start();

        Thread thread3 = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(1000);
                    Put put = new Put(Bytes.toBytes("RRKey3"));
                    put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("a"),
                        Bytes.toBytes("RRKeyValue"));
                    Table table = HBaseConnectionFactory.getConnection().getTable(TableName.valueOf(TABLE));
                    table.put(put);
                    table.close();
                    System.out.println(System.currentTimeMillis()+ "RRKey3PutDone");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        thread3.start();
    }

}
