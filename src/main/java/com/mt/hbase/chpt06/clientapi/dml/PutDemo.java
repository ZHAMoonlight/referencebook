package com.mt.hbase.chpt06.clientapi.dml;

import com.mt.hbase.chpt05.rowkeydesign.RowKeyUtil;
import com.mt.hbase.connection.HBaseConnectionFactory;
import com.mt.hbase.constants.Constants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * @author pengxu
 */
public class PutDemo {

    private static final String[] ITEM_ID_ARRAY = new String[]{"1001", "1002", "1004", "1009"};

    private static final long userId = 12345;

    private static final RowKeyUtil rowKeyUtil = new RowKeyUtil();

    public static void main(String[] args) throws IOException, InterruptedException {
        doPut();
//        doBufferedMutator();
    }

    /**
     * 直接向hbase写入数据
     */
    private static void doPut() throws IOException, InterruptedException {
        List<Put> actions = new ArrayList<Put>();
        buildActions(actions);
        Table table = HBaseConnectionFactory.getConnection()
            .getTable(TableName.valueOf(Constants.TABLE));
        /**
         * 方法一：向Table写入数据
         */
        table.put(actions);

        /**
         * 方法二：执行table的批量操作，actions可以是Put、Delete、Get、Increment等操作，并且有可以获取执行结果
         */
//        Object[] results = new Object[actions.size()];
//        table.batch(actions, results);
    }

    private static void buildActions(List<Put> actions) {
        Random random = new Random();
        for (int i = 0; i < ITEM_ID_ARRAY.length; i++) {
            String rowKey = generateRowkey(userId, System.currentTimeMillis(), i);
            Put put = new Put(Bytes.toBytes(rowKey));
            //添加列
            put.addColumn(Bytes.toBytes(Constants.CF_PC), Bytes.toBytes(Constants.COLUMN_VIEW),
                Bytes.toBytes(ITEM_ID_ARRAY[i]));
            if (random.nextBoolean()) {
                put.addColumn(Bytes.toBytes(Constants.CF_PC), Bytes.toBytes(Constants.COLUMN_ORDER),
                    Bytes.toBytes(ITEM_ID_ARRAY[i]));
            }
            actions.add(put);
        }
    }

    /**
     * 批量、异步向hbase写入数据
     */
    private static void doBufferedMutator() {
        final BufferedMutator.ExceptionListener listener = new BufferedMutator.ExceptionListener() {
            @Override
            public void onException(RetriesExhaustedWithDetailsException e,
                BufferedMutator mutator) {
                for (int i = 0; i < e.getNumExceptions(); i++) {
                    System.out.println("error mutator: " + e.getRow(i));
                }
            }
        };
        BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf(Constants.TABLE))
            .listener(listener);
        params.writeBufferSize(512);
        try {
            Connection conn = HBaseConnectionFactory.getConnection();
            BufferedMutator mutator = conn.getBufferedMutator(params);
            List<Put> actions = new ArrayList<Put>();
            buildActions(actions);

            mutator.mutate(actions);
            mutator.close();
            conn.close();
        } catch (IOException e1) {
            e1.printStackTrace();
        }
    }

    private static String generateRowkey(long userId, long timestamp, long seqId) {
        return rowKeyUtil.formatUserId(userId) + rowKeyUtil.formatTimeStamp(timestamp) + seqId;
    }
}
