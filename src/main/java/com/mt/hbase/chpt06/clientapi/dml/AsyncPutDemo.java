package com.mt.hbase.chpt06.clientapi.dml;

import com.mt.hbase.chpt05.rowkeydesign.RowKeyUtil;
import com.mt.hbase.connection.HBaseConnectionFactory;
import com.mt.hbase.constants.Constants;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AdvancedScanResultConsumer;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author pengxu
 */
public class AsyncPutDemo {

    private static final String[] ITEM_ID_ARRAY = new String[]{"1001", "1002", "1004", "1009"};

    private static final long userId = 12345;

    private static final RowKeyUtil rowKeyUtil = new RowKeyUtil();

    public static void main(String[] args) throws IOException, InterruptedException {
        doPut();
    }

    /**
     * 向hbase写入数据
     */
    private static void doPut(){
        List<Put> actions = new ArrayList<Put>();
        buildActions(actions);
        AsyncTable<AdvancedScanResultConsumer> asyncTable = HBaseConnectionFactory.getAsycConnection()
            .getTable(TableName.valueOf(Constants.TABLE));
        /**
         * 异步写入表
         */
        asyncTable.put(actions);
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

    private static String generateRowkey(long userId, long timestamp, long seqId) {
        return rowKeyUtil.formatUserId(userId) + rowKeyUtil.formatTimeStamp(timestamp) + seqId;
    }
}
