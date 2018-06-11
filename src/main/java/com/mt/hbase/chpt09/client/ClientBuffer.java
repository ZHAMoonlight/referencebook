package com.mt.hbase.chpt09.client;

import com.mt.hbase.chpt05.rowkeydesign.RowKeyUtil;
import com.mt.hbase.connection.HBaseConnectionFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class ClientBuffer {


    private static final String TABLE="s_behavior";

    private static final String CF_PC="pc";

    private static final String COLUMN_VIEW="v";

    private static final long userId = 12345;

    private static RowKeyUtil rowKeyUtil = new RowKeyUtil();

    public static void main(String[] args) throws IOException, InterruptedException {
        List<Put> actions = new ArrayList<Put>();
        Put put = new Put(Bytes.toBytes(generateRowkey(userId,System.currentTimeMillis(),1)));
        put.addColumn(Bytes.toBytes(CF_PC), Bytes.toBytes(COLUMN_VIEW),
                Bytes.toBytes("1001"));
        actions.add(put);
        HTable table = (HTable)HBaseConnectionFactory.getConnection().getTable(TableName.valueOf(TABLE));

        table.setAutoFlushTo(false);
        table.setWriteBufferSize(1024 * 1024 * 10);// 缓存大小10M

        Object[] results = new Object[actions.size()];
        table.batch(actions, results);
    }

    private static String generateRowkey(long userId, long timestamp, long seqId){
        return rowKeyUtil.formatUserId(userId)+ rowKeyUtil.formatTimeStamp(timestamp)+seqId;
    }

}
