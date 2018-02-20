package com.mt.hbase.chpt06.clientapi;

import com.mt.hbase.chpt05.rowkeydesign.RowKeyUtil;
import com.mt.hbase.connection.HBaseConnectionFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class PutDemo {

    private static final String TABLE="s_behavior";

    private static final String CF_PC="pc";
    private static final String CF_PHONE="ph";

    private static final String COLUMN_VIEW="v";

    private static final String COLUMN_ORDER="o";

    private static final String UNDER_SCORE="_";

    private static final String[] ITEM_ID_ARRAY = new String[]{"1001","1002","1004","1009"};

    private static final long userId = 12345;

    private static RowKeyUtil rowKeyUtil = new RowKeyUtil();

    public static void main(String[] args) throws IOException, InterruptedException {
        List<Put> actions = new ArrayList<Put>();
        Random random = new Random();
        for (int i = 0; i < ITEM_ID_ARRAY.length; i++) {
            Put put = new Put(Bytes.toBytes(generateRowkey(userId,System.currentTimeMillis(),i)));
            put.addColumn(Bytes.toBytes(CF_PC), Bytes.toBytes(COLUMN_VIEW),
                    Bytes.toBytes(ITEM_ID_ARRAY[i]));

            if(random.nextBoolean()){
                put.addColumn(Bytes.toBytes(CF_PC), Bytes.toBytes(COLUMN_ORDER),
                        Bytes.toBytes(ITEM_ID_ARRAY[i]));
            }
            actions.add(put);
        }
        Table table = HBaseConnectionFactory.getConnection().getTable(TableName.valueOf(TABLE));
        Object[] results = new Object[actions.size()];
        table.batch(actions, results);
    }

    private static String generateRowkey(long userId, long timestamp, long seqId){
        return rowKeyUtil.formatUserId(userId)+ rowKeyUtil.formatTimeStamp(timestamp)+seqId;
    }
}
