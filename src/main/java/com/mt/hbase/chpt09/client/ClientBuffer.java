package com.mt.hbase.chpt09.client;

import com.mt.hbase.chpt05.rowkeydesign.RowKeyUtil;
import com.mt.hbase.connection.HBaseConnectionFactory;
import com.mt.hbase.constants.Constants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author pengxu
 */
public class ClientBuffer {

    public static void main(String[] args) throws IOException, InterruptedException {
        doBufferedMutator();
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
        params.writeBufferSize(10*1024*1024);
        try {
            Connection conn = HBaseConnectionFactory.getConnection();
            BufferedMutator mutator = conn.getBufferedMutator(params);
            List<Put> actions = new ArrayList<Put>();
            Put put = new Put(Bytes.toBytes("rowkey1"));
            put.addColumn(Bytes.toBytes(Constants.CF_PC), Bytes.toBytes(Constants.COLUMN_VIEW),
                Bytes.toBytes("value1"));
            actions.add(put);
            mutator.mutate(actions);
            mutator.close();
            conn.close();
        } catch (IOException e1) {
            e1.printStackTrace();
        }
    }

}
