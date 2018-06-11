package com.mt.hbase.chpt06.clientapi.dml;

import com.mt.hbase.connection.HBaseConnectionFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;

public class IncrDemo {

    private static final String TABLE = "s_behavior";

    private static final String CF_PC    = "pc";

    private static final String COLUMN_FOR_INCR= "i";

    public static void main(String[] args) throws InterruptedException {


        List<Thread> incrThreadList = new ArrayList<Thread>();
        for(int i=0; i < 20; i++){
            IncrThread incrThread = new IncrThread();
            Thread t = new Thread(incrThread);
            t.setName("Thread_"+ i);
            incrThreadList.add(t);
            t.start();
        }

        for(Thread incrThread: incrThreadList){
            incrThread.join();
        }
    }

   static class IncrThread implements Runnable{

        @Override public void run() {
            try {
                Table table = HBaseConnectionFactory.getConnection().getTable(TableName.valueOf(TABLE));
                Increment increment = new Increment(Bytes.toBytes("rowkeyforincr"));
                increment.addColumn(Bytes.toBytes(CF_PC), Bytes.toBytes(COLUMN_FOR_INCR), 1);
                table.increment(increment);

                Get oneGet = new Get(Bytes.toBytes("rowkeyforincr"));
                Result getResult = table.get(oneGet);
                Cell[] getCells = getResult.rawCells();
                for (Cell cell : getCells) {
                    String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                    Long value = Bytes.toLong(CellUtil.cloneValue(cell));
                    System.out.println(Thread.currentThread().getName()+": qualifier=" + qualifier + ",value=" + value);
                }

            }catch(Exception e){
                System.out.println("incr failed"+ e.getMessage());
            }
        }
    }
}
