package com.mt.hbase.chpt06.clientapi;

import com.mt.hbase.connection.HBaseConnectionFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class IncrDemo {


    private static final String TABLE = "s_behavior";

    private static final String CF_PC    = "pc";

    private static final String COLUMN_FOR_INCR= "i";


    public static void main(String[] args) throws IOException, InterruptedException {

        Table table = HBaseConnectionFactory.getConnection().getTable(TableName.valueOf(TABLE));
        Increment increment = new Increment(Bytes.toBytes("rowkeyforincr"));
        increment.addColumn(Bytes.toBytes(CF_PC), Bytes.toBytes(COLUMN_FOR_INCR), 1);

        Result result = table.increment(increment);
        Cell[] cells = result.rawCells();
        System.out.println(result);
        System.out.println("rowkey=" + Bytes.toString(result.getRow()));
        for (Cell cell : cells) {
            String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
            Long value = Bytes.toLong(CellUtil.cloneValue(cell));
            System.out.println("qualifier=" + qualifier + ",value=" + value);
        }

    }
}
