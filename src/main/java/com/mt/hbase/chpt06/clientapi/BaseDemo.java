package com.mt.hbase.chpt06.clientapi;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class BaseDemo {

    protected static final String TABLE = "s_behavior";

    protected static final String CF_PC="pc";
    protected static final String CF_PHONE="ph";

    protected static final String COLUMN_VIEW="v";

    protected static final String COLUMN_ORDER="o";


    protected static void printResult(ResultScanner resultScanner) throws IOException {
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
