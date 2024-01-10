package com.mt.hbase.chpt06.clientapi;

import com.mt.hbase.constants.Constants;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class BaseDemo {

    protected static final String TABLE = Constants.TABLE;

    protected static final String CF_PC = Constants.CF_PC;
    protected static final String CF_PHONE = Constants.CF_PHONE;

    protected static final String COLUMN_VIEW = Constants.COLUMN_VIEW;

    protected static final String COLUMN_ORDER = Constants.COLUMN_ORDER;


    protected static void printResult(ResultScanner resultScanner) throws IOException {
        Result result = null;
        while ((result = resultScanner.next()) != null) {
            if (result.getRow() == null) {
                continue;// key-values=NONE
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
