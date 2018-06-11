package com.mt.hbase.chpt08.coprocessor;

import junit.framework.TestCase;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by pengxu on 2017/10/27.
 */
public class KeyWordFilterRegionObserverTest extends TestCase{


    //敏感词
    private static final String ALIBABA = "阿里巴巴";
    //敏感词替换字符串
    private static final String MASK = "**";

    @Test
    public void testReplace(){

        List<Result> list= new ArrayList<Result>();

        Cell[] cellArray = new Cell[2];
        KeyValue cell1 =  new KeyValue(Bytes.toBytes("阿里巴巴row1"),
                Bytes.toBytes("cf"), Bytes.toBytes("q"),
                System.currentTimeMillis(), Bytes.toBytes(ALIBABA +"value1"));

        KeyValue cell2 =  new KeyValue(Bytes.toBytes("阿里巴巴row2"),
                Bytes.toBytes("cf"), Bytes.toBytes("q"),
                System.currentTimeMillis(), Bytes.toBytes(ALIBABA +"value2"));
        cellArray[0]=cell1;
        cellArray[1]=cell2;
        Result result1=  Result.create(cellArray);
        list.add(result1);

        Iterator<Result> iterator = list.iterator();
        while (iterator.hasNext()) {
            Result result = iterator.next();
            Cell[] cells = result.rawCells();
            if (CollectionUtils.isNotEmpty(list)) {
                for (int j = 0; j < cells.length; j++) {
                    Cell cell = cells[j];
                    KeyValue keyValue = replaceCell(cell);
                    cells[j] = keyValue;
                }
            }
        }

        Cell[] cells = result1.rawCells();
        for(Cell cell : cells){
            System.out.println(Bytes.toString(CellUtil.cloneRow(cell)));
            System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
        }
    }

    private KeyValue replaceCell(Cell cell) {
        byte[] oldValueByte = CellUtil.cloneValue(cell);
        String oldValueByteString = Bytes.toString(oldValueByte);
        String newValueByteString = oldValueByteString.replaceAll(ALIBABA, MASK);
        byte[] newValueByte = Bytes.toBytes(newValueByteString);
        return new KeyValue(CellUtil.cloneRow(cell),
                CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell),
                cell.getTimestamp(), newValueByte);
    }
}
