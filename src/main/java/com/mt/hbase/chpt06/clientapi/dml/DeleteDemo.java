package com.mt.hbase.chpt06.clientapi.dml;

import com.mt.hbase.connection.HBaseConnectionFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class DeleteDemo {

    private static final String TABLE="s_behavior";

    private static final String CF_PC="pc";
    private static final String CF_PHONE="ph";

    private static final String COLUMN_VIEW="v";

    private static final String COLUMN_ORDER="o";


    public static void main(String[] args) throws IOException, InterruptedException {
        String rowkeyToDelete = "54321000000000000000092233705146317032071";
        Table table = HBaseConnectionFactory.getConnection().getTable(TableName.valueOf(TABLE));


        Get oneGet = new Get(Bytes.toBytes(rowkeyToDelete));
        Result result = table.get(oneGet);
        /**
         * 下行输出如下：
         * lineNo=1，qualifier=o,value=1002
         * lineNo=1，qualifier=v,value=1002
         */
        printResult(result,"1");

        Delete deleteColumn = new Delete(Bytes.toBytes(rowkeyToDelete));
        //设置需要删除的列
        deleteColumn.addColumn(Bytes.toBytes(CF_PC),Bytes.toBytes(COLUMN_VIEW));
        //设置需要删除一天之前的数据版本
        deleteColumn.setTimestamp(System.currentTimeMillis() - 24*60*60*1000);
        table.delete(deleteColumn);

        oneGet = new Get(Bytes.toBytes(rowkeyToDelete));
        result = table.get(oneGet);
        /**
         * 下行输出如下：
         * lineNo=2，qualifier=o,value=1002
         */
        printResult(result,"2");

        Delete deleteFamily = new Delete(Bytes.toBytes(rowkeyToDelete));
        //设置需要删除的列族
        deleteFamily.addFamily(Bytes.toBytes(CF_PC));
        table.delete(deleteFamily);

        oneGet = new Get(Bytes.toBytes(rowkeyToDelete));
        result = table.get(oneGet);
        /**
         * 下行输出为空
         */
        printResult(result,"3");

        Delete deleteRow = new Delete(Bytes.toBytes(rowkeyToDelete));
        //删除整行
        table.delete(deleteRow);

        oneGet = new Get(Bytes.toBytes(rowkeyToDelete));
        result = table.get(oneGet);
        /**
         * 下行输出为空
         */
        printResult(result,"4");
    }


    private static void printResult(Result result, String lineNo) {
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
            String value = Bytes.toString(CellUtil.cloneValue(cell));
            System.out.println("lineNo="+lineNo+ "，qualifier="+ qualifier+",value="+value);
        }
    }
}
