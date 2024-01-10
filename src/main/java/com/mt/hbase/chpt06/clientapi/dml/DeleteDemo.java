package com.mt.hbase.chpt06.clientapi.dml;

import com.mt.hbase.connection.HBaseConnectionFactory;
import com.mt.hbase.constants.Constants;
import javafx.scene.control.Tab;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author pengxu
 */
public class DeleteDemo {

    public static void main(String[] args) throws IOException, InterruptedException {
        doDelete();
    }

    private static void doDelete() throws IOException {
        String rowkeyToDelete = "54321000000000000000092233703344678657363";
        Connection connection = null;
        Table table = null;
        try {
            connection = HBaseConnectionFactory.getConnection();
            table = connection.getTable(TableName.valueOf(Constants.TABLE));

            Get oneGet = new Get(Bytes.toBytes(rowkeyToDelete));
            Result result = table.get(oneGet);
            /**
             * 下行输出如下：
             * lineNo=1，qualifier=o,value=1009
             * lineNo=1，qualifier=v,value=1009
             */
            printResult(result, "1");

            Delete deleteColumn = new Delete(Bytes.toBytes(rowkeyToDelete));
            //设置需要删除的列
            deleteColumn
                .addColumn(Bytes.toBytes(Constants.CF_PC), Bytes.toBytes(Constants.COLUMN_VIEW));
            //设置需要删除一天之前的数据版本
            deleteColumn.setTimestamp(System.currentTimeMillis() - 24 * 60 * 60 * 1000);
            table.delete(deleteColumn);

            oneGet = new Get(Bytes.toBytes(rowkeyToDelete));
            result = table.get(oneGet);
            /**
             * 下行输出如下：
             * lineNo=2，qualifier=o,value=1009
             */
            printResult(result, "2");

            Delete deleteFamily = new Delete(Bytes.toBytes(rowkeyToDelete));
            //设置需要删除的列族
            deleteFamily.addFamily(Bytes.toBytes(Constants.CF_PC));
            table.delete(deleteFamily);

            oneGet = new Get(Bytes.toBytes(rowkeyToDelete));
            result = table.get(oneGet);
            /**
             * 下行输出为空
             */
            printResult(result, "3");

            Delete deleteRow = new Delete(Bytes.toBytes(rowkeyToDelete));
            //删除整行
            table.delete(deleteRow);

            oneGet = new Get(Bytes.toBytes(rowkeyToDelete));
            result = table.get(oneGet);
            /**
             * 下行输出为空
             */
            printResult(result, "4");
        }finally {
            if (null != table) {
                table.close();
            }
            if (null != connection) {
                connection.close();
            }
        }
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
