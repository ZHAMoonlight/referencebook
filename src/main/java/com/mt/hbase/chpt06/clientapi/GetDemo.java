package com.mt.hbase.chpt06.clientapi;

import com.mt.hbase.connection.HBaseConnectionFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GetDemo {

    private static final String TABLE="s_behavior";

    private static final String CF_PC="pc";
    private static final String CF_PHONE="ph";

    private static final String COLUMN_VIEW="v";

    private static final String COLUMN_ORDER="o";


    public static void main(String[] args) throws IOException, InterruptedException {
        List<Get> gets = new ArrayList<Get>();
        Get oneGet = new Get(Bytes.toBytes("54321000000000000000092233705201799536092"));
        oneGet.addFamily(Bytes.toBytes(CF_PC));
//        oneGet.addColumn(Bytes.toBytes(CF_PHONE),Bytes.toBytes(COLUMN_ORDER));
        gets.add(oneGet);

        Table table = HBaseConnectionFactory.getConnection().getTable(TableName.valueOf(TABLE));
        Result[] results = table.get(gets);
        for(Result result : results){
            if (null != result.getRow()) {
                Cell[] cells = result.rawCells();
                System.out.println("rowkey="+ Bytes.toString(result.getRow()));
                for (Cell cell : cells) {
                    String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String value = Bytes.toString(CellUtil.cloneValue(cell));
                    System.out.println("qualifier="+ qualifier+",value="+value);
                }
            }
        }
    }
}
