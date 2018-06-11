package com.mt.hbase.chpt06.clientapi.dml;

import com.mt.hbase.connection.HBaseConnectionFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class GetDemo {

    private static final String TABLE="s_behavior";

    private static final String CF_PC="pc";
    private static final String CF_PHONE="ph";

    private static final String COLUMN_VIEW="v";

    private static final String COLUMN_ORDER="o";


    public static void main(String[] args) throws IOException, InterruptedException,
            ParseException {
        List<Get> gets = new ArrayList<Get>();
        Get oneGet = new Get(Bytes.toBytes("54321000000000000000092233705146317032071"));
        //设置需要Get的数据列族
        oneGet.addFamily(Bytes.toBytes(CF_PC));
        //设置需要Get的数据列
        //oneGet.addColumn(Bytes.toBytes(CF_PHONE),Bytes.toBytes(COLUMN_ORDER));
        //设置Get的数据时间范围为2018-01-01到现在
        String startS = "2018-01-01";
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Date startDate = dateFormat.parse(startS);
        oneGet.setTimeRange(startDate.getTime(),System.currentTimeMillis());

        //设置Get的数据版本为2
        oneGet.setMaxVersions(2);
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

        /**
         * 输出结果如下所示：
         * rowkey=54321000000000000000092233705146317032071
         * qualifier=o,value=1002
         * qualifier=v,value=1002
         */
    }
}
