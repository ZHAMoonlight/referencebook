package com.mt.hbase.chpt06.clientapi.filter;

import com.mt.hbase.chpt06.clientapi.BaseDemo;
import com.mt.hbase.connection.HBaseConnectionFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class SingleColumnValueFilterDemo extends BaseDemo {

    private static final String TABLE = "s_behavior";

    private static final String CF_PC="pc";
    private static final String CF_PHONE="ph";

    private static final String COLUMN_VIEW="v";

    private static final String COLUMN_ORDER="o";

    public static void main(String[] args) throws Exception {

        Scan scan = new Scan();
        /**
         *  查询表s_behavior 列限定符'o'，值为1004的数据
         */
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(Bytes.toBytes(CF_PC), Bytes.toBytes(COLUMN_ORDER),
                CompareFilter.CompareOp.EQUAL, Bytes.toBytes("1004"));
        /**
         * 如果数据行不包含列限定符'o' 则不返回该行
         * 如果设置为false，则会返回不包含列限定符'o'的数据行
         */
        singleColumnValueFilter.setFilterIfMissing(true);

        scan.setFilter(singleColumnValueFilter);
        Table table = HBaseConnectionFactory.getConnection().getTable(TableName.valueOf(TABLE));
        ResultScanner resultScanner = table.getScanner(scan);

        printResult(resultScanner);
        /**
         输出结果：
         rowkey=54321000000000000000092233705133122008862
         qualifier=o,value=1004
         qualifier=v,value=1004
         */
    }
}
