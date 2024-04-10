package com.mt.hbase.chpt08.coprocessor;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WALEdit;

/**
 * 该Observer的作用是在s_order表写入的时候， 抽取refund_no、行键写入i_order_refund表建立二级索引
 */
public class IndexRegionObserver implements RegionCoprocessor, RegionObserver {
    private static final String REFUND_NO = "refund_no";
    private static final String INDEX_TABLE_NAME = "i_order_refund";
    private static final String CF = "cf";
    private static final String COLUMN_K = "k";
    @Override
    public Optional<RegionObserver> getRegionObserver() {
        return Optional.of(this);
    }
    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> c, Put put, WALEdit edit)
        throws IOException {
        RegionObserver.super.prePut(c, put, edit);

        // 当一行数据中带有退款单号才写索引
        List<Cell> putRefundCells = put.get(Bytes.toBytes(CF), Bytes.toBytes(REFUND_NO));
        if(putRefundCells==null || putRefundCells.size()==0){
            return;
        }
        // 获取refund_no和行键
        byte[] refundNo = putRefundCells.get(0).getValueArray();
        byte[] rowKey = put.getRow();

        RegionCoprocessorEnvironment env = c.getEnvironment();
        Put indexPut = new Put(Bytes.toBytes(Bytes.toString(refundNo)));
        indexPut.addColumn(Bytes.toBytes(CF), Bytes.toBytes(COLUMN_K), rowKey);
        Table indexTable = env.getConnection().getTable(TableName.valueOf(INDEX_TABLE_NAME));
        indexTable.put(indexPut);
        indexTable.close();
    }

}
