package com.mt.hbase.chpt08.coprocessor;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author pengxu
 * @date 2022/02/03
 * 该Observer的作用是实现对结果关键字的过滤
 */
public class KeyWordFilterRegionObserver implements RegionCoprocessor, RegionObserver {
    /**
     * 关键词
     */
    private static final String ALIBABA = "阿里巴巴";
    /**
     * 关键词替换字符串
     */
    private static final String MASK = "**";

    @Override
    public Optional<RegionObserver> getRegionObserver() {
        return Optional.of(this);
    }

    @Override
    public void postGetOp(ObserverContext<RegionCoprocessorEnvironment> observerContext,
        Get get, List<Cell> list) throws IOException {
        if (CollectionUtils.isNotEmpty(list)) {
            for (int i = 0; i < list.size(); i++) {
                Cell cell = list.get(i);
                KeyValue keyValue = replaceCell(cell);
                list.set(i, keyValue);
            }
        }
    }

    @Override
    public boolean postScannerNext(
        ObserverContext<RegionCoprocessorEnvironment> observerContext,
        InternalScanner internalScanner, List<Result> list, int i, boolean b)
        throws IOException {
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
        return true;
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
