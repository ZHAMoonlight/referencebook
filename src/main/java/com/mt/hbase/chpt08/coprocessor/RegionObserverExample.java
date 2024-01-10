package com.mt.hbase.chpt08.coprocessor;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author pengxu
 * @Date 2022/1/27.
 */
public class RegionObserverExample implements RegionCoprocessor, RegionObserver {

    private static final byte[] ADMIN = Bytes.toBytes("admin");
    private static final byte[] COLUMN_FAMILY = Bytes.toBytes("details");
    private static final byte[] COLUMN = Bytes.toBytes("Admin_det");
    private static final byte[] VALUE = Bytes.toBytes("You can't see Admin details");

    @Override
    public Optional<RegionObserver> getRegionObserver() {
        return Optional.of(this);
    }

    @Override
    public void preGetOp(final ObserverContext<RegionCoprocessorEnvironment> e, final
    Get get, final List<Cell> results)
        throws IOException {
        if (Bytes.equals(get.getRow(), ADMIN)) {
            Cell c = CellUtil.createCell(get.getRow(), COLUMN_FAMILY, COLUMN,
                System.currentTimeMillis(), (byte) 4, VALUE);
            results.add(c);
            e.bypass();
        }
    }

    @Override
    public void preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> e, Scan scan)
        throws IOException {
        Filter filter = new RowFilter(CompareOperator.NOT_EQUAL, new BinaryComparator(ADMIN));
        scan.setFilter(filter);
    }

    @Override
    public boolean postScannerNext(final ObserverContext<RegionCoprocessorEnvironment> e,
        final InternalScanner s,
        final List<Result> results, final int limit, final boolean hasMore) throws IOException {
        Result result = null;
        Iterator<Result> iterator = results.iterator();
        while (iterator.hasNext()) {
            result = iterator.next();
            if (Bytes.equals(result.getRow(), ADMIN)) {
                iterator.remove();
                break;
            }
        }
        return hasMore;
    }
}
