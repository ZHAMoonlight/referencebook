package com.mt.hbase.chpt08.coprocessor;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import com.mt.hbase.chpt08.coprocessor.generated.SumDTO;
import java.util.Collections;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.shaded.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author pengxu
 * @date 2022/2/4
 */
public class SumOrderEndpoint extends SumDTO.SumService implements RegionCoprocessor {

    private RegionCoprocessorEnvironment regionCoprocessorEnvironment;

    @Override
    public void getSum(RpcController controller, SumDTO.SumRequest request,
        RpcCallback<SumDTO.SumResponse> done) {

        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(request.getFamily()));
        scan.addColumn(Bytes.toBytes(request.getFamily()), Bytes.toBytes(request.getColumn()));
        SumDTO.SumResponse response = null;
        InternalScanner scanner = null;
        try {
            scanner = regionCoprocessorEnvironment.getRegion().getScanner(scan);
            List<Cell> results = new ArrayList<Cell>();
            long sum = 0L;
            while (true) {
                boolean hasMore = scanner.next(results);
                for (Cell cell : results) {
                    if (cell.getValueLength() > 0) {
                        String cellValue = Bytes.toString(CellUtil.cloneValue(cell));
                        sum = sum + Long.parseLong(cellValue);
                    }
                }
                results.clear();
                if (!hasMore) {
                    break;
                }
            }
            response = SumDTO.SumResponse.newBuilder().setSum(sum).build();

        } catch (IOException ioe) {
            ResponseConverter.setControllerException(controller, ioe);
        } finally {
            if (scanner != null) {
                try {
                    scanner.close();
                } catch (IOException ignored) {
                }
            }
        }
        done.run(response);
    }


    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        regionCoprocessorEnvironment = (RegionCoprocessorEnvironment) env;
    }

    @Override
    public Iterable<Service> getServices() {
        return Collections.singleton(this);
    }
}
