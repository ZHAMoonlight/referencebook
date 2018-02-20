package com.mt.hbase.chpt8.coprocessor;

import com.google.protobuf.ServiceException;
import com.mt.hbase.chpt8.coprocessor.generated.SumDTO;
import com.mt.hbase.connection.HBaseConnectionFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;

import java.io.IOException;
import java.util.Map;

/**
 * Created by pengxu on 2017/10/27.
 */
public class SumOrderEndpointTest {


    public static void main(String [] args) throws IOException {

        TableName tableName = TableName.valueOf("s_order");
        Table table = HBaseConnectionFactory.getConnection().getTable(tableName);

        final SumDTO.SumRequest request = SumDTO.SumRequest.newBuilder().setFamily("cf").setColumn("c")
                .build();
        try {
            Map<byte[], Long> results = table.coprocessorService(SumDTO.SumService.class, null, null,
                    new Batch.Call<SumDTO.SumService, Long>() {
                        @Override
                        public Long call(SumDTO.SumService aggregate) throws IOException {
                            BlockingRpcCallback<SumDTO.SumResponse> rpcCallback = new BlockingRpcCallback<SumDTO.SumResponse>();
                            aggregate.getSum(null, request, rpcCallback);
                            SumDTO.SumResponse response = rpcCallback.get();
                            return response.getSum();
                        }
                    });
            for (Long sum : results.values()) {
                System.out.println("Sum = " + sum);
            }
        } catch (ServiceException e) {
            e.printStackTrace();
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }
}
