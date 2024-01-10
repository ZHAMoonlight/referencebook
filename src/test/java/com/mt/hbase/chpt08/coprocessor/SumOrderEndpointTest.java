package com.mt.hbase.chpt08.coprocessor;

import com.google.protobuf.ServiceException;
import com.mt.hbase.chpt08.coprocessor.generated.SumDTO;
import com.mt.hbase.chpt08.coprocessor.generated.SumDTO.SumResponse;
import com.mt.hbase.connection.HBaseConnectionFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;

import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils;

/**
 * @author pengxu
 */
public class SumOrderEndpointTest {


    public static void main(String[] args) throws IOException {

        /**
         * 测试之前确保存在s_order，可以使用如下命令创建并初始化数据
         * create 's_order', {NAME => 'cf'}
         * put 's_order','row1','cf:c','1'
         * put 's_order','row2','cf:c','2'
         * put 's_order','row3','cf:c','7'
         */
        TableName tableName = TableName.valueOf("s_order");
        Table table = HBaseConnectionFactory.getConnection().getTable(tableName);

        final SumDTO.SumRequest request = SumDTO.SumRequest.newBuilder().setFamily("cf")
            .setColumn("c")
            .build();
        try {
            Map<byte[], Long> results = table
                .coprocessorService(SumDTO.SumService.class, null, null,
                    new Batch.Call<SumDTO.SumService, Long>() {
                        @Override
                        public Long call(SumDTO.SumService aggregate) throws IOException {

                            CoprocessorRpcUtils.BlockingRpcCallback<SumDTO.SumResponse> rpcCallback = new CoprocessorRpcUtils.BlockingRpcCallback<SumDTO.SumResponse>();
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
