package com.mt.hbase.chpt06.clientapi.ddl;

import com.mt.hbase.connection.HBaseConnectionFactory;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class AdminDemo {

    private static ExecutorService executors = Executors.newFixedThreadPool(20);


    public void compact(String table) throws Exception {
        Admin admin = HBaseConnectionFactory.getConnection().getAdmin();

        Map<String, List<byte[]>> serverMap = new HashMap<String, List<byte[]>>();
        List<HRegionInfo> regionInfos = admin.getTableRegions(TableName.valueOf(table));
        //将表所有的Region按所在RegionServer分组，这样可以并发在每个RegionServer compact一个Region
        for (HRegionInfo hRegionInfo : regionInfos) {
            HRegionLocation regionLocation = MetaTableAccessor
                    .getRegionLocation(HBaseConnectionFactory.getConnection(), hRegionInfo);
            if (serverMap.containsKey(regionLocation.getHostname())) {
                serverMap.get(regionLocation.getHostname()).add(hRegionInfo.getRegionName());
            } else {
                List<byte[]> list = new ArrayList<byte[]>();
                list.add(hRegionInfo.getRegionName());
                serverMap.put(regionLocation.getHostname(), list);
            }
        }
        List<Future<String>> futures = new ArrayList<Future<String>>();
        //为每个RegionServer compact一个Region
        for (Map.Entry<String, List<byte[]>> entry : serverMap.entrySet()) {
            futures.add(executors.submit(new HBaseCompactThread(entry)));
        }
        for (Future<String> future : futures) {
            System.out.println("compact results: " + future.get());
        }
    }


    class HBaseCompactThread implements Callable<String> {

        private Map.Entry<String, List<byte[]>> entry;

        public HBaseCompactThread(Map.Entry<String, List<byte[]>> entry) {
            this.entry = entry;
        }


        public String call() throws Exception {
            Admin admin = HBaseConnectionFactory.getConnection().getAdmin();

            for (byte[] bytes : entry.getValue()) {
                AdminProtos.GetRegionInfoResponse.CompactionState state = admin
                        .getCompactionStateForRegion(bytes);
                //如果Region当前状态不为主压缩，则触发主压缩
                if (state != AdminProtos.GetRegionInfoResponse.CompactionState.MAJOR) {
                    admin.majorCompactRegion(bytes);
                }
                while (true) {
                    //休眠等待当前Region compact结束，以免compact过多Region造成RegionServer压力过大
                    Thread.sleep(3 * 60 * 1000);
                    state = admin.getCompactionStateForRegion(bytes);

                    if (state == AdminProtos.GetRegionInfoResponse.CompactionState.NONE) {
                        break;
                    }
                }
            }
            return entry.getKey() + " success";
        }
    }


    public static void main(String[] args) throws Exception {
        AdminDemo adminDemo = new AdminDemo();
        adminDemo.compact("s_behavior");
    }
}
