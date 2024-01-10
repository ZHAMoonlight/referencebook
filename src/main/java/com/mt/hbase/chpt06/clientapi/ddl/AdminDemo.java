package com.mt.hbase.chpt06.clientapi.ddl;

import com.mt.hbase.connection.HBaseConnectionFactory;
import com.mt.hbase.constants.Constants;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.CompactionState;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionInfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * @author pengxu
 */
public class AdminDemo {

    private final static ExecutorService executors = new ThreadPoolExecutor(1, 2, 1000,
        TimeUnit.MILLISECONDS, new SynchronousQueue<Runnable>(), Executors.defaultThreadFactory(),
        new ThreadPoolExecutor.AbortPolicy());


    public void compact(String table) throws Exception {
        Connection connection = null;
        Admin admin = null;
        try {
            connection = HBaseConnectionFactory.getConnection();
            admin = connection.getAdmin();
            Map<String, List<byte[]>> serverMap = new HashMap<String, List<byte[]>>();
            List<RegionInfo> regionInfos = admin.getRegions(TableName.valueOf(table));
            //将表所有的Region按所在RegionServer分组，这样可以并发在每个RegionServer compact一个Region
            for (RegionInfo regionInfo : regionInfos) {
                HRegionLocation regionLocation = MetaTableAccessor
                    .getRegionLocation(HBaseConnectionFactory.getConnection(), regionInfo);
                if (serverMap.containsKey(regionLocation.getHostname())) {
                    serverMap.get(regionLocation.getHostname()).add(regionInfo.getRegionName());
                } else {
                    List<byte[]> list = new ArrayList<byte[]>();
                    list.add(regionInfo.getRegionName());
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
        } finally {
            if (null != admin) {
                admin.close();
            }
            if (null != connection) {
                connection.close();
            }
        }
    }


    static class HBaseCompactThread implements Callable<String> {

        private Map.Entry<String, List<byte[]>> entry;

        public HBaseCompactThread(Map.Entry<String, List<byte[]>> entry) {
            this.entry = entry;
        }

        @Override
        public String call() throws Exception {
            Admin admin = null;
            try {
                admin = HBaseConnectionFactory.getConnection().getAdmin();
                for (byte[] bytes : entry.getValue()) {
                    CompactionState state = admin.getCompactionStateForRegion(bytes);
                    //如果Region当前状态不为主压缩，则触发主压缩
                    if (state != CompactionState.MAJOR) {
                        admin.majorCompactRegion(bytes);
                    }
                    while (true) {
                        //休眠等待当前Region compact结束，以免compact过多Region造成RegionServer压力过大
                        Thread.sleep(30 * 1000);
                        state = admin.getCompactionStateForRegion(bytes);

                        if (state == CompactionState.NONE) {
                            break;
                        }
                    }
                }
                return entry.getKey() + " success";
            } finally {
                if (null != admin) {
                    admin.close();
                }
            }
        }
    }


    public static void main(String[] args) throws Exception {
        AdminDemo adminDemo = new AdminDemo();
        adminDemo.compact(Constants.TABLE);
    }
}
