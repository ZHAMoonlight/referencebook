package com.mt.hbase.chpt11.monitor;

import com.mt.hbase.connection.HBaseConnectionFactory;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;


public class HBaseMonitor {


    public static void main(String [] args) throws IOException {

        Admin admin = HBaseConnectionFactory.getConnection().getAdmin();

        HBaseAdmin hBaseAdmin = (HBaseAdmin) admin;

        ClusterStatus clusterStatus = hBaseAdmin.getClusterStatus();

        /**
         * 获得集群平均Region load，即平均每个RegionServer负责几个Region
         */
        System.out.println(clusterStatus.getAverageLoad());
        /**
         * 获得集群在线的RegionServer
         */
        System.out.println(clusterStatus.getServers());

        /**
         * 获得集群死亡的RegionServer
         */
        System.out.println( clusterStatus.getDeadServers());


    }
}
