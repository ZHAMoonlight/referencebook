package com.mt.hbase.connection;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;


public class HBaseConnectionFactory {

    private static Connection connection = null;

    static{
        createConnection();
    }

    private static synchronized void createConnection() {

        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.client.pause", "100");
        configuration.set("hbase.client.write.buffer", "10485760");
        configuration.set("hbase.client.retries.number", "5");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.client.scanner.timeout.period", "100000");
        configuration.set("hbase.rpc.timeout", "40000");
        configuration.set("hbase.zookeeper.quorum", "master1,master2,slave1");

        try {
            connection = ConnectionFactory.createConnection(configuration);
        }catch(IOException ioException){
            throw new RuntimeException(ioException);
        }
    }

    public static Connection getConnection() {
        return connection;
    }


}
