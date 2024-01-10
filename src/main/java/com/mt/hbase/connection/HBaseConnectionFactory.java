package com.mt.hbase.connection;

import java.util.concurrent.ExecutionException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;
/**
 * @author pengxu
 */
public class HBaseConnectionFactory {

    private static Connection connection = null;

    private static AsyncConnection asycConnection = null;

    static {
        createConnection();
    }

    private static synchronized void createConnection() {

        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.client.pause", "100");
        configuration.set("hbase.client.retries.number", "5");
        configuration.set("hbase.client.write.buffer.autoFlush", "5");
        configuration.set("hbase.client.write.buffer", "10485760");//1024*1024*10=10M
        configuration.set("hbase.client.scanner.timeout.period", "100000");
        configuration.set("hbase.rpc.timeout", "40000");
        /**
         *  设置连接地址
         *  2.x.y 版本使用
         */
        configuration.set("hbase.zookeeper.quorum", "127.0.0.1");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");


        try {
            connection = ConnectionFactory.createConnection(configuration);
            asycConnection = ConnectionFactory.createAsyncConnection().get();
        }  catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Connection getConnection() {
        return connection;
    }

    public static AsyncConnection getAsycConnection() {
        return asycConnection;
    }

}
