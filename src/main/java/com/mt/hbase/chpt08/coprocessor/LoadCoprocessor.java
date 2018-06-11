package com.mt.hbase.chpt08.coprocessor;

import com.mt.hbase.connection.HBaseConnectionFactory;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;

/**
 * Created by pengxu on 2017/10/30.
 */
public class LoadCoprocessor {

    public static void main(String[] args) throws IOException {

        String path = "hdfs://wxmaster1:9000/hadoop/referencebook-1.0.jar";
        Connection connection =  HBaseConnectionFactory.getConnection();
        Admin admin = connection.getAdmin();

        TableName tableName = TableName.valueOf("s_order");
        admin.disableTable(tableName);
        HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
        HColumnDescriptor cf = new HColumnDescriptor("cf");
        cf.setMaxVersions(2);
        hTableDescriptor.addFamily(cf);

        hTableDescriptor.setValue("COPROCESSOR$1", path + "|"
                + SumOrderEndpoint.class.getCanonicalName() + "|"
                + Coprocessor.PRIORITY_USER);
        admin.modifyTable(tableName, hTableDescriptor);
        admin.enableTable(tableName);
    }
}
