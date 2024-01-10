package com.mt.hbase.chpt08.coprocessor;

import com.mt.hbase.connection.HBaseConnectionFactory;
import com.mt.hbase.constants.Constants;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;
import org.apache.hadoop.hbase.client.CoprocessorDescriptor;
import org.apache.hadoop.hbase.client.CoprocessorDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;

/**
 * @author pengxu
 */
public class LoadCoprocessor {

    private static final String CF = "cf";

    public static void main(String[] args) throws IOException {
        doLoadCoprocessor();
    }

    private static void doLoadCoprocessor() throws IOException {
        Connection connection = null;
        Admin admin = null;
        try {
            String path = "hdfs://master2:9000/hadoop/referencebook-1.0.jar";
            connection = HBaseConnectionFactory.getConnection();
            admin = connection.getAdmin();

            TableName tableName = TableName.valueOf("s_behavior3");
            admin.disableTable(tableName);

            TableDescriptorBuilder tableBuilder = TableDescriptorBuilder
                .newBuilder(tableName);

            List<ColumnFamilyDescriptor> familyList = new ArrayList<>();
            ColumnFamilyDescriptor oneFamily = ColumnFamilyDescriptorBuilder
                .newBuilder(CF.getBytes()).build();
            familyList.add(oneFamily);
            tableBuilder.setColumnFamilies(familyList);

            CoprocessorDescriptor coprocessorDescriptor = CoprocessorDescriptorBuilder
                .newBuilder(KeyWordFilterRegionObserver.class.getCanonicalName()).setJarPath(path)
                .setPriority(1001).build();
            tableBuilder.setCoprocessor(coprocessorDescriptor);

            admin.modifyTable(tableBuilder.build());
            admin.enableTable(tableName);
            System.out.println("success");
        } finally {
            if (null != admin) {
                admin.close();
            }
            if (null != connection) {
                connection.close();
            }

        }
    }
}
