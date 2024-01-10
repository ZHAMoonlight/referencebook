package com.mt.hbase.chpt08.coprocessor;

import com.mt.hbase.connection.HBaseConnectionFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.CoprocessorDescriptor;
import org.apache.hadoop.hbase.client.CoprocessorDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;

/**
 * @author pengxu
 * @date 2022/2/4
 */
public class UnLoadCoprocessor {

    private static final String CF = "cf";

    public static void main(String[] args) throws IOException {
        doUnLoadCoprocessor();
    }
    private static void doUnLoadCoprocessor() throws IOException {
        Connection connection = null;
        Admin admin = null;
        try {
            String path = "hdfs://master1:9000/hadoop/referencebook-1.0.jar";
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
