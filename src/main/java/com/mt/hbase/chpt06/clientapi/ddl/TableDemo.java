package com.mt.hbase.chpt06.clientapi.ddl;

import com.mt.hbase.connection.HBaseConnectionFactory;
import com.mt.hbase.constants.Constants;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;

/**
 * @author pengxu
 */
public class TableDemo {

    /**
     * 创建表
     *
     * @param tableName 表名
     * @param familyNames 列族名
     */
    public boolean createTable(String tableName, String... familyNames) throws Exception {
        Admin admin = null;
        try {
            admin = HBaseConnectionFactory.getConnection().getAdmin();
            if (admin.tableExists(TableName.valueOf(tableName))) {
                return false;
            }
            //通过TableDescriptor类来描述一个表，ColumnDescriptor描述一个列族
            TableDescriptorBuilder tableBuilder = TableDescriptorBuilder
                .newBuilder(TableName.valueOf(tableName));

            List<ColumnFamilyDescriptor> familyList = new ArrayList<>();
            for (String familyName : familyNames) {
                ColumnFamilyDescriptor oneFamily = ColumnFamilyDescriptorBuilder
                    .newBuilder(familyName.getBytes()).build();
                familyList.add(oneFamily);
            }
            TableDescriptor tableDes = tableBuilder.setColumnFamilies(familyList).build();
            admin.createTable(tableDes);
            return true;
        } finally {
            if (null != admin) {
                admin.close();
            }
        }
    }

    /**
     * 删除表
     *
     * @param tableName 表名
     */
    public boolean deleteTable(String tableName) throws Exception {
        Admin admin = null;
        try {
            admin = HBaseConnectionFactory.getConnection().getAdmin();
            if (!admin.tableExists(TableName.valueOf(tableName))) {
                return false;
            }
            //删除之前要将表disable
            if (!admin.isTableDisabled(TableName.valueOf(tableName))) {
                admin.disableTable(TableName.valueOf(tableName));
            }
            admin.deleteTable(TableName.valueOf(tableName));
            return true;
        } finally {
            if (null != admin) {
                admin.close();
            }
        }
    }


    public static void main(String[] args) throws Exception {

        Calendar calendar = Calendar.getInstance();
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
        TableDemo tableDemo = new TableDemo();

        String tableNameSuffix = format.format(calendar.getTime());
        String tableName = Constants.TABLE + tableNameSuffix;
        //新建表
        System.out.println(
            "创建table结果：" + tableDemo.createTable(tableName, Constants.CF_PC, Constants.CF_PHONE));

        calendar.add(Calendar.MONTH, -1);
        String lastMonthTableNameSuffix = format.format(calendar.getTime());
        String lastMonthTableName = Constants.TABLE + lastMonthTableNameSuffix;
        //删除表，删除之前有disable表
        System.out.println("删除table结果：" + tableDemo.deleteTable(lastMonthTableName));
    }

}
