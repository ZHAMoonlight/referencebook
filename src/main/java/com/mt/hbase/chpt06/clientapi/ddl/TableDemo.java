package com.mt.hbase.chpt06.clientapi.ddl;

import com.mt.hbase.connection.HBaseConnectionFactory;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;

import java.text.SimpleDateFormat;
import java.util.Calendar;

public class TableDemo {

    /**
     * 创建表
     * @param tableName 表名
     * @param familyNames 列族名
     * @return
     */
    public boolean createTable(String tableName, String... familyNames) throws Exception {
        Admin admin = HBaseConnectionFactory.getConnection().getAdmin();
        if (admin.tableExists(TableName.valueOf(tableName))) {
            return false;
        }
        //通过HTableDescriptor类来描述一个表，HColumnDescriptor描述一个列族
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        for (String familyName : familyNames) {
            HColumnDescriptor oneFamily= new HColumnDescriptor(familyName);
            //设置行键编码格式
            oneFamily.setDataBlockEncoding(DataBlockEncoding.PREFIX_TREE);
            //设置数据保留多版本
            oneFamily.setMaxVersions(3);
            tableDescriptor.addFamily(oneFamily);
        }
        //设置
        admin.createTable(tableDescriptor);
        return true;
    }

    /**
     * 删除表
     * @param tableName 表名
     * @return
     */
    public boolean deleteTable(String tableName) throws Exception {
        Admin admin = HBaseConnectionFactory.getConnection().getAdmin();
        if (!admin.tableExists(TableName.valueOf(tableName))) {
            return false;
        }
        //删除之前要将表disable
        if (!admin.isTableDisabled(TableName.valueOf(tableName))) {
            admin.disableTable(TableName.valueOf(tableName));
        }
        admin.deleteTable(TableName.valueOf(tableName));
        return true;
    }


    public static void main(String[] args) throws Exception {

        Calendar calendar = Calendar.getInstance();
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
        TableDemo tableDemo = new TableDemo();
        //新建表
        System.out.println(tableDemo.createTable("s_behavior"+format.format(calendar.getTime()),"pc","ph"));

        calendar.add(Calendar.MONTH,-1);
        //删除表
        System.out.println(tableDemo.deleteTable("s_behavior"+format.format(calendar.getTime())));
    }

}
