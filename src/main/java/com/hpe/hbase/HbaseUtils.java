package com.hpe.hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.*;

public class HbaseUtils {

    private Configuration config;
    private Connection conn;
    private Admin admin;

    /*
    初始化
     */
    public HbaseUtils() {
        config = HBaseConfiguration.create();
        try {

            conn = ConnectionFactory.createConnection(config);
            admin = conn.getAdmin();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void close() {
        try {
            admin.close();
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 建表
     *
     * @param tableName
     * @return
     */
    public int createTable(String tableName, String[] cfs) {
        int status = 0;

        TableName htb = TableName.valueOf(tableName);

        try {
            if (admin.tableExists(htb)) {
                System.out.println("table is exists!");
            } else {
                HTableDescriptor hTableDescriptor = new HTableDescriptor(htb);
                for (String cf : cfs) {
                    HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
                    hTableDescriptor.addFamily(hColumnDescriptor);
                }
                admin.createTable(hTableDescriptor);
                System.out.println("table is created success.");
            }
            status = 1;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return status;
    }

    /***
     *
     * @param tableName
     * @return
     */
    public int dropTable(String tableName) {
        int status = 0;
        try {

            TableName htb = TableName.valueOf(tableName);
            if (admin.tableExists(htb)) {
                admin.disableTable(htb);
                admin.deleteTable(htb);
            }
            status = 1;
            System.out.println("drop table success.");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return status;
    }

    /***
     * 获取表列表
     * @return
     */
    public String[] listTables() {
        ArrayList<String> tables = new ArrayList<String>();
        try {
            HTableDescriptor[] hTableDescriptors = admin.listTables();
            for (HTableDescriptor hTableDescriptor : hTableDescriptors) {
                tables.add(hTableDescriptor.getNameAsString());
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return tables.toArray(new String[tables.size()]);
    }

    /**
     * 插入单条数据
     *
     * @param tableName
     * @param rowKey
     * @param cf
     * @param col
     * @param value
     * @return
     */
    public int insertData(String tableName, String rowKey, String cf, String col, String value) {
        int status = 0;

        try {

            Table table = conn.getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(col), Bytes.toBytes(value));
            table.put(put);
            table.close();
            status = 1;
            System.out.println("one row insert success.");
        } catch (Exception e) {
            e.printStackTrace();
        }

        return status;
    }

    /***
     * 批量插入
     * @param tableName
     * @param rows
     * @return
     */
    public int insertDataBatch(String tableName, List<String[]> rows) {
        int status = 0;

        try {
            Table table = conn.getTable(TableName.valueOf(tableName));
            List<Put> putList = new ArrayList<Put>();
            for (String[] row : rows) {
                Put put = new Put(Bytes.toBytes(row[0]));
                put.addColumn(Bytes.toBytes(row[1]), Bytes.toBytes(row[2]), Bytes.toBytes(row[3]));
                putList.add(put);
            }
            table.put(putList);
            table.close();
            System.out.println("batch row insert success.");
        } catch (Exception e) {
            e.printStackTrace();
        }

        return status;
    }

    /***
     * 删除数据
     * @param tableName
     * @param rowKey
     * @return
     */
    public int delData(String tableName, String rowKey) {
        int status = 0;
        try {
            Table table = conn.getTable(TableName.valueOf(tableName));
            // 删除整行
            Delete deleteAction = new Delete(Bytes.toBytes(rowKey));
            // 删除指定列族
            //deleteAction.addFamily(Bytes.toBytes(colFamily));
            // 删除指定列
            //deleteAction.addColumn(Bytes.toBytes(colFamily),Bytes.toBytes(col));
            table.delete(deleteAction);
            table.close();
            System.out.println("row del success.");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return status;
    }

    /***
     * 批量删除
     * @param tableName
     * @param rows
     * @return
     */
    public int delBatchData(String tableName, Object[] rows) {
        int status = 0;
        /*Table table = conn.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        // 删除指定列族
        // delete.addFamily(Bytes.toBytes(colFamily));
        // 删除指定列
        // delete.addColumn(Bytes.toBytes(colFamily),Bytes.toBytes(col));
        table.delete(delete);
        // 批量删除

         * List<Delete> deleteList = new ArrayList<Delete>();
         * deleteList.add(delete); table.delete(deleteList);

        table.close();*/
        return status;
    }

    /***
     * 更新数据
     * @param tableName
     * @param rowKey
     * @param cf
     * @param col
     * @param value
     * @return
     */
    public int updateData(String tableName, String rowKey, String cf, String col, String value) {
        return insertData(tableName, rowKey, cf, col, value);
    }

    /***
     * get数据
     * @param tableName
     * @param rowkey
     * @return
     */
    public void getData(String tableName, String rowkey) {

        try {
            Table table = conn.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowkey));
            // 获取指定列族数据
            // get.addFamily(Bytes.toBytes(colFamily));
            // 获取指定列数据
            // get.addColumn(Bytes.toBytes(colFamily),Bytes.toBytes(col));
            Result result = table.get(get);

            for (Cell cell : result.rawCells()) {
                System.out.println("RowName:" + new String(CellUtil.cloneRow(cell)) + " ");
                System.out.println("Timetamp:" + cell.getTimestamp() + " ");
                System.out.println("column Family:" + new String(CellUtil.cloneFamily(cell)) + " ");
                System.out.println("row Name:" + new String(CellUtil.cloneQualifier(cell)) + " ");
                System.out.println("value:" + new String(CellUtil.cloneValue(cell)) + " ");
            }
            table.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /***
     *
     * @param tableName
     * @param startRow
     * @param endRow
     * @return
     */
    public void getBatchData(String tableName, String startRow, String endRow) {
        try {
            Table table = conn.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            scan.withStartRow(Bytes.toBytes(startRow));//.setStartRow(Bytes.toBytes(startRow));
            scan.withStopRow(Bytes.toBytes(endRow));//.setStopRow(Bytes.toBytes(stopRow));
            ResultScanner resultScanner = table.getScanner(scan);
            for (Result result : resultScanner) {
                for (Cell cell : result.rawCells()) {
                    System.out.println("RowName:" + new String(CellUtil.cloneRow(cell)) + " ");
                    System.out.println("Timetamp:" + cell.getTimestamp() + " ");
                    System.out.println("column Family:" + new String(CellUtil.cloneFamily(cell)) + " ");
                    System.out.println("row Name:" + new String(CellUtil.cloneQualifier(cell)) + " ");
                    System.out.println("value:" + new String(CellUtil.cloneValue(cell)) + " ");
                }
            }
            table.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
