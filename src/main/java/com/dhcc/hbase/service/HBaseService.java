package com.dhcc.hbase.service;

import com.dhcc.hbase.dto.PacsDTO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HBaseService {
    private Logger log = LoggerFactory.getLogger(HBaseService.class);
    /**
     * 管理员可以做表以及数据的增删改查功能
     */
    private Admin admin = null;
    private Connection connection = null;

    @Autowired
    private HdfsService hdfsService;

    public HBaseService(Configuration conf) {
        try {
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
        } catch (IOException e) {
            log.error("获取HBase连接失败!");
        }
    }

    /**
     * 创建表 create <table>, {NAME => <column family>, VERSIONS => <VERSIONS>}
     */
    public boolean creatTable(String tableName, List<String> columnFamily) {
        try {
            //表 table
            HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));

            //列族column family
            columnFamily.forEach(cf -> {
                HColumnDescriptor hcd = new HColumnDescriptor(cf);
                //hcd.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);
                //hcd.setCompressionType(Compression.Algorithm.SNAPPY);
                htd.addFamily(hcd);
            });

            if (admin.tableExists(TableName.valueOf(tableName))) {
                log.debug("table Exists!");
            } else {
                admin.createTable(htd);
                log.debug("create table Success!");
            }
        } catch (IOException e) {
            log.error(MessageFormat.format("创建表{0}失败", tableName), e);
            return false;
        } finally {
            close(admin, null, null);
        }
        return true;
    }

    /**
     * 查询所有表的表名
     */
    public List<String> getAllTableNames() {
        List<String> result = new ArrayList<>();
        try {
            TableName[] tableNames = admin.listTableNames();
            for (TableName tableName : tableNames) {
                result.add(tableName.getNameAsString());
            }
        } catch (IOException e) {
            log.error("获取所有表的表名失败", e);
        } finally {
            close(admin, null, null);
        }
        return result;
    }

    /**
     * 遍历查询指定表中的所有数据
     */
    public Map<String, Map<String, String>> getResultScanner(String tableName) {
        Scan scan = new Scan();
        return this.queryData(tableName, scan);
    }

    /**
     * 通过表名及过滤条件查询数据
     */
    private Map<String, Map<String, String>> queryData(String tableName, Scan scan) {
        // <rowKey,对应的行数据>
        Map<String, Map<String, String>> result = new HashMap<>();
        ResultScanner rs = null;
        //获取表
        Table table = null;
        try {
            table = getTable(tableName);
            rs = table.getScanner(scan);
            for (Result r : rs) {
                // 每一行数据
                Map<String, String> columnMap = new HashMap<>();
                String rowKey = null;
                // 行键，列族和列限定符一起确定一个单元（Cell）
                for (Cell cell : r.listCells()) {
                    if (rowKey == null) {
                        rowKey = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                    }
                    columnMap.put(
                            //列限定符
                            Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()),
                            //列族
                            Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                }
                if (rowKey != null) {
                    result.put(rowKey, columnMap);
                }
            }
        } catch (IOException e) {
            log.error(MessageFormat.format("遍历查询指定表中的所有数据失败,tableName:{0}", tableName), e);
        } finally {
            close(null, rs, table);
        }
        return result;
    }

    /**
     * 为表添加或者更新数据
     */
    public void putData(String tableName, String rowKey, String familyName, String[] columns, String[] values) {
        Table table = null;
        try {
            table = getTable(tableName);
            putData(table, rowKey, tableName, familyName, columns, values);
        } catch (Exception e) {
            log.error(MessageFormat.format("为表添加 or 更新数据失败,tableName:{0},rowKey:{1},familyName:{2}", tableName, rowKey, familyName), e);
        } finally {
            close(null, null, table);
        }
    }

    public void putPacs(String tableName, String rowKey, String familyName1, String[] columns, String[] values, String familyName2, byte[] fileContent) {
        Table table = null;
        try {
            table = getTable(tableName);
            try {
                //设置rowkey
                Put put = new Put(Bytes.toBytes(rowKey));
                if (columns != null && values != null && columns.length == values.length) {
                    for (int i = 0; i < columns.length; i++) {
                        if (columns[i] != null && values[i] != null) {
                            put.addColumn(Bytes.toBytes(familyName1), Bytes.toBytes(columns[i]), Bytes.toBytes(values[i]));
                        }
                    }
                }
                put.addColumn(Bytes.toBytes(familyName2),Bytes.toBytes("c1"),fileContent);
                table.put(put);
                log.debug("putData add or update data Success,rowKey:" + rowKey);
                table.close();
            } catch (Exception e) {
                log.error(MessageFormat.format(
                        "为表添加 or 更新数据失败,tableName:{0},rowKey:{1},familyName:{2}",
                        tableName, rowKey, familyName1), e);
            }
        } catch (Exception e) {
            log.error(MessageFormat.format("为表添加 or 更新数据失败,tableName:{0},rowKey:{1},familyName:{2}", tableName, rowKey, familyName1), e);
        } finally {
            close(null, null, table);
        }
    }

    private void putData(Table table, String rowKey, String tableName, String familyName, String[] columns, String[] values) {
        try {
            //设置rowkey
            Put put = new Put(Bytes.toBytes(rowKey));
            if (columns != null && values != null && columns.length == values.length) {
                for (int i = 0; i < columns.length; i++) {
                    if (columns[i] != null && values[i] != null) {
                        put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columns[i]), Bytes.toBytes(values[i]));
                    } else {
                        throw new NullPointerException(MessageFormat.format(
                                "列名和列数据都不能为空,column:{0},value:{1}", columns[i], values[i]));
                    }
                }
            }
            table.put(put);
            log.debug("putData add or update data Success,rowKey:" + rowKey);
            table.close();
        } catch (Exception e) {
            log.error(MessageFormat.format(
                    "为表添加 or 更新数据失败,tableName:{0},rowKey:{1},familyName:{2}",
                    tableName, rowKey, familyName), e);
        }
    }

    public PacsDTO getPacs(String tableName, String rowKey, OutputStream outputStream) {
        PacsDTO pacsDTO = new PacsDTO();
        Table table = null;
        DataOutputStream dataOutputStream = null;
        try {
            table = getTable(tableName);
            try {
                Get get = new Get(Bytes.toBytes(rowKey));
                Result set = table.get(get);
                Cell[] cells  = set.rawCells();
                ArrayList<String> columns = new ArrayList<>();
                ArrayList<String> values = new ArrayList<>();
                boolean isHdfs = false;
                String hdfsPath = "";
                for(Cell cell : cells) {
                    String curColumn = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                    int len = cell.getValueLength();
                    byte [] curValue = new byte[len];
                    System.arraycopy(cell.getValueArray(), cell.getValueOffset(), curValue,0, len);
                    if ("a2".equals(curColumn)) {
                        if (len>0) {
                            isHdfs = true;
                            hdfsPath = Bytes.toString(curValue);
                            hdfsService.getHdfs(hdfsPath,outputStream);
                        }
                    }
                    if ("c1".equals(curColumn)) {
                        if ((!isHdfs)&&(len>0)) {
                            dataOutputStream = new DataOutputStream(outputStream);
                            dataOutputStream.write(curValue);
                            dataOutputStream.flush();
                            pacsDTO.setFileContent(curValue);
                        }
                    } else {
                        columns.add(curColumn);
                        values.add(Bytes.toString(curValue));
                    }
                    //System.out.println(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength())+"::"+
                    //        Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                }
                pacsDTO.setColums(columns);
                pacsDTO.setValues(values);
                log.debug("getPacs Success,rowKey:" + rowKey);
                table.close();
            } catch (Exception e) {
                log.error(MessageFormat.format(
                        "获取表数据失败,tableName:{0},rowKey:{1}",
                        tableName, rowKey), e);
            }
        } catch (Exception e) {
            log.error(MessageFormat.format("获取表数据失败,tableName:{0},rowKey:{1}", tableName, rowKey), e);
        } finally {
            close(null, null, table);
            if (dataOutputStream!=null) {
                try {
                    dataOutputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return pacsDTO;
    }

    /**
     * 根据表名获取table
     */
    private Table getTable(String tableName) throws IOException {
        return connection.getTable(TableName.valueOf(tableName));
    }

    /**
     * 关闭流
     */
    private void close(Admin admin, ResultScanner rs, Table table) {
        if (admin != null) {
            try {
                admin.close();
            } catch (IOException e) {
                log.error("关闭Admin失败", e);
            }
            if (rs != null) {
                rs.close();
            }
            if (table != null) {
                rs.close();
            }
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    log.error("关闭Table失败", e);
                }
            }
        }
    }
}