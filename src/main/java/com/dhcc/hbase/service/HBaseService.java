package com.dhcc.hbase.service;

import com.dhcc.hbase.config.HadoopConfig;
import com.dhcc.hbase.config.SocketProperties;
import com.dhcc.hbase.dto.PacsDTO;
import com.dhcc.hbase.util.FileUtil;
import com.dhcc.hbase.util.TimeUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.*;
import java.text.MessageFormat;
import java.util.*;

public class HBaseService {
    private Logger log = LoggerFactory.getLogger(HBaseService.class);

    @Autowired
    private HadoopConfig hadoopConfig;

    /**
     * 管理员可以做表以及数据的增删改查功能
     */
    private Admin admin = null;
    private Connection connection = null;

    @Autowired
    private HdfsService hdfsService;

    public HBaseService(Configuration config) {
        try {
            connection = ConnectionFactory.createConnection(config);
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

    public String putPacsIndex(String tableName, String rowKey, List<String> columns, List<String> values) {
        Table table = null;
        try {
            table = getTable(tableName);
            Put put = new Put(Bytes.toBytes(rowKey));
            if (columns != null && values != null && columns.size() == values.size()) {
                for (int i = 0; i < columns.size(); i++) {
                    if (columns.get(i) != null && values.get(i) != null) {
                        put.addColumn(Bytes.toBytes(hadoopConfig.getFamily1()),
                                Bytes.toBytes(columns.get(i)), Bytes.toBytes(values.get(i)));
                    }
                }
            }
            table.put(put);
        } catch (Exception e) {
            log.error(MessageFormat.format("-> -> 保存数据索引到HBase失败：{1}", rowKey), e);
            return "202^保存记录到HBase失败: " + rowKey;
        } finally {
            close(null, null, table);
        }
        log.info("-> -> 保存文件索引到HBase成功:" + rowKey);
        return "000^保存记录到HBase成功: " + rowKey;
    }

    public String putPacsData(String tableName, String rowKey, List<String> columns, List<String> values, InputStream inputStream) {
        Table table = null;
        DataInputStream dataInputStream = null;
        long totalLength = 0;
        Date startDate = new Date();
        try {
            table = getTable(tableName);
            Put put = new Put(Bytes.toBytes(rowKey));
            if (columns != null && values != null && columns.size() == values.size()) {
                for (int i = 0; i < columns.size(); i++) {
                    if (columns.get(i) != null && values.get(i) != null) {
                        put.addColumn(Bytes.toBytes(hadoopConfig.getFamily1()),
                                Bytes.toBytes(columns.get(i)), Bytes.toBytes(values.get(i)));
                    }
                }
            }
            dataInputStream = new DataInputStream(inputStream);
            ByteArrayOutputStream bAOutputStream = new ByteArrayOutputStream();
            byte[] bytes = new byte[1024];
            int len = 0;
            while ((len=dataInputStream.read(bytes)) != -1) {
                bAOutputStream.write(bytes,0,len);
                totalLength += len;
            }
            byte[] fileContent = bAOutputStream.toByteArray();
            put.addColumn(Bytes.toBytes(hadoopConfig.getFamily2()),Bytes.toBytes(hadoopConfig.getColumns2()),fileContent);
            table.put(put);
        } catch (Exception e) {
            log.error(MessageFormat.format("-> -> 保存数据到HBase失败：{1}", rowKey), e);
            return "201^保存记录到HBase失败: " + rowKey;
        } finally {
            close(null, null, table);
            if (dataInputStream!=null) {
                try {
                    dataInputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        Date endDate = new Date();
        log.info("-> -> 保存数据到HBase成功:" + rowKey +
                ", 文件大小：" + FileUtil.getFileScale(totalLength) +
                ", 耗时：" + TimeUtil.getTimeDiff(startDate,endDate));
        return "000^保存记录到HBase成功: " + rowKey;
    }

    public HashMap<String, String> getPacsData(String tableName, String rowKey, OutputStream outputStream) {
        Table table = null;
        DataOutputStream dataOutputStream = null;
        HashMap<String, String> map = new HashMap<>();
        map.put("RES_CODE","000");
        Date startDate = new Date();
        long totalLength = 0;
        try {
            table = getTable(tableName);
            Get get = new Get(Bytes.toBytes(rowKey));
            Result set = table.get(get);
            Cell[] cells  = set.rawCells();
            String family;
            for(Cell cell : cells) {
                map.put("RES_CODE","001");
                family = Bytes.toString(cell.getFamilyArray(),cell.getFamilyOffset(),cell.getFamilyLength());
                if (hadoopConfig.getFamily1().equalsIgnoreCase(family)) {
                    map.put(Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength()),
                            Bytes.toString(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength()));
                } else if (hadoopConfig.getFamily2().equalsIgnoreCase(family)) {
                    int len = cell.getValueLength();
                    totalLength = len;
                    byte [] curValue = new byte[len];
                    System.arraycopy(cell.getValueArray(), cell.getValueOffset(), curValue,0, len);
                    dataOutputStream = new DataOutputStream(outputStream);
                    dataOutputStream.write(curValue);
                    dataOutputStream.flush();
                    map.put("RES_CODE","002");
                }
            }
        } catch (Exception e) {
            log.error(MessageFormat.format("-> -> 读取HBase表数据失败：{1}", rowKey), e);
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
        Date endDate = new Date();
        log.info("-> -> 读取HBase数据成功:" + rowKey +
                ", 文件长度：" + totalLength + "," + FileUtil.getFileScale(totalLength) +
                ", 耗时：" +TimeUtil.getTimeDiff(startDate,endDate));
        return map;
    }
}