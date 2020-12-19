package com.dhcc.hbase.service;

import com.dhcc.hbase.config.HadoopConfig;
import com.dhcc.hbase.util.FileUtil;
import com.dhcc.hbase.util.TimeUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.*;
import java.util.Date;

@Component
public class HdfsService {
    private Logger log = LoggerFactory.getLogger(HdfsService.class);

    @Autowired
    private HadoopConfig hadoopConfig;

    public boolean putHdfs(String filePath, byte[] bytes) {
        FileSystem fs = null;
        FSDataOutputStream out = null;
        try {
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", hadoopConfig.getDefaultFS());
            fs = FileSystem.get(conf);
            out = fs.create(new Path(filePath));
            out.write(bytes,0,bytes.length);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        } finally {
            if (fs!=null) {
                try {
                    fs.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (out!=null) {
                try {
                    out.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return true;
    }

    public void getHdfs(String filePath, OutputStream outputStream) {
        FileSystem fs = null;
        FSDataInputStream in = null;
        DataOutputStream dataOutputStream = null;
        try {
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", hadoopConfig.getDefaultFS());
            fs = FileSystem.get(conf);
            in = fs.open(new Path(filePath));
            dataOutputStream = new DataOutputStream(outputStream);
            int c = -1;
            while ((c = in.read()) != -1) {
                dataOutputStream.write(c);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (fs!=null) {
                try {
                    fs.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (dataOutputStream!=null) {
                try {
                    dataOutputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public String putPacsFile(String hdfsFileName, InputStream inputStream) {
        FileSystem fs = null;
        FSDataOutputStream out = null;
        DataInputStream dataInputStream = null;
        long totalLength = 0;
        Date startDate = new Date();
        try {
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", hadoopConfig.getDefaultFS());
            fs = FileSystem.get(conf);
            out = fs.create(new Path(hdfsFileName));
            dataInputStream = new DataInputStream(inputStream);
            byte[] bytes = new byte[1024];
            int len = 0;
            while ((len=dataInputStream.read(bytes)) != -1) {
                out.write(bytes,0,len);
                totalLength += len;
            }
        } catch (IOException e) {
            e.printStackTrace();
            log.error("-> -> 上传文件到HDFS失败:" + hdfsFileName);
            return "100^上传文件到HDFS失败";
        } finally {
            if (dataInputStream!=null) {
                try {
                    dataInputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (fs!=null) {
                try {
                    fs.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (out!=null) {
                try {
                    out.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        Date endDate = new Date();
        log.info("-> -> 上传文件到HDFS成功:" + hdfsFileName +
                ", 文件大小：" + FileUtil.getFileScale(totalLength) +
                ", 耗时：" + TimeUtil.getTimeDiff(startDate,endDate));
        return "000^上传文件到HDFS成功";
    }

    public String getPacsFile(String fileName, OutputStream outputStream) {
        FileSystem fs = null;
        FSDataInputStream in = null;
        DataOutputStream dataOutputStream = null;
        long totalLength = 0;
        Date startDate = new Date();
        try {
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", hadoopConfig.getDefaultFS());
            fs = FileSystem.get(conf);
            in = fs.open(new Path(fileName));
            dataOutputStream = new DataOutputStream(outputStream);
            byte[] bytes = new byte[1024];
            int len = 0;
            while ((len=in.read(bytes)) != -1) {
                dataOutputStream.write(bytes,0,len);
                dataOutputStream.flush();
                totalLength += len;
            }
        } catch (IOException e) {
            e.printStackTrace();
            log.error("-> -> 从HDFS下载文件失败:" + fileName);
            return "100^从HDFS下载文件失败";
        } finally {
            if (fs!=null) {
                try {
                    fs.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (dataOutputStream!=null) {
                try {
                    dataOutputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        Date endDate = new Date();
        log.info("-> -> 从HDFS下载文件成功:" + fileName +
                ", 文件大小：" + FileUtil.getFileScale(totalLength) +
                ", 耗时：" + TimeUtil.getTimeDiff(startDate,endDate));
        return "000^从HDFS下载文件成功";
    }
}
