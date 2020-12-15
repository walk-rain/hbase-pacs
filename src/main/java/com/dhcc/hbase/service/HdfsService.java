package com.dhcc.hbase.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.stereotype.Component;

import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;

@Component
public class HdfsService {
    public boolean putHdfs(String filePath, byte[] bytes) {
        FileSystem fs = null;
        FSDataOutputStream out = null;
        try {
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://wjw.dhcc.com:9000");
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
            conf.set("fs.defaultFS", "hdfs://wjw.dhcc.com:9000");
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

    public static void main(String[] args) throws IOException {
        //1 创建连接
        Configuration conf = new Configuration();
        //2 连接端口
        conf.set("fs.defaultFS", "hdfs://wjw.dhcc.com:9000");
        //3 获取连接对象
        FileSystem fs = FileSystem.get(conf);
        //本地文件上传到 hdfs
        FileInputStream in = new FileInputStream("D:\\Test\\input\\1.tif");//读取本地文件
        FSDataOutputStream out = fs.create(new Path("/pacs/1.tif"));//在hdfs上创建路径
        byte[] b = new byte[1024*1024];
        int read = 0;
        while((read = in.read(b)) > 0){
            out.write(b, 0, read);
        }
        fs.close();
    }
}
