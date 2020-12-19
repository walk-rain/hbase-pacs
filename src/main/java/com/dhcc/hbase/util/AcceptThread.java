package com.dhcc.hbase.util;

import com.dhcc.hbase.service.HBaseService;
import com.dhcc.hbase.service.HdfsService;

import java.io.*;

public class AcceptThread extends Thread {
    private InputStream inputStream;

    private HBaseService hBaseService;

    private HdfsService hdfsService;

    public AcceptThread(InputStream inputStream, HBaseService hBaseService, HdfsService hdfsService) {
        this.inputStream = inputStream;
        this.hBaseService = hBaseService;
        this.hdfsService = hdfsService;
    }

    @Override
    public void run() {
        try {
            //使用DataInputStream包装输入流
            DataInputStream dataInputStream = new DataInputStream(inputStream);

            String fileName = dataInputStream.readUTF();
            System.out.println(fileName);//在控制台显示文件名

            //往某个位置写入文件
            //FileOutputStream fileOutputStream = new FileOutputStream("D:\\Test\\output" + File.separator + fileName);
            ByteArrayOutputStream bAOutputStream = new ByteArrayOutputStream();
            int c = -1;
            while ((c = dataInputStream.read()) != -1) {
                //fileOutputStream.write(c);
                //fileOutputStream.flush();
                bAOutputStream.write(c);
            }
            byte[] bytes = bAOutputStream.toByteArray();
            //if (bytes.length>=10240000) {
            if (bytes.length>=307200) {
                String hdfsFileName = "/pacs/" + fileName;
                hdfsService.putHdfs(hdfsFileName,bytes);
                //hBaseService.putData("test:pacs_f2",fileName,"f1",new String[]{"a1","a2"},new String[]{"1",hdfsFileName});
            } else {
                hBaseService.putPacs("test:pacs_f2",fileName,"f1",new String[]{"a1"},new String[]{"1"},"f2",bytes);
            }

            System.out.println("文件上传成功！");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
