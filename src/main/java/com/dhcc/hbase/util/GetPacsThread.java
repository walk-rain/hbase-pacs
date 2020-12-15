package com.dhcc.hbase.util;

import com.dhcc.hbase.dto.PacsDTO;
import com.dhcc.hbase.service.HBaseService;
import com.dhcc.hbase.service.HdfsService;
import com.google.protobuf.ByteString;

import java.io.*;

public class GetPacsThread extends Thread {

    private InputStream inputStream;

    private OutputStream outputStream;

    private HBaseService hBaseService;

    private HdfsService hdfsService;

    public GetPacsThread(InputStream inputStream, OutputStream outputStream, HBaseService hBaseService, HdfsService hdfsService) {
        this.inputStream = inputStream;
        this.outputStream = outputStream;
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

            //DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
            PacsDTO pacsDTO = hBaseService.getPacs("test:pacs_f2",fileName, outputStream);
            //dataOutputStream.write(pacsDTO.getFileContent());
            //dataOutputStream.flush();

            System.out.println("文件下载成功！");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
