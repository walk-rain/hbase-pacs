package com.dhcc.hbase.util;

import java.io.*;
import java.net.Socket;

public class SocketClient {
    public static void main(String[] args) {
        //        File[] files = new File("E:\\wan\\testfile").listFiles();
        //如果是多个文件，只需要把文件放入一个list或者数组中，使用for循环把数组的文件全部上传到服务器
        //上传的文件
        File file = new File("D:\\Test\\input\\212516-15666531161ade.jpg");
        long fileLength = file.length();

        try(Socket socket = new Socket("139.9.115.81", 2323)) {

            OutputStream outputStream = socket.getOutputStream();
            //使用DataOutputStream
            DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
            //向服务器端传文件名
            dataOutputStream.writeUTF("w^" + file.getName() + "^" + fileLength);
            dataOutputStream.flush();//刷新流，传输到服务端

            //向服务器端传文件，通过字节流
            //字节流先读取硬盘文件
            BufferedInputStream bufferedInputStream = new BufferedInputStream(new FileInputStream(file));

            byte[] bytes = new byte[1024];
            int len = 0;
            while ((len=bufferedInputStream.read(bytes))!=-1) {
                dataOutputStream.write(bytes,0,len);
                dataOutputStream.flush();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}