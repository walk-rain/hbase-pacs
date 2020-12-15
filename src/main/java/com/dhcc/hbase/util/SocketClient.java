package com.dhcc.hbase.util;

import java.io.*;
import java.net.Socket;

public class SocketClient {
    public static void main(String[] args) {
        //        File[] files = new File("E:\\wan\\testfile").listFiles();
        //如果是多个文件，只需要把文件放入一个list或者数组中，使用for循环把数组的文件全部上传到服务器
        //上传的文件
        File file = new File("D:\\Test\\input\\test2.png");

        try(Socket socket = new Socket("localhost", 9090)) {

            OutputStream outputStream = socket.getOutputStream();
            //使用DataOutputStream
            DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
            //向服务器端传文件名
            dataOutputStream.writeUTF(file.getName());
            dataOutputStream.flush();//刷新流，传输到服务端

            //向服务器端传文件，通过字节流
            //字节流先读取硬盘文件
            BufferedInputStream bufferedInputStream = new BufferedInputStream(new FileInputStream(file));

            int c = -1;
            while ((c=bufferedInputStream.read())!=-1) {
                //将读取的文件以字节方式写入DataOutputStream，之后传输到服务端
                //这里也可以使用byte[]数据进行读取和写入
                dataOutputStream.write(c);
                dataOutputStream.flush();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}