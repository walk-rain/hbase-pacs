package com.dhcc.hbase.util;

import java.io.*;
import java.net.Socket;

public class SocketGetClient {
    public static void main(String[] args) {
        String fileName = "195251-1608033171c818.jpg";
        try(Socket socket = new Socket("139.9.115.81", 2323)) {

            OutputStream outputStream = socket.getOutputStream();
            //使用DataOutputStream
            DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
            //向服务器端传文件名
            dataOutputStream.writeUTF("r^" + fileName);
            dataOutputStream.flush();//刷新流，传输到服务端

            DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
            FileOutputStream fileOutputStream = new FileOutputStream("D:\\Test\\output" + File.separator + fileName);
            byte[] bytes = new byte[1024];
            int len = 0;
            while ((len=dataInputStream.read(bytes))!=-1) {
                fileOutputStream.write(bytes,0,len);
                fileOutputStream.flush();
            }
            fileOutputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}