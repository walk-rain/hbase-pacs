package com.dhcc.hbase.util;

import java.io.*;
import java.net.Socket;

public class SocketGetClient {
    public static void main(String[] args) {
        String fileName = "test3.jpg";
        try(Socket socket = new Socket("localhost", 9093)) {

            OutputStream outputStream = socket.getOutputStream();
            //使用DataOutputStream
            DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
            //向服务器端传文件名
            dataOutputStream.writeUTF(fileName);
            dataOutputStream.flush();//刷新流，传输到服务端

            DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
            FileOutputStream fileOutputStream = new FileOutputStream("D:\\Test\\output" + File.separator + fileName);
            int c = -1;
            while ((c=dataInputStream.read())!=-1) {
                fileOutputStream.write(c);
                fileOutputStream.flush();
            }
            fileOutputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}