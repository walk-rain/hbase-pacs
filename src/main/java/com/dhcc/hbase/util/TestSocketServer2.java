package com.dhcc.hbase.util;

import com.dhcc.hbase.service.HBaseService;
import com.dhcc.hbase.service.HdfsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

@Component
public class TestSocketServer2 {
    @Autowired
    private HBaseService hBaseService;

    @Autowired
    private HdfsService hdfsService;

    public void start() {
        try (ServerSocket serverSocket = new ServerSocket(9094)){

            System.out.println("Test2服务器已启动...");

            while (true) {
                System.out.println("等待接收文件");
                //调用了accept方法之后，会一直处于等待接受文件的状态
                Socket socket = serverSocket.accept();//接收客户端传来的数据
                //交给后台线程处理
                new AcceptThread(socket.getInputStream(),hBaseService,hdfsService).start();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
