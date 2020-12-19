package com.dhcc.hbase.config;

import com.dhcc.hbase.service.PacsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;
import java.net.SocketException;

public class ServerConfig extends Thread {
    private Logger log = LoggerFactory.getLogger(ServerConfig.class);

    private Socket socket;

    private PacsService pacsService;

    public ServerConfig(Socket socket, PacsService pacsService) {
        this.socket = socket;
        this.pacsService = pacsService;
    }

    private String handle(InputStream inputStream, OutputStream outputStream) {
        DataInputStream dataInputStream = null;
        dataInputStream = new DataInputStream(inputStream);
        String controlStr = null;
        try {
            controlStr = dataInputStream.readUTF();
            log.info("控制信息: " + controlStr);
            String[] info = controlStr.split("\\^");
            if (info.length<2) {
                return "001^参数不能少于2个";
            }
            if ("W".equalsIgnoreCase(info[0])) {
                pacsService.write(info,inputStream);
            } else if ("R".equalsIgnoreCase(info[0])) {
                pacsService.read(info,outputStream);
            } else {
                return "002^类型不正确";
            }
        } catch (IOException e) {
            e.printStackTrace();
            log.error("程序异常");
        } finally {
            if (dataInputStream!=null) {
                try {
                    dataInputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return "000^成功";
    }

    @Override
    public void run() {

        try {
            socket.setSoTimeout(3000);
            log.info("客户 - " + socket.getRemoteSocketAddress() + " -> 机连接成功");
            InputStream inputStream = socket.getInputStream();
            OutputStream outputStream = socket.getOutputStream();
            String result = handle(inputStream, outputStream);
        } catch (SocketException socketException) {
            socketException.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
