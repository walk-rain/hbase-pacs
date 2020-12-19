package com.dhcc.hbase.service;

import com.dhcc.hbase.config.ServerConfig;
import com.dhcc.hbase.config.SocketProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Component
public class TestRunner implements CommandLineRunner {

    private Logger log = LoggerFactory.getLogger(TestRunner.class);

    @Autowired
    private SocketProperties properties;

    @Autowired
    private PacsService pacsService;

    @Override
    public void run(String... args) throws Exception {
        ServerSocket server = null;
        Socket socket = null;
        server = new ServerSocket(properties.getPort());
        log.info("Socket服务已经开启, 监听端口:" + properties.getPort());
        ThreadPoolExecutor pool = new ThreadPoolExecutor(
                properties.getPoolCore(),
                properties.getPoolMax(),
                properties.getPoolKeep(),
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<Runnable>(properties.getPoolQueueInit()),
                new ThreadPoolExecutor.DiscardOldestPolicy()
        );
        while (true) {
            socket = server.accept();
            pool.execute(new ServerConfig(socket, pacsService));
        }
    }
}