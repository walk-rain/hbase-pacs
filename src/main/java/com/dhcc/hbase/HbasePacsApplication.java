package com.dhcc.hbase;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
public class HbasePacsApplication {

    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(HbasePacsApplication.class, args);
        //context.getBean(TestSocketServer.class).start();
    }

}