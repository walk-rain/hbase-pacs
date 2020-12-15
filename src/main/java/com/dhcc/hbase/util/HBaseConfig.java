package com.dhcc.hbase.util;

import com.dhcc.hbase.service.HBaseService;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class HBaseConfig {
    @Bean
    public HBaseService getHbaseService() {
        //设置临时的hadoop环境变量，之后程序会去这个目录下的\bin目录下找winutils.exe工具，windows连接hadoop时会用到
        //System.setProperty("hadoop.home.dir", "D:\\Program Files\\Hadoop");
        //执行此步时，会去resources目录下找相应的配置文件，例如hbase-site.xml
        org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();// 配置
        //config.set("hbase.zookeeper.quorum", "192.168.58.151");// zookeeper地址
        //config.set("hbase.zookeeper.property.clientPort", "2181");// zookeeper端口
        config.set("hbase.zookeeper.quorum", "192.168.58.155");
        config.set("hbase.zookeeper.property.clientPort", "2182");
        //org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
        return new HBaseService(config);
    }
}