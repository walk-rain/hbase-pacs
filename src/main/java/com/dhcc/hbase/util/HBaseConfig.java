package com.dhcc.hbase.util;

import com.dhcc.hbase.config.HadoopConfig;
import com.dhcc.hbase.service.HBaseService;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class HBaseConfig {

    @Autowired
    private HadoopConfig hadoopConfig;

    @Bean
    public HBaseService getHbaseService() {
        org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", hadoopConfig.getZkQuorum());
        config.set("hbase.zookeeper.property.clientPort", hadoopConfig.getZkPort());
        return new HBaseService(config);
    }
}