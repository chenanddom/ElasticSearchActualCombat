package com.es.config;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.InetAddress;
import java.net.UnknownHostException;

@Configuration
public class MyConfig {

    @Bean
    public TransportClient client() throws UnknownHostException {
        Settings settings = Settings.builder().put("cluster.name","chendom").build();
        TransportClient client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"), 9300));
        return client;
    }

}
