package com.example.spring.netty.spring_netty;

import com.example.spring.netty.spring_netty.server.clickhouse.http.ClickhouseHttpNettyProxyServer;
import com.example.spring.netty.spring_netty.server.clickhouse.http.helloworld.HttpHelloWorldServer;
import com.example.spring.netty.spring_netty.server.postgres.tcp.PostgresTcpNettyProxyServer;
import com.example.spring.netty.spring_netty.utils.sql.semantic.SemanticNamesConversion;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;

@Slf4j
@AllArgsConstructor
@EnableConfigurationProperties
@SpringBootApplication
public class App {

    private final ClickhouseHttpNettyProxyServer clickhouseHttpNettyProxyServer;
    private final PostgresTcpNettyProxyServer postgresTcpNettyProxyServer;
    private final SemanticNamesConversion semanticNamesConversion;
    private final HttpHelloWorldServer helloWorldServer;

    public static void main(String[] args) {
        SpringApplication.run(App.class, args);
    }

    /**
     * This can not be implemented with lambda, because of the spring framework limitation
     * (<a href="https://github.com/spring-projects/spring-framework/issues/18681">...</a>)
     */
    @SuppressWarnings({"Convert2Lambda", "java:S1604"})
    @Bean
    public ApplicationListener<ApplicationReadyEvent> readyEventApplicationListener() {
        return new ApplicationListener<ApplicationReadyEvent>() {
            @Override
            public void onApplicationEvent(ApplicationReadyEvent applicationReadyEvent) {
                helloWorldServer.start();
//                clickhouseHttpNettyProxyServer.start();
                postgresTcpNettyProxyServer.start();
                log.info("Semantic conversions: {}", semanticNamesConversion.getConversions());
            }
        };
    }
}
