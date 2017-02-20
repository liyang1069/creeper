package com.creeper;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ImportResource;

import java.util.concurrent.CountDownLatch;

@SpringBootApplication
@ImportResource({"classpath:spring-dubbo.xml"})
public class CreeperApplication {

    @Bean
    public CountDownLatch closeLatch() {
        return new CountDownLatch(1);
    }
    
	public static void main(String[] args) throws InterruptedException {
		//SpringApplication.run(CreeperApplication.class, args);
		ApplicationContext ctx = new SpringApplicationBuilder().sources(CreeperApplication.class).web(false).run(args);

        System.out.println("项目启动!");

        CountDownLatch closeLatch = ctx.getBean(CountDownLatch.class);
        closeLatch.await();
	}
}
