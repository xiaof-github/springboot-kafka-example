package com.example.kafkademo.handler;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.*;

@Component
public class KafkaProcess{

    @Value("${spring.brokerList}")
    private String params;

    private int consumerCount = 2;
    private int workerCount = 10;

    private Properties props;

    private KafkaConsumer<String, String> consumer;

    private ThreadPoolExecutor workerThreadPool;


    @PostConstruct
    public void init(){
        //启动线程实例
//        new Thread(this).start();
        // 配置
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.49.131:9092");
        // group
        props.put("group.id", "testggg");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        Collection<String> topics = Collections.singletonList("mytopic");

        ExecutorService fixedThreadPool = Executors.newFixedThreadPool(consumerCount);
        workerThreadPool = new ThreadPoolExecutor(consumerCount, workerCount,
                60000L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(100));

        for (int i = 0; i < consumerCount; i++) {
            consumer = new KafkaConsumer<>(props);
            consumer.subscribe(topics);
            fixedThreadPool.execute(new ConsumerRunner<>(consumer, workerThreadPool));
        }

    }
}
