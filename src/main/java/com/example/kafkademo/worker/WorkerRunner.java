package com.example.kafkademo.worker;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;

@Slf4j
public class WorkerRunner<K, V> implements Runnable {
    private ConsumerRecord<K, V> record;

    public WorkerRunner(ConsumerRecord<K, V> record) {
        this.record = record;
    }

    @Override
    public void run() {
        try {
            //todo
            //业务逻辑处理
            log.info("record:{}",record.value());
        } catch (Exception e) {
            log.error("do kafka multi consumer work error! {}", e);
        }
    }
}
