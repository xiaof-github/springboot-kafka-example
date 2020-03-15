package com.example.kafkademo.handler;

import com.example.kafkademo.worker.WorkerRunner;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;

import java.util.Collection;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 消费者实例封装，poll数据到任务线程池
 *
 * @param <K>
 * @param <V>
 */
public class ConsumerRunner<K, V> implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(ConsumerRunner.class);

    /**
     * 是否关闭
     */
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * 消费者实例
     */
    private final KafkaConsumer<K, V> consumer;

    /**
     * 任务处理线程池
     */
    private final ThreadPoolExecutor handlerExecutor;


    public ConsumerRunner(KafkaConsumer<K, V> consumer, ThreadPoolExecutor handlerExecutor) {
        this.consumer = consumer;
        this.handlerExecutor = handlerExecutor;
    }

    @Override
    synchronized public void run() {
        try {
            while (!closed.get()) {
                ConsumerRecords<K, V> records = consumer.poll(200);
                for (ConsumerRecord<K, V> record : records) {
                    // 提交到任务线程池
                    handlerExecutor.execute(new WorkerRunner(record));
                }
            }
        } catch (WakeupException e) {
            // Ignore exception if closing
            if (!closed.get()) {
                throw e;
            }
        } finally {
            consumer.close();
        }
    }

    /**
     * 订阅topic
     *
     * @param topics 主题
     */
    public void subscribe(Collection<String> topics) {
        consumer.subscribe(topics);
    }

    /**
     * Shutdown hook which can be called from a separate thread
     */

    public void shutdown() {
        closed.set(true);

        if (consumer != null) {
            consumer.close();
        }
        if (handlerExecutor != null) {
            handlerExecutor.shutdown();
        }
        try {
            if (!handlerExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                System.out.println("Timeout.... Ignore for this case");
            }
        } catch (InterruptedException ignored) {
            System.out.println("Other thread interrupted this shutdown, ignore for this case.");
            Thread.currentThread().interrupt();
        }
    }


}
