package com.dianrong.honeycomb.canalparser.listener;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.concurrent.CountDownLatch;

/**
 * Created by guoyubo on 2017/5/12.
 */
@Service
public class KafkaConsumer {

    public CountDownLatch getCountDownLatch1() {
        return countDownLatch1;
    }

    private CountDownLatch countDownLatch1 = new CountDownLatch(1);

    @KafkaListener(containerFactory = "kafkaListenerContainerFactory", topics = "canal")
    public void listen(String message) {
        System.out.println("Received message: " + message);

        countDownLatch1.countDown();
    }

}
