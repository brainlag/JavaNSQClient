package com.github.brainlag.nsq.rx;

import java.security.SecureRandom;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.WorkQueueProcessor;

/**
 * Attempt to test to see if the work process can be used for queue from async events like NSQ
 */
public class WorkQueueProcessorTest {
    private static final Logger log = LoggerFactory.getLogger(WorkQueueProcessorTest.class);

    @Test
    public void testWorkQueue() throws Exception {

        WorkQueueProcessor<Integer> processor = WorkQueueProcessor.create("work", 8);


        ExecutorService executor = Executors.newFixedThreadPool(5);

        // need to submit a few of these ever
        AtomicInteger val = new AtomicInteger(0);
        for (int i = 0; i < 1; i++) {
            executor.execute(() -> {
                processor.getAvailableCapacity();
                while (true) {
                    log.info("Sending: " + val.get());
                    processor.onNext(val.getAndIncrement());
                }
            });
        }

        Flux<Integer> p = processor.map(i -> {
            log.info("Current Value: " + i);
            return i;
        });

        for (int i = 0; i < 1; i++) {
            p.subscribe(new Subscriber<Integer>() {
                Subscription sub;
                AtomicInteger i = new AtomicInteger();

                @Override
                public void onSubscribe(Subscription s) {
                    this.sub = s;
                    i.set(10_000);
                    s.request(10_000);
                }

                @Override
                public void onNext(Integer integer) {
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    i.decrementAndGet();
                    if (0 == i.get()) {
                        this.sub.request(5);
                    }
                    log.info("Consuming Value: " + integer);
                }

                @Override
                public void onError(Throwable t) {
                    log.info("Error: " + t.getLocalizedMessage());
                }

                @Override
                public void onComplete() {
                    log.info("Complete.");
                }
            });
        }

        Thread.sleep(1000000);
    }
}
