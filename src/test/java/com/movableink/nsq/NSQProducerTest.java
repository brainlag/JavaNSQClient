package com.movableink.nsq;

import com.movableink.nsq.exceptions.NSQException;
import com.movableink.nsq.lookup.DefaultNSQLookup;
import com.movableink.nsq.lookup.NSQLookup;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import org.apache.logging.log4j.LogManager;
import org.junit.Test;

import javax.net.ssl.SSLException;
import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class NSQProducerTest {

    private NSQConfig getSnappyConfig() {
        final NSQConfig config = new NSQConfig();
        config.setCompression(NSQConfig.Compression.SNAPPY);
        return config;
    }

    private NSQConfig getDeflateConfig() {
        final NSQConfig config = new NSQConfig();
        config.setCompression(NSQConfig.Compression.DEFLATE);
        config.setDeflateLevel(4);
        return config;
    }

    private NSQConfig getSslConfig() throws SSLException {
        final NSQConfig config = new NSQConfig();
        File serverKeyFile = new File(getClass().getResource("/server.pem").getFile());
        File clientKeyFile = new File(getClass().getResource("/client.key").getFile());
        File clientCertFile = new File(getClass().getResource("/client.pem").getFile());
        SslContext ctx = SslContextBuilder.forClient().sslProvider(SslProvider.OPENSSL).trustManager(serverKeyFile)
                .keyManager(clientCertFile, clientKeyFile).build();
        config.setSslContext(ctx);
        return config;
    }

    private NSQConfig getSslAndSnappyConfig() throws SSLException {
        final NSQConfig config = getSslConfig();
        config.setCompression(NSQConfig.Compression.SNAPPY);
        return config;
    }

    private NSQConfig getSslAndDeflateConfig() throws SSLException {
        final NSQConfig config = getSslConfig();
        config.setCompression(NSQConfig.Compression.DEFLATE);
        config.setDeflateLevel(4);
        return config;
    }

    @Test
    public void testProduceOneMsgSnappy() throws NSQException, TimeoutException, InterruptedException, IOException {
        AtomicInteger counter = new AtomicInteger(0);
        NSQLookup lookup = new DefaultNSQLookup();
        lookup.addLookupAddress("localhost", 4161);

        NSQProducer producer = new NSQProducer();
        producer.setConfig(getSnappyConfig());
        producer.addAddress("localhost", 4150);
        producer.start();
        String msg = randomString();
        producer.produce("test3", msg.getBytes());
        producer.shutdown();

        NSQConsumer consumer = new NSQConsumer(lookup, "test3", "testconsumer", (message) -> {
            LogManager.getLogger(this).info("Processing message: " + new String(message.getMessage()));
            counter.incrementAndGet();
            message.finished();
        }, getSnappyConfig());
        consumer.start();
        while (counter.get() == 0) {
            Thread.sleep(500);
        }
        assertEquals(1, counter.get());
        consumer.shutdown();
    }

    @Test
    public void testProduceOneMsgDeflate() throws NSQException, TimeoutException, InterruptedException, IOException {
        System.setProperty("io.netty.noJdkZlibDecoder", "false");
        AtomicInteger counter = new AtomicInteger(0);
        NSQLookup lookup = new DefaultNSQLookup();
        lookup.addLookupAddress("localhost", 4161);

        NSQProducer producer = new NSQProducer();
        producer.setConfig(getDeflateConfig());
        producer.addAddress("localhost", 4150);
        producer.start();
        String msg = randomString();
        producer.produce("test3", msg.getBytes());
        producer.shutdown();

        NSQConsumer consumer = new NSQConsumer(lookup, "test3", "testconsumer", (message) -> {
            LogManager.getLogger(this).info("Processing message: " + new String(message.getMessage()));
            counter.incrementAndGet();
            message.finished();
        }, getDeflateConfig());
        consumer.start();
        while (counter.get() == 0) {
            Thread.sleep(500);
        }
        assertEquals(1, counter.get());
        consumer.shutdown();
    }

    @Test
    public void testProduceOneMsgSsl() throws InterruptedException, NSQException, TimeoutException, SSLException, IOException {
        AtomicInteger counter = new AtomicInteger(0);
        NSQLookup lookup = new DefaultNSQLookup();
        lookup.addLookupAddress("localhost", 4161);

        NSQProducer producer = new NSQProducer();
        producer.setConfig(getSslConfig());
        producer.addAddress("localhost", 4150);
        producer.start();
        String msg = randomString();
        producer.produce("test3", msg.getBytes());
        producer.shutdown();

        NSQConsumer consumer = new NSQConsumer(lookup, "test3", "testconsumer", (message) -> {
            LogManager.getLogger(this).info("Processing message: " + new String(message.getMessage()));
            counter.incrementAndGet();
            message.finished();
        }, getSslConfig());
        consumer.start();
        while (counter.get() == 0) {
            Thread.sleep(500);
        }
        assertEquals(1, counter.get());
        consumer.shutdown();
    }

    @Test
    public void testProduceOneMsgSslAndSnappy() throws InterruptedException, NSQException, TimeoutException, SSLException, IOException {
        AtomicInteger counter = new AtomicInteger(0);
        NSQLookup lookup = new DefaultNSQLookup();
        lookup.addLookupAddress("localhost", 4161);

        NSQProducer producer = new NSQProducer();
        producer.setConfig(getSslAndSnappyConfig());
        producer.addAddress("localhost", 4150);
        producer.start();
        String msg = randomString();
        producer.produce("test3", msg.getBytes());
        producer.shutdown();

        NSQConsumer consumer = new NSQConsumer(lookup, "test3", "testconsumer", (message) -> {
            LogManager.getLogger(this).info("Processing message: " + new String(message.getMessage()));
            counter.incrementAndGet();
            message.finished();
        }, getSslAndSnappyConfig());
        consumer.start();
        while (counter.get() == 0) {
            Thread.sleep(500);
        }
        assertEquals(1, counter.get());
        consumer.shutdown();
    }

    @Test
    public void testProduceOneMsgSslAndDeflat() throws InterruptedException, NSQException, TimeoutException, IOException {
        System.setProperty("io.netty.noJdkZlibDecoder", "false");
        AtomicInteger counter = new AtomicInteger(0);
        NSQLookup lookup = new DefaultNSQLookup();
        lookup.addLookupAddress("localhost", 4161);

        NSQProducer producer = new NSQProducer();
        producer.setConfig(getSslAndDeflateConfig());
        producer.addAddress("localhost", 4150);
        producer.start();
        String msg = randomString();
        producer.produce("test3", msg.getBytes());
        producer.shutdown();

        NSQConsumer consumer = new NSQConsumer(lookup, "test3", "testconsumer", (message) -> {
            LogManager.getLogger(this).info("Processing message: " + new String(message.getMessage()));
            counter.incrementAndGet();
            message.finished();
        }, getSslAndDeflateConfig());
        consumer.start();
        while (counter.get() == 0) {
            Thread.sleep(500);
        }
        assertEquals(1, counter.get());
        consumer.shutdown();
    }


    @Test
    public void testProduceMoreMsg() throws NSQException, TimeoutException, InterruptedException, IOException {
        AtomicInteger counter = new AtomicInteger(0);
        NSQLookup lookup = new DefaultNSQLookup();
        lookup.addLookupAddress("localhost", 4161);

        NSQConsumer consumer = new NSQConsumer(lookup, "test3", "testconsumer", (message) -> {
            LogManager.getLogger(this).info("Processing message: " + new String(message.getMessage()));
            counter.incrementAndGet();
            message.finished();
        });
        consumer.start();

        NSQProducer producer = new NSQProducer();
        producer.addAddress("localhost", 4150);
        producer.start();
        for (int i = 0; i < 1000; i++) {
            String msg = randomString();
            producer.produce("test3", msg.getBytes());
        }
        producer.shutdown();

        while (counter.get() < 1000) {
            Thread.sleep(500);
        }
        assertTrue(counter.get() >= 1000);
        consumer.shutdown();
    }

    @Test
    public void testParallelProducer() throws NSQException, TimeoutException, InterruptedException, IOException {
        AtomicInteger counter = new AtomicInteger(0);
        NSQLookup lookup = new DefaultNSQLookup();
        lookup.addLookupAddress("localhost", 4161);

        NSQConsumer consumer = new NSQConsumer(lookup, "test3", "testconsumer", (message) -> {
            LogManager.getLogger(this).info("Processing message: " + new String(message.getMessage()));
            counter.incrementAndGet();
            message.finished();
        });
        consumer.start();

        NSQProducer producer = new NSQProducer();
        producer.addAddress("localhost", 4150);
        producer.start();
        for (int n = 0; n < 5; n++) {
            new Thread(() -> {
                for (int i = 0; i < 1000; i++) {
                    String msg = randomString();
                    try {
                        producer.produce("test3", msg.getBytes());
                    } catch (NSQException | TimeoutException e) {
                        Throwables.propagate(e);
                    }
                }
            }).start();
        }
        while (counter.get() < 5000) {
            Thread.sleep(500);
        }
        assertTrue(counter.get() >= 5000);
        producer.shutdown();
        consumer.shutdown();
    }

    @Test
    public void testMulitMessage() throws NSQException, TimeoutException, InterruptedException, IOException {
        AtomicInteger counter = new AtomicInteger(0);
        NSQLookup lookup = new DefaultNSQLookup();
        lookup.addLookupAddress("localhost", 4161);

        NSQConsumer consumer = new NSQConsumer(lookup, "test3", "testconsumer", (message) -> {
            LogManager.getLogger(this).info("Processing message: " + new String(message.getMessage()));
            counter.incrementAndGet();
            message.finished();
        });
        consumer.start();

        NSQProducer producer = new NSQProducer();
        producer.addAddress("localhost", 4150);
        producer.start();
        List<byte[]> messages = Lists.newArrayList();
        for (int i = 0; i < 50; i++) {
            messages.add(randomString().getBytes());
        }
        producer.produceMulti("test3", messages);
        producer.shutdown();

        while (counter.get() < 50) {
            Thread.sleep(500);
        }
        assertTrue(counter.get() >= 50);
        consumer.shutdown();
    }

    @Test
    public void testBackoff() throws InterruptedException, NSQException, TimeoutException, IOException {
        AtomicInteger counter = new AtomicInteger(0);
        NSQLookup lookup = new DefaultNSQLookup();
        lookup.addLookupAddress("localhost", 4161);

        NSQConsumer consumer = new NSQConsumer(lookup, "test3", "testconsumer", (message) -> {
            LogManager.getLogger(this).info("Processing message: " + new String(message.getMessage()));
            counter.incrementAndGet();
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
            message.finished();
        });
        consumer.setExecutor(newBackoffThreadExecutor());
        consumer.start();

        NSQProducer producer = new NSQProducer();
        producer.addAddress("localhost", 4150);
        producer.start();
        for (int i = 0; i < 20; i++) {
            String msg = randomString();
            producer.produce("test3", msg.getBytes());
        }
        producer.shutdown();

        while (counter.get() < 20) {
            Thread.sleep(500);
        }
        assertTrue(counter.get() >= 20);
        consumer.shutdown();
    }

    public static ExecutorService newBackoffThreadExecutor() {
        return new ThreadPoolExecutor(1, 1,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(1));
    }

    private String randomString() {
        return "Message" + new Date().getTime();
    }
}
