package com.github.brainlag.nsq.rx;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import reactor.core.publisher.Flux;
import reactor.core.publisher.WorkQueueProcessor;

import com.github.brainlag.nsq.AbstractNSQConnection;
import com.github.brainlag.nsq.AbstractNSQConsumer;
import com.github.brainlag.nsq.NSQCommand;
import com.github.brainlag.nsq.NSQConfig;
import com.github.brainlag.nsq.NSQMessage;
import com.github.brainlag.nsq.ServerAddress;
import com.github.brainlag.nsq.lookup.NSQLookup;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Reactive Streams based consumer with backpressure.
 */
@Slf4j
public class RxNSQConsumer extends AbstractNSQConsumer {

    private final WorkQueueProcessor<NSQMessage> processor;
    private final ExecutorService executorService;

    public RxNSQConsumer(final NSQConfig config,
                         final NSQLookup lookup,
                         final String topic,
                         final String channel) {
        super(lookup, topic, channel, config);
        val cores = Runtime.getRuntime().availableProcessors();
        val threadFactory = new ThreadFactoryBuilder().setNameFormat("consumer-%d").build();
        this.executorService = Executors.newFixedThreadPool(cores, threadFactory);
        this.processor = WorkQueueProcessor.share(this.executorService);
    }

    @Override
    @SneakyThrows
    protected AbstractNSQConnection createConnection(ServerAddress serverAddress) {
        val connection = new RxNSQConnection(serverAddress, this.config, this.processor);
        connection.command(NSQCommand.instance("SUB " + this.topic + " " + this.channel));
        connection.command(NSQCommand.instance("RDY " + this.config.getMessagesPerBatch()));
        return connection;
    }

    /**
     * Start pulling messages from the queue.
     */
    public Flux<NSQMessage> start() {
        this.startLookupSchedule();
        return this.processor;
    }

    @Override
    public void close() throws IOException {
        super.close();
        this.executorService.shutdown();
    }
}
