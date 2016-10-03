package com.github.brainlag.nsq.rx;

import reactor.core.publisher.WorkQueueProcessor;

import com.github.brainlag.nsq.AbstractNSQConnection;
import com.github.brainlag.nsq.NSQConfig;
import com.github.brainlag.nsq.NSQMessage;
import com.github.brainlag.nsq.ServerAddress;
import com.github.brainlag.nsq.exceptions.NSQException;
import com.github.brainlag.nsq.exceptions.NoConnectionsException;

public class RxNSQConnection extends AbstractNSQConnection {
    private final WorkQueueProcessor processor;

    public RxNSQConnection(final ServerAddress serverAddress,
                           final NSQConfig config,
                           final WorkQueueProcessor processor)
            throws NoConnectionsException {
        super(serverAddress, config);
        this.processor = processor;
        // initialize
        init();
    }

    @Override
    protected void processError(NSQException ex) {
        // send the error
        this.processor.onError(ex);
    }

    @Override
    protected void processMessage(NSQMessage message) {
        super.processMessage(message);
        // send it to the stream
        this.processor.onNext(message);
    }
}
