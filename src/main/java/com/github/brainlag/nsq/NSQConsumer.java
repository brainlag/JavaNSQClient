package com.github.brainlag.nsq;

import lombok.extern.slf4j.Slf4j;
import lombok.val;

import com.github.brainlag.nsq.callbacks.NSQErrorCallback;
import com.github.brainlag.nsq.callbacks.NSQMessageCallback;
import com.github.brainlag.nsq.exceptions.NoConnectionsException;
import com.github.brainlag.nsq.lookup.NSQLookup;

@Slf4j
public class NSQConsumer extends AbstractNSQConsumer {
    private final NSQMessageCallback callback;
    private final NSQErrorCallback errorCallback;

    public NSQConsumer(final NSQLookup lookup,
                       final String topic,
                       final String channel,
                       final NSQMessageCallback callback,
                       final NSQConfig config,
                       final NSQErrorCallback errCallback) {
        super(lookup, topic, channel, config);
        this.callback = callback;
        this.errorCallback = errCallback;
    }

    public NSQConsumer start() {
        startLookupSchedule();
        return this;
    }

    protected NSQConnection createConnection(final ServerAddress serverAddress) {
        try {
            val conn = new NSQConnection(serverAddress, this.config, this.callback, this.errorCallback);
            conn.command(NSQCommand.instance("SUB " + this.topic + " " + this.channel));
            conn.command(NSQCommand.instance("RDY " + this.config.getMessagesPerBatch()));
            return conn;
        } catch (final NoConnectionsException e) {
            log.warn("Could not create connection to server {} - {}", serverAddress, e.getMessage());
            return null;
        }
    }
}
