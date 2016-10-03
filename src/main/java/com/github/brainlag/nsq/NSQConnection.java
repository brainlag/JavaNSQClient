package com.github.brainlag.nsq;

import com.github.brainlag.nsq.callbacks.NSQErrorCallback;
import com.github.brainlag.nsq.callbacks.NSQMessageCallback;
import com.github.brainlag.nsq.exceptions.NSQException;
import com.github.brainlag.nsq.exceptions.NoConnectionsException;

public class NSQConnection extends AbstractNSQConnection {

    private final NSQMessageCallback messageCallback;
    private final NSQErrorCallback errorCallback;

    public NSQConnection(final ServerAddress serverAddress,
                         final NSQConfig config,
                         final NSQMessageCallback messageCallback,
                         final NSQErrorCallback errorCallback)
            throws NoConnectionsException {
        super(serverAddress, config);
        assert messageCallback != null && errorCallback != null;
        this.messageCallback = messageCallback;
        this.errorCallback = errorCallback;

        // make sure that error and message callbacks are set first..
        init();
    }

    protected void processMessage(final NSQMessage message) {
        this.messageCallback.message(message);
        super.processMessage(message);
    }

    @Override
    protected void processError(NSQException x) {
        this.errorCallback.error(x);
    }

}
