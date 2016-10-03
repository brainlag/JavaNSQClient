package com.github.brainlag.nsq.pool;

import io.netty.channel.ChannelFuture;
import org.apache.commons.pool2.BaseKeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

import com.github.brainlag.nsq.NSQCommand;
import com.github.brainlag.nsq.NSQConfig;
import com.github.brainlag.nsq.NSQConnection;
import com.github.brainlag.nsq.ServerAddress;
import com.github.brainlag.nsq.callbacks.NSQErrorCallback;
import com.github.brainlag.nsq.callbacks.NSQMessageCallback;

public class ConnectionPoolFactory extends BaseKeyedPooledObjectFactory<ServerAddress, NSQConnection> {
    private NSQConfig config;

    public ConnectionPoolFactory(NSQConfig config) {
        this.config = config;
    }

    @Override
    public NSQConnection create(final ServerAddress serverAddress) throws Exception {
        NSQMessageCallback callback = (message) -> {
        };
        NSQErrorCallback errorCallback = (ex) -> {
        };
        return new NSQConnection(serverAddress, this.config, callback, errorCallback);
    }

    @Override
    public PooledObject<NSQConnection> wrap(final NSQConnection con) {
        return new DefaultPooledObject<>(con);
    }

    @Override
    public boolean validateObject(final ServerAddress key, final PooledObject<NSQConnection> p) {
        ChannelFuture command = p.getObject().command(NSQCommand.instance("NOP"));
        return command.awaitUninterruptibly().isSuccess();
    }

    @Override
    public void destroyObject(final ServerAddress key, final PooledObject<NSQConnection> p) throws Exception {
        p.getObject().close();
    }
}
