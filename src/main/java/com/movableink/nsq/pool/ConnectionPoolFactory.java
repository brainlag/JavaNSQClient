package com.movableink.nsq.pool;

import com.movableink.nsq.Connection;
import com.movableink.nsq.NSQCommand;
import com.movableink.nsq.NSQConfig;
import com.movableink.nsq.ServerAddress;
import com.movableink.nsq.netty.NSQClientInitializer;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.commons.pool2.BaseKeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

public class ConnectionPoolFactory extends BaseKeyedPooledObjectFactory<ServerAddress, Connection> {
    private Bootstrap bootstrap;
    private NSQConfig config;


    public ConnectionPoolFactory(NSQConfig config) {
        bootstrap = new Bootstrap();
        bootstrap.group(new NioEventLoopGroup());
        bootstrap.channel(NioSocketChannel.class);
        bootstrap.handler(new NSQClientInitializer());
        this.config = config;
    }

    @Override
    public Connection create(final ServerAddress serverAddress) throws Exception {
        return new Connection(serverAddress, config);
    }


    @Override
    public PooledObject<Connection> wrap(final Connection con) {
        return new DefaultPooledObject<>(con);
    }

    @Override
    public boolean validateObject(final ServerAddress key, final PooledObject<Connection> p) {
        ChannelFuture command = p.getObject().command(NSQCommand.instance("NOP"));
        return command.awaitUninterruptibly().isSuccess();
    }

    @Override
    public void destroyObject(final ServerAddress key, final PooledObject<Connection> p) throws Exception {
        p.getObject().close();
    }
}
