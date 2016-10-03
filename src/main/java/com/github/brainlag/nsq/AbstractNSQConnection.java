package com.github.brainlag.nsq;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.Date;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.AttributeKey;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import com.github.brainlag.nsq.exceptions.NSQException;
import com.github.brainlag.nsq.exceptions.NoConnectionsException;
import com.github.brainlag.nsq.frames.ErrorFrame;
import com.github.brainlag.nsq.frames.MessageFrame;
import com.github.brainlag.nsq.frames.NSQFrame;
import com.github.brainlag.nsq.frames.ResponseFrame;
import com.github.brainlag.nsq.netty.NSQClientInitializer;
import com.google.common.base.Charsets;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Base class for connections.
 */
@Slf4j
public abstract class AbstractNSQConnection implements Closeable {

    public static final byte[] MAGIC_PROTOCOL_VERSION = "  V2".getBytes();
    public static final AttributeKey<AbstractNSQConnection> STATE = AttributeKey.valueOf("RxNSQConnection.state");

    private static EventLoopGroup defaultGroup = null;

    private final NSQConfig config;
    private final ServerAddress address;

    private final LinkedBlockingQueue<NSQCommand> requests = new LinkedBlockingQueue<>(1);
    private final LinkedBlockingQueue<NSQFrame> responses = new LinkedBlockingQueue<>(1);
    private final EventLoopGroup eventLoopGroup;

    private final AtomicLong totalMessages = new AtomicLong(0l);

    private Channel channel;

    public AbstractNSQConnection(final ServerAddress serverAddress,
                                 final NSQConfig config)
            throws NoConnectionsException {
        this.config = config;
        this.address = serverAddress;
        this.eventLoopGroup = config.getEventLoopGroup() != null ? config.getEventLoopGroup() : getDefaultGroup();
    }

    protected void init() {
        val bootstrap = new Bootstrap();
        bootstrap.group(this.eventLoopGroup);
        bootstrap.channel(NioSocketChannel.class);
        bootstrap.handler(new NSQClientInitializer());

        // Start the connection attempt.
        val future = bootstrap.connect(new InetSocketAddress(this.address.getHost(), this.address.getPort()));

        // Wait until the connection attempt succeeds or fails.
        this.channel = future.awaitUninterruptibly().channel();
        if (!future.isSuccess()) {
            throw new NoConnectionsException("Could not connect to server", future.cause());
        }
        log.info("Created connection: {}", this.address);
        this.channel.attr(STATE).set(this);
        final ByteBuf buf = Unpooled.buffer();
        buf.writeBytes(MAGIC_PROTOCOL_VERSION);
        this.channel.write(buf);
        this.channel.flush();

        //identify
        val json = this.config.toJSON();
        val ident = NSQCommand.instance("IDENTIFY", json.getBytes(Charsets.UTF_8));
        try {
            val response = this.commandAndWait(ident);
            if (response != null) {
                log.info("Server identification: {}", ((ResponseFrame) response).getMessage());
            }
        } catch (final TimeoutException e) {
            log.error("Creating connection timed out", e);
            close();
        }
    }

    private synchronized EventLoopGroup getDefaultGroup() {
        if (defaultGroup == null) {
            defaultGroup = new NioEventLoopGroup();
        }
        return defaultGroup;
    }

    public void incoming(final NSQFrame frame) {
        if (frame instanceof ResponseFrame) {
            if ("_heartbeat_".equals(((ResponseFrame) frame).getMessage())) {
                log.trace("HEARTBEAT!");
                command(NSQCommand.instance("NOP"));
                return;
            } else {
                if (!requests.isEmpty()) {
                    try {
                        responses.offer(frame, 20, SECONDS);
                    } catch (final InterruptedException e) {
                        log.error("Thread was interrupted, probably shutting down", e);
                        close();
                    }
                }
                return;
            }
        }

        if (frame instanceof ErrorFrame) {
            processError(NSQException.of((ErrorFrame) frame));
            this.responses.add(frame);
            return;
        }

        if (frame instanceof MessageFrame) {
            val msg = (MessageFrame) frame;
            val message = NSQMessage.builder()
                    .connection(this)
                    .id(msg.getMessageId())
                    .message(msg.getMessageBody())
                    .timestamp(new Date(NANOSECONDS.toMillis(msg.getTimestamp())))
                    .build();
            this.processMessage(message);
            return;
        }
        log.warn("Unknown frame type: " + frame);
    }

    protected void processMessage(final NSQMessage message) {
        val messagesPerBatch = this.config.getMessagesPerBatch();
        val tot = this.totalMessages.incrementAndGet();
        if (tot % messagesPerBatch > (messagesPerBatch / 2)) {
            //request some more!
            message.rdy(messagesPerBatch);
        }
    }

    protected abstract void processError(NSQException ex);

    @Override
    public void close() {
        log.info("Closing connection: " + this);
        this.channel.disconnect();
    }

    public NSQFrame commandAndWait(final NSQCommand command) throws TimeoutException {
        try {
            if (!requests.offer(command, 15, SECONDS)) {
                throw new TimeoutException("command: " + command + " timedout");
            }

            this.responses.clear(); //clear the response queue if needed.
            final ChannelFuture fut = command(command);

            if (!fut.await(15, SECONDS)) {
                throw new TimeoutException("command: " + command + " timedout");
            }

            final NSQFrame frame = this.responses.poll(15, SECONDS);
            if (frame == null) {
                throw new TimeoutException("command: " + command + " timedout");
            }

            this.requests.poll(); //clear the request object
            return frame;
        } catch (final InterruptedException e) {
            close();
            log.warn("Thread was interrupted!", e);
        }
        return null;
    }

    public ChannelFuture command(final NSQCommand command) {
        return this.channel.writeAndFlush(command);
    }

    public boolean isConnected() {
        return this.channel.isActive();
    }

    public ServerAddress getServerAddress() {
        return this.address;
    }

    public NSQConfig getConfig() {
        return this.config;
    }

    @Override
    public String toString() {
        return "Connection: " + this.getServerAddress().toString();
    }

    @Override
    public int hashCode() {
        return getServerAddress().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof AbstractNSQConnection)) {
            return false;
        }
        val other = (AbstractNSQConnection) obj;
        return other.address.equals(this.address);
    }
}
