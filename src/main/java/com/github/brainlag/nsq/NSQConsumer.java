package com.github.brainlag.nsq;

import com.github.brainlag.nsq.callbacks.NSQErrorCallback;
import com.github.brainlag.nsq.callbacks.NSQMessageCallback;
import com.github.brainlag.nsq.exceptions.NoConnectionsException;
import com.github.brainlag.nsq.frames.ErrorFrame;
import com.github.brainlag.nsq.frames.NSQFrame;
import com.github.brainlag.nsq.lookup.NSQLookup;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class NSQConsumer {

    private final NSQLookup lookup;
    private final String topic;
    private final String channel;
    private final NSQMessageCallback callback;
    private final NSQErrorCallback errorCallback;
    private final NSQConfig config;
    private volatile long nextTimeout = 0;
    private final Map<ServerAddress, Connection> connections = Maps.newHashMap();
    private final AtomicLong totalMessages = new AtomicLong(0l);

    private boolean started = false;
    private int messagesPerBatch = 200;
    private long lookupPeriod = 60 * 1000; // how often to recheck for new nodes (and clean up non responsive nodes)
    private ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private ExecutorService executor = Executors.newCachedThreadPool();
    private Optional<ScheduledFuture<?>> timeout = Optional.empty();

    public NSQConsumer(final NSQLookup lookup, final String topic, final String channel, final NSQMessageCallback callback) {
        this(lookup, topic, channel, callback, new NSQConfig());
    }

    public NSQConsumer(final NSQLookup lookup, final String topic, final String channel, final NSQMessageCallback callback,
                       final NSQConfig config) {
        this(lookup, topic, channel, callback, config, null);
    }

    public NSQConsumer(final NSQLookup lookup, final String topic, final String channel, final NSQMessageCallback callback,
                       final NSQConfig config, final NSQErrorCallback errCallback) {
        this.lookup = lookup;
        this.topic = topic;
        this.channel = channel;
        this.config = config;
        this.callback = callback;
        this.errorCallback = errCallback;

    }

    public NSQConsumer start() throws IOException {
        if (!started) {
            started = true;
            //connect once otherwise we might have to wait one lookupPeriod
            connect();
            scheduler.scheduleAtFixedRate(() -> {
                try {
                    connect();
                } catch (Throwable t) {
                    //dangerous but do nothing for now
                    //The connect outside of this loop will throw an exception
                }
            }, lookupPeriod, lookupPeriod, TimeUnit.MILLISECONDS);
        }
        return this;
    }

    private Connection createConnection(final ServerAddress serverAddress) {
        try {
            final Connection connection = new Connection(serverAddress, config);

            connection.setConsumer(this);
            connection.setErrorCallback(errorCallback);
            connection.command(NSQCommand.instance("SUB " + topic + " " + this.channel));
            connection.command(NSQCommand.instance("RDY " + messagesPerBatch));

            return connection;
        } catch (final NoConnectionsException e) {
            return null;
        }
    }

    protected void processMessage(final NSQMessage message) {
        if (callback == null) {
            LogManager.getLogger(this).warn("NO Callback, dropping message: " + message);
        } else {
            try {
                executor.execute(() -> callback.message(message));
                if (nextTimeout > 0) {
                    updateTimeout(message, -500);
                }
            } catch (RejectedExecutionException re) {
                LogManager.getLogger(this).trace("Backing off");
                message.requeue();
                updateTimeout(message, 500);
            }
        }

        final long tot = totalMessages.incrementAndGet();
        if (tot % messagesPerBatch > (messagesPerBatch / 2)) {
            //request some more!
            rdy(message, messagesPerBatch);
        }
    }

    private void updateTimeout(final NSQMessage message, long change) {
        rdy(message, 0);
        LogManager.getLogger(this).trace("RDY 0! Halt Flow.");
        if (timeout.isPresent()) {
            timeout.get().cancel(true);
        }
        Date newTimeout = calculateTimeoutDate(change);
        if (newTimeout != null) {
            timeout = Optional.of(scheduler.schedule(() -> {
                rdy(message, 1); // test the waters
            }, 0, TimeUnit.MILLISECONDS));
        }
    }

    private void rdy(final NSQMessage message, int size) {
        message.getConnection().command(NSQCommand.instance("RDY " + size));
    }

    private Date calculateTimeoutDate(final long i) {
        if (System.currentTimeMillis() - nextTimeout + i > 50) {
            nextTimeout += i;
            return new Date();
        } else {
            nextTimeout = 0;
            return null;
        }
    }

    public void shutdown() {
        scheduler.shutdown();
        cleanClose();
    }

    private void cleanClose() {
        final NSQCommand command = NSQCommand.instance("CLS");
        try {
            for (final Connection connection : connections.values()) {
                final NSQFrame frame = connection.commandAndWait(command);
                if (frame instanceof ErrorFrame) {
                    final String err = ((ErrorFrame) frame).getErrorMessage();
                    if (err.startsWith("E_INVALID")) {
                        throw new IllegalStateException(err);
                    }
                }
            }
        } catch (final TimeoutException e) {
            LogManager.getLogger(this).warn("No clean disconnect", e);
        }
    }

    public NSQConsumer setMessagesPerBatch(final int messagesPerBatch) {
        if (!started) {
            this.messagesPerBatch = messagesPerBatch;
        }
        return this;
    }

    public NSQConsumer setLookupPeriod(final long periodMillis) {
        if (!started) {
            this.lookupPeriod = periodMillis;
        }
        return this;
    }


    private void connect() throws IOException {
        for (final Iterator<Map.Entry<ServerAddress, Connection>> it = connections.entrySet().iterator(); it.hasNext(); ) {
            if (!it.next().getValue().isConnected()) {
                it.remove();
            }
        }

        final Set<ServerAddress> newAddresses = lookupAddresses();
        final Set<ServerAddress> oldAddresses = connections.keySet();

        LogManager.getLogger(this).warn("Addresses NSQ connected to: " + newAddresses);
        if (newAddresses.isEmpty()) {
            throw new IOException("No NSQLookup server connections");
        }

        for (final ServerAddress server : Sets.difference(oldAddresses, newAddresses)) {
            connections.get(server).close();
            connections.remove(server);
        }

        for (final ServerAddress server : Sets.difference(newAddresses, oldAddresses)) {
            if (!connections.containsKey(server)) {
                connections.put(server, createConnection(server));
            }
        }
    }

    public long getTotalMessages() {
        return totalMessages.get();
    }

    /**
     * This is the executor where the callbacks happen.
     * The executer can only changed before the client is started.
     * Default is a cached threadpool.
     */
    public NSQConsumer setExecutor(final ExecutorService executor) {
        if (!started) {
            this.executor = executor;
        }
        return this;
    }

    private Set<ServerAddress> lookupAddresses() {
        try {
            return lookup.lookup(topic);
        } catch (final IOException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * This method allows for a runnable task to be scheduled using the NSQConsumer's scheduler executor
     * This is intended for calling a periodic method in a NSQMessageCallback for batching messages
     * without needing state in the callback itself
     *
     * @param task The Runnable task
     * @param delay Delay in milliseconds
     * @param period Period of time between scheduled runs
     * @param unit TimeUnit for delay and period times
     * @return ScheduledFuture - useful for cancelling scheduled task
     */
    public ScheduledFuture scheduleRun(Runnable task, int delay, int period, TimeUnit unit) {
        return scheduler.scheduleAtFixedRate(task, delay, period, unit);
    }

    /**
     * Executor where scheduled callback methods are sent to
     * @param scheduler scheduler to use (defaults to SingleThreadScheduledExecutor)
     * @return this NSQConsumer
     */
    public NSQConsumer setScheduledExecutor(final ScheduledExecutorService scheduler) {
        if (!started) {
            this.scheduler = scheduler;
        }
        return this;
    }
}
