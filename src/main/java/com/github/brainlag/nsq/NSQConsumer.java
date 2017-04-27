package com.github.brainlag.nsq;

import com.github.brainlag.nsq.callbacks.ConnectErrorCallback;
import com.github.brainlag.nsq.callbacks.NSQErrorCallback;
import com.github.brainlag.nsq.callbacks.NSQMessageCallback;
import com.github.brainlag.nsq.exceptions.NoConnectionsException;
import com.github.brainlag.nsq.frames.ErrorFrame;
import com.github.brainlag.nsq.frames.NSQFrame;
import com.github.brainlag.nsq.lookup.NSQLookup;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.jmx.Server;

import java.io.Closeable;
import java.io.IOException;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

public class NSQConsumer implements Closeable {

    private final NSQLookup lookup;
    private final String topic;
    private final String channel;
    private final NSQMessageCallback callback;
    private final NSQErrorCallback errorCallback;
    private final NSQConfig config;
    private volatile long nextTimeout = 0;
    private final Map<ServerAddress, Connection> connections = Maps.newHashMap();
    private final AtomicLong totalMessages = new AtomicLong(0l);
    private final ConnectErrorCallback connectErrorCallback;

    private boolean started = false;
    private int messagesPerBatch;
    private long lookupPeriod = 60 * 1000; // how often to recheck for new nodes (and clean up non responsive nodes)
    private ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private Executor executor = Executors.newCachedThreadPool();
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
        this(lookup, topic, channel, callback, config, errCallback, null);
    }

    public NSQConsumer(final NSQLookup lookup, final String topic, final String channel, final NSQMessageCallback callback,
        final NSQConfig config, final NSQErrorCallback errCallback, final ConnectErrorCallback connectErrorCallback) {
        this.lookup = lookup;
        this.topic = topic;
        this.channel = channel;
        this.config = config;
        this.callback = callback;
        this.errorCallback = errCallback;
        this.messagesPerBatch = config.getMaxInFlight().orElse(200);
        this.connectErrorCallback = connectErrorCallback;
    }

    public NSQConsumer start() {
        if (!started) {
            started = true;
            //connect once otherwise we might have to wait one lookupPeriod
            connect();
            scheduler.scheduleAtFixedRate(() -> connect(), lookupPeriod, lookupPeriod, TimeUnit.MILLISECONDS);
        }
        return this;
    }

    private Connection createConnection(final ServerAddress serverAddress) {
        try {
            final Connection connection = new Connection(serverAddress, config);

            connection.setConsumer(this);
            connection.setErrorCallback(errorCallback);
            connection.command(NSQCommand.subscribe(topic, channel));
            connection.command(NSQCommand.ready(messagesPerBatch));

            return connection;
        } catch (final NoConnectionsException e) {
            LogManager.getLogger(this).warn("Could not create connection to server {}", serverAddress.toString(), e);
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
        message.getConnection().command(NSQCommand.ready(size));
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
        final NSQCommand command = NSQCommand.startClose();
        try {
            for (final Connection connection : connections.values()) {
                final NSQFrame frame = connection.commandAndWait(command);
                if (frame != null && frame instanceof ErrorFrame) {
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


    private void connect() {
        try {
            for (final Iterator<Map.Entry<ServerAddress, Connection>> it = connections.entrySet().iterator(); it.hasNext(); ) {
                Connection cnn = it.next().getValue();
                if (!cnn.isConnected() || !cnn.isHeartbeatStatusOK()) {
                    //force close
                    cnn.close();
                    it.remove();
                }
            }
        } catch (Exception e) {
            // very broad intentionally; that way users of the lib can determine which exceptions they care about
            // and handle them appropriately
            if (connectErrorCallback != null) {
                connectErrorCallback.error(e);
            } else {
                throw e;
            }
        }

        final Set<ServerAddress> newAddresses = lookupAddresses();
        final Set<ServerAddress> oldAddresses = connections.keySet();

        LogManager.getLogger(this).debug("Addresses NSQ connected to: " + newAddresses);
        if (newAddresses.isEmpty()) {
            // in case the lookup server is not reachable for a short time we don't we dont want to
            // force close connection
            // just log a message and keep moving
            LogManager.getLogger(this).warn("No NSQLookup server connections or topic does not exist.");
        } else {
            try {
                ImmutableSet<ServerAddress> addressesToRemove = Sets.difference(oldAddresses, newAddresses).immutableCopy();
                for (final Iterator<ServerAddress> removeIterator = addressesToRemove.iterator(); removeIterator.hasNext(); ) {
                    ServerAddress oldServer = removeIterator.next();
                    LogManager.getLogger(this).info("Remove connection " + oldServer.toString());
                    connections.get(oldServer).close();
                    connections.remove(oldServer);
                }

                ImmutableSet<ServerAddress> addressesToAdd = Sets.difference(newAddresses, oldAddresses).immutableCopy();
                for (final Iterator<ServerAddress> addIterator = addressesToAdd.iterator(); addIterator.hasNext(); ) {
                    ServerAddress server = addIterator.next();
                    if (!connections.containsKey(server)) {
                        final Connection connection = createConnection(server);
                        if (connection != null) {
                            connections.put(server, connection);
                        }
                    }
                }
            } catch (Exception e) {
                // very broad intentionally; that way users of the lib can determine which exceptions they care about
                // and handle them appropriately
                if (connectErrorCallback != null) {
                    connectErrorCallback.error(e);
                } else {
                    throw e;
                }
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
    public NSQConsumer setExecutor(final Executor executor) {
        if (!started) {
            this.executor = executor;
        }
        return this;
    }

    private Set<ServerAddress> lookupAddresses() {
        return lookup.lookup(topic);
    }

    /**
     * This method allows for a runnable task to be scheduled using the NSQConsumer's scheduler executor
     * This is intended for calling a periodic method in a NSQMessageCallback for batching messages
     * without needing state in the callback itself
     *
     * @param task   The Runnable task
     * @param delay  Delay in milliseconds
     * @param period Period of time between scheduled runs
     * @param unit   TimeUnit for delay and period times
     * @return ScheduledFuture - useful for cancelling scheduled task
     */
    public ScheduledFuture scheduleRun(Runnable task, int delay, int period, TimeUnit unit) {
        return scheduler.scheduleAtFixedRate(task, delay, period, unit);
    }

    /**
     * Executor where scheduled callback methods are sent to
     *
     * @param scheduler scheduler to use (defaults to SingleThreadScheduledExecutor)
     * @return this NSQConsumer
     */
    public NSQConsumer setScheduledExecutor(final ScheduledExecutorService scheduler) {
        if (!started) {
            this.scheduler = scheduler;
        }
        return this;
    }

    @Override
    public void close() throws IOException {
        shutdown();
    }
}
