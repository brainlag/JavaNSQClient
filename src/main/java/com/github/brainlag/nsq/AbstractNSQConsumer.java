package com.github.brainlag.nsq;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;

import lombok.extern.slf4j.Slf4j;
import lombok.val;

import com.github.brainlag.nsq.frames.ErrorFrame;
import com.github.brainlag.nsq.lookup.NSQLookup;
import com.google.common.collect.Maps;

import static com.google.common.collect.Sets.difference;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toSet;

/**
 * Base class for consumers
 */
@Slf4j
public abstract class AbstractNSQConsumer implements Closeable {

    protected final NSQLookup lookup;
    protected final String topic;
    protected final String channel;
    protected final NSQConfig config;
    private boolean started = false;
    private long lookupPeriod = 60 * 1000; // how often to recheck for new nodes (and clean up non responsive nodes)
    private ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final Map<ServerAddress, AbstractNSQConnection> connections = Maps.newHashMap();

    protected AbstractNSQConsumer(NSQLookup lookup, String topic, String channel, NSQConfig config) {
        this.lookup = lookup;
        this.topic = topic;
        this.channel = channel;
        this.config = config;
    }

    protected void startLookupSchedule() {
        if (!started) {
            started = true;
            scheduler.scheduleAtFixedRate(this::connect, 0, this.lookupPeriod, MILLISECONDS);
        }
    }

    protected abstract AbstractNSQConnection createConnection(final ServerAddress serverAddress);

    private void cleanClose() {
        val command = NSQCommand.instance("CLS");
        try {
            for (val connection : this.connections.values()) {
                val frame = connection.commandAndWait(command);
                if (frame != null && frame instanceof ErrorFrame) {
                    val err = ((ErrorFrame) frame).getErrorMessage();
                    if (err.startsWith("E_INVALID")) {
                        throw new IllegalStateException(err);
                    }
                }
            }
        } catch (final TimeoutException e) {
            log.warn("No clean disconnect", e);
        }
    }

    private void connect() {
        this.connections.values()
                .stream()
                .filter(c -> !c.isConnected())
                .map(c -> c.getServerAddress())
                .collect(toSet())
                .forEach(this.connections::remove);

        val newAddresses = lookupAddresses();
        val oldAddresses = this.connections.keySet();

        log.debug("Addresses NSQ connected to: {}", newAddresses);
        if (newAddresses.isEmpty()) {
            log.warn("No NSQLookup server connections or topic does not exist.");
        } else {
            for (val server : difference(oldAddresses, newAddresses)) {
                log.info("Remove connection {}", server.toString());
                this.connections.get(server).close();
                this.connections.remove(server);
            }

            difference(newAddresses, oldAddresses)
                    .stream()
                    .filter(server -> !this.connections.containsKey(server))
                    .map(this::createConnection)
                    .filter(conn -> null != conn)
                    .forEach(conn -> this.connections.put(conn.getServerAddress(), conn));
        }
    }

    private Set<ServerAddress> lookupAddresses() {
        return this.lookup.lookup(this.topic);
    }

    @Override
    public void close() throws IOException {
        this.scheduler.shutdown();
        cleanClose();
    }
}
