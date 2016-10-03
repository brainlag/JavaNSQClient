package com.github.brainlag.nsq;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;

import com.github.brainlag.nsq.exceptions.BadMessageException;
import com.github.brainlag.nsq.exceptions.BadTopicException;
import com.github.brainlag.nsq.exceptions.NSQException;
import com.github.brainlag.nsq.exceptions.NoConnectionsException;
import com.github.brainlag.nsq.frames.ErrorFrame;
import com.github.brainlag.nsq.frames.NSQFrame;
import com.github.brainlag.nsq.pool.ConnectionPoolFactory;
import com.google.common.collect.Sets;

public class NSQProducer {
    private Set<ServerAddress> addresses = Sets.newConcurrentHashSet();
    private int roundRobinCount = 0;
    private volatile boolean started = false;
    private ExecutorService executor = Executors.newCachedThreadPool();
    private GenericKeyedObjectPoolConfig poolConfig = null;
    private GenericKeyedObjectPool<ServerAddress, NSQConnection> pool;
    private NSQConfig config = new NSQConfig();
    private int connectionRetries = 5;

    public NSQProducer start() {
        if (!started) {
            started = true;
            createPool();
        }
        return this;
    }

    private void createPool() {
        if (poolConfig == null) {
            poolConfig = new GenericKeyedObjectPoolConfig();
            poolConfig.setTestOnBorrow(true);
            poolConfig.setJmxEnabled(false);
        }
        pool = new GenericKeyedObjectPool<>(new ConnectionPoolFactory(config), poolConfig);
    }

    protected NSQConnection getConnection() throws NoConnectionsException {
        int c = 0;
        while (c < connectionRetries) {
            ServerAddress[] serverAddresses = addresses.toArray(new ServerAddress[addresses.size()]);
            if (serverAddresses.length != 0) {
                try {
                    return pool.borrowObject(serverAddresses[roundRobinCount++ % serverAddresses.length]);
                } catch (NoSuchElementException e) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ix) {
                        throw new NoConnectionsException("Could not acquire a connection to a server", ix);
                    }
                } catch (Exception ex) {
                    throw new NoConnectionsException("Could not acquire a connection to a server", ex);
                }
            }
        }
        throw new IllegalStateException("No server configured for producer");
    }

    /**
     * produce multiple messages.
     */
    public void produceMulti(String topic, List<byte[]> messages) throws TimeoutException, NSQException {
        if (!started) {
            throw new IllegalStateException("Producer must be started before producing messages!");
        }
        if (messages == null || messages.isEmpty()) {
            return;
        }

        if (messages.size() == 1) {
            //encoding will be screwed up if we MPUB a
            this.produce(topic, messages.get(0));
            return;
        }

        NSQConnection c = this.getConnection();
        try {
            NSQCommand command = NSQCommand.instance("MPUB " + topic);
            command.setData(messages);


            NSQFrame frame = c.commandAndWait(command);
            if (frame != null && frame instanceof ErrorFrame) {
                String err = ((ErrorFrame) frame).getErrorMessage();
                if (err.startsWith("E_BAD_TOPIC")) {
                    throw new BadTopicException(err);
                }
                if (err.startsWith("E_BAD_MESSAGE")) {
                    throw new BadMessageException(err);
                }
            }
        } finally {
            pool.returnObject(c.getServerAddress(), c);
        }
    }

    public void produce(String topic, byte[] message) throws NSQException, TimeoutException {
        if (!started) {
            throw new IllegalStateException("Producer must be started before producing messages!");
        }
        NSQConnection c = getConnection();
        try {
            NSQCommand command = NSQCommand.instance("PUB " + topic, message);
            NSQFrame frame = c.commandAndWait(command);
            if (frame != null && frame instanceof ErrorFrame) {
                String err = ((ErrorFrame) frame).getErrorMessage();
                if (err.startsWith("E_BAD_TOPIC")) {
                    throw new BadTopicException(err);
                }
                if (err.startsWith("E_BAD_MESSAGE")) {
                    throw new BadMessageException(err);
                }
            }
        } finally {
            pool.returnObject(c.getServerAddress(), c);
        }
    }

    public NSQProducer addAddress(String host, int port) {
        addresses.add(new ServerAddress(host, port));
        return this;
    }

    public NSQProducer removeAddress(String host, int port) {
        addresses.remove(new ServerAddress(host, port));
        return this;
    }

    public NSQProducer setPoolConfig(GenericKeyedObjectPoolConfig poolConfig) {
        if (!started) {
            this.poolConfig = poolConfig;
        }
        return this;
    }

    public NSQProducer setConfig(NSQConfig config) {
        if (!started) {
            this.config = config;
        }
        return this;
    }

    protected ExecutorService getExecutor() {
        return executor;
    }

    /**
     * This is the executor where the callbacks happen.
     * The executer can only changed before the client is started.
     * Default is a cached threadpool.
     *
     * @param executor
     */
    public NSQProducer setExecutor(ExecutorService executor) {
        if (!started) {
            this.executor = executor;
        }
        return this;
    }

    public GenericKeyedObjectPool<ServerAddress, NSQConnection> getPool() {
        return pool;
    }

    public void shutdown() {
        started = false;
        pool.close();
        executor.shutdown();
    }
}
