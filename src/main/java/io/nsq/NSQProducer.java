package io.nsq;

import com.google.common.collect.Sets;
import io.nsq.exceptions.BadMessageException;
import io.nsq.exceptions.BadTopicException;
import io.nsq.exceptions.NSQException;
import io.nsq.exceptions.NoConnectionsException;
import io.nsq.frames.ErrorFrame;
import io.nsq.frames.NSQFrame;
import io.nsq.pool.ConnectionPoolFactory;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

public class NSQProducer {
    private Set<ServerAddress> addresses = Sets.newConcurrentHashSet();
    private int roundRobinCount = 0;
    private volatile boolean started = false;
    private ExecutorService executor = Executors.newCachedThreadPool();
    private GenericKeyedObjectPoolConfig poolConfig = null;
    private GenericKeyedObjectPool<ServerAddress, Connection> pool;

    /**
     * If no connections are available, will try this many times with 5 second pause between, before throwing a
     * no connections available exception.
     */
    private int connectionRetries = 5;

    public void start() {
        if (!started) {
            createPool();
            started = true;
        }
    }

    private void createPool() {
        if (poolConfig == null) {
            poolConfig = new GenericKeyedObjectPoolConfig();
            poolConfig.setTestOnBorrow(true);
            poolConfig.setJmxEnabled(false);
        }
        pool = new GenericKeyedObjectPool<>(new ConnectionPoolFactory(getExecutor()), poolConfig);
    }

    protected Connection getConnection() throws NoConnectionsException {
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
        throw new IllegalStateException("No server configured for producer");
    }

    /**
     * produce multiple messages.
     */
    public void produceMulti(String topic, List<byte[]> messages) throws TimeoutException, NSQException {
        if (messages == null || messages.isEmpty()) {
            return;
        }

        if (messages.size() == 1) {
            //encoding will be screwed up if we MPUB a
            this.produce(topic, messages.get(0));
            return;
        }

        Connection c = this.getConnection();

        NSQCommand command = NSQCommand.instance("MPUB " + topic);
        command.setData(messages);


        NSQFrame frame = c.commandAndWait(command);
        if (frame instanceof ErrorFrame) {
            String err = ((ErrorFrame) frame).getErrorMessage();
            if (err.startsWith("E_BAD_TOPIC")) {
                throw new BadTopicException(err);
            }
            if (err.startsWith("E_BAD_MESSAGE")) {
                throw new BadMessageException(err);
            }
        }
    }

    public void produce(String topic, byte[] message) throws NSQException, TimeoutException {
        Connection c = getConnection();
        try {
            NSQCommand command = NSQCommand.instance("PUB " + topic, message);
            NSQFrame frame = c.commandAndWait(command);
            if (frame instanceof ErrorFrame) {
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

    public void addAddress(String host, int port) {
        addresses.add(new ServerAddress(host, port));
    }

    public void removeAddress(String host, int port) {
        addresses.remove(new ServerAddress(host, port));
    }

    public void setPoolConfig(GenericKeyedObjectPoolConfig poolConfig) {
        if (!started) {
            this.poolConfig = poolConfig;
        }
    }

    /**
     * This is the executor where the callbacks happen.
     * The executer can only changed before the client is started.
     * Default is a cached threadpool.
     *
     * @param executor
     */
    public void setExecutor(ExecutorService executor) {
        if (!started) {
            this.executor = executor;
        }
    }

    protected ExecutorService getExecutor() {
        return executor;
    }

    public void shutdown() {
        pool.close();
        executor.shutdown();
    }
}