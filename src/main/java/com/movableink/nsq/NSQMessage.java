package com.movableink.nsq;

import org.apache.logging.log4j.LogManager;

import java.io.UnsupportedEncodingException;
import java.util.Date;

public class NSQMessage {

    private byte[] id;
    private int attempts;
    private Date timestamp;
    private byte[] message;
    private Connection connection;

    /**
     * Finished processing this message, let nsq know so it doesnt get reprocessed.
     */
    public void finished() {
        try {
            connection.command(NSQCommand.instance("FIN " + new String(id, "ascii")));
        } catch (UnsupportedEncodingException e) {
            LogManager.getLogger(this).error("ASCII charset is not supported by your JVM?", e);
        }
    }

    /**
     * indicates a problem with processing, puts it back on the queue.
     */
    public void requeue(int timeoutMillis) {
        try {
            connection.command(NSQCommand.instance("REQ " + new String(id, "ascii") + " " + timeoutMillis));
        } catch (UnsupportedEncodingException e) {
            LogManager.getLogger(this).error("ASCII charset is not supported by your JVM?", e);
        }
    }

    public void requeue() {
        requeue(0);
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    protected Connection getConnection() {
        return connection;
    }

    public byte[] getId() {
        return id;
    }

    public void setId(byte[] id) {
        this.id = id;
    }

    public int getAttempts() {
        return attempts;
    }

    public void setAttempts(int attempts) {
        this.attempts = attempts;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    public byte[] getMessage() {
        return message;
    }

    public void setMessage(byte[] message) {
        this.message = message;
    }
}
