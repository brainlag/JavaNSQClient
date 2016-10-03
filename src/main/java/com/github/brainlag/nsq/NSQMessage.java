package com.github.brainlag.nsq;

import java.util.Date;

import lombok.Builder;

import static com.google.common.base.Charsets.US_ASCII;

@Builder
public class NSQMessage {
    byte[] id;
    byte[] message;
    final Date timestamp;

    private final AbstractNSQConnection connection;

    /**
     * Finished processing this message, let nsq know so it doesnt get reprocessed.
     */
    public void finished() {
        this.connection.command(NSQCommand.instance("FIN " + new String(id, US_ASCII)));
    }

    /**
     * indicates a problem with processing, puts it back on the queue.
     */
    public void requeue(int timeoutMillis) {
        this.connection.command(NSQCommand.instance("REQ " + new String(id, US_ASCII) + " " + timeoutMillis));
    }

    /**
     * Default re-queue.
     */
    public void requeue() {
        requeue(0);
    }

    /**
     * Issue the RDY command for more messages.
     */
    public void rdy(int size) {
        this.connection.command(NSQCommand.instance("RDY " + size));
    }

}
