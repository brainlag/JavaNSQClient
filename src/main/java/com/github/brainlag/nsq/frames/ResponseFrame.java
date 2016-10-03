package com.github.brainlag.nsq.frames;

import java.io.UnsupportedEncodingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResponseFrame extends NSQFrame {
    private static Logger LOG = LoggerFactory.getLogger(ResponseFrame.class);

    public String getMessage() {
        try {
            return new String(getData(), "utf8");
        } catch (UnsupportedEncodingException e) {
            LOG.error("Caught", e);
        }
        return null;
    }

    public String toString() {
        return "RESPONSE: " + this.getMessage();
    }
}
