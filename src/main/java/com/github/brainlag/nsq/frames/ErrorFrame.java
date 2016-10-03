package com.github.brainlag.nsq.frames;

import java.io.UnsupportedEncodingException;

import org.slf4j.LoggerFactory;

public class ErrorFrame extends NSQFrame {

    public String getErrorMessage() {
        try {
            return new String(getData(), "utf8");
        } catch (UnsupportedEncodingException e) {
            LoggerFactory.getLogger(this.getClass()).error("Caught", e);
        }
        return null;
    }

    public String toString() {
        return getErrorMessage();
    }
}
