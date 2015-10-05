package com.movableink.nsq.callbacks;

import com.movableink.nsq.exceptions.NSQException;

@FunctionalInterface
public interface NSQErrorCallback {

    void error(NSQException x);
}
