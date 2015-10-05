package com.movableink.nsq.callbacks;

import com.movableink.nsq.NSQMessage;

@FunctionalInterface
public interface NSQMessageCallback {

	public void message(NSQMessage message);
}
