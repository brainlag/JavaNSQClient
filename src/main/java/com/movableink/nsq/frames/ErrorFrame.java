package com.movableink.nsq.frames;

import org.apache.logging.log4j.LogManager;

import java.io.UnsupportedEncodingException;

public class ErrorFrame extends NSQFrame {

	public String getErrorMessage() {
		try {
			return new String(getData(), "utf8");
		} catch (UnsupportedEncodingException e) {
            LogManager.getLogger(this).error("Caught", e);
		}
		return null;
	}
	
	public String toString() {
		return getErrorMessage();
	}
}
