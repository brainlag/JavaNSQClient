package com.github.brainlag.nsq.callbacks;

import java.io.IOException;

/**
 * Functional interface for handling errors encountered during the connection phase.
 *
 * This allows one to listen for exceptions in the `NSQConsumer#connect` operations and react
 * in an appropriate way, including raising exceptions of its own. If an exception is raised, it
 * must be of type IOException
 *
 * @author Josh Wickham
 */
@FunctionalInterface
public interface ConnectErrorCallback {
    void error(Exception e);
}
