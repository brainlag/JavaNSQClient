package com.github.brainlag.nsq.lookup;

import com.github.brainlag.nsq.ServerAddress;

import java.io.IOException;
import java.util.Set;

public interface NSQLookup {
    Set<ServerAddress> lookup(String topic) throws IOException;

    void addLookupAddress(String addr, int port);
}
