package com.github.brainlag.nsq.lookup;

import java.util.Set;

import com.github.brainlag.nsq.ServerAddress;

public interface NSQLookup {
    Set<ServerAddress> lookup(String topic);

    void addLookupAddress(String addr, int port);
}
