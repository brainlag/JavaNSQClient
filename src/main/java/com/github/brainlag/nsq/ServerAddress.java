package com.github.brainlag.nsq;

import lombok.Value;

@Value
public class ServerAddress {

    String host;
    int port;
}
