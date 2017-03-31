package com.github.brainlag.nsq.lookup;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.brainlag.nsq.ServerAddress;
import com.google.common.base.Charsets;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;

import java.io.IOException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Set;

public class DefaultNSQLookup implements NSQLookup {
    Set<String> addresses = Sets.newHashSet();
    private final ObjectMapper mapper;

    public DefaultNSQLookup() {
        this(new ObjectMapper());
    }

    public DefaultNSQLookup(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public void addLookupAddress(String addr, int port) {
        if (!addr.startsWith("http")) {
            addr = "http://" + addr;
        }
        addr = addr + ":" + port;
        this.addresses.add(addr);
    }

    @Override
    public Set<ServerAddress> lookup(String topic) {
        Set<ServerAddress> addresses = Sets.newHashSet();

        for (String addr : getLookupAddresses()) {
            try {
                String topicEncoded = URLEncoder.encode(topic, Charsets.UTF_8.name());
                JsonNode jsonNode = mapper.readTree(new URL(addr + "/lookup?topic=" + topicEncoded));
                LogManager.getLogger(this).debug("Server connection information: {}", jsonNode);
                JsonNode producers = jsonNode.findValue("producers");
                for (JsonNode node : producers) {
                    String host = node.get("broadcast_address").asText();
                    ServerAddress address = new ServerAddress(host, node.get("tcp_port").asInt());
                    addresses.add(address);
                }
            } catch (IOException e) {
                LogManager.getLogger(this).warn("Unable to connect to address {} for topic {}", addr, topic);
                LogManager.getLogger(this).debug(e.getMessage());
            }
        }
        if (addresses.isEmpty()) {
            LogManager.getLogger(this).warn("Unable to connect to any NSQ Lookup servers, servers tried: {} on topic: {}", this.addresses, topic);
        }
        return addresses;
    }

    public Set<String> getLookupAddresses() {
        return addresses;
    }
}
