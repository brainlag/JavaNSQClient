package com.github.brainlag.nsq.lookup;

import java.io.IOException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.HashSet;
import java.util.Set;

import lombok.val;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.brainlag.nsq.ServerAddress;
import com.google.common.base.Charsets;

public class DefaultNSQLookup implements NSQLookup {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultNSQLookup.class);

    private final Set<String> addresses = new HashSet<>();
    private final ObjectMapper mapper = new ObjectMapper();

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
        val addresses = new HashSet<ServerAddress>();

        for (val addr : this.addresses) {
            try {
                val topicEncoded = URLEncoder.encode(topic, Charsets.UTF_8.name());
                val jsonNode = mapper.readTree(new URL(addr + "/lookup?topic=" + topicEncoded));
                LOG.debug("Server connection information: {}", jsonNode.toString());
                val producers = jsonNode.get("data").get("producers");
                for (val node : producers) {
                    val host = node.get("broadcast_address").asText();
                    val address = new ServerAddress(host, node.get("tcp_port").asInt());
                    addresses.add(address);
                }
            } catch (IOException e) {
                LOG.warn("Unable to connect to address {}", addr);
                LOG.debug("Failed to lookup topic.", e);
            }
        }
        if (addresses.isEmpty()) {
            LOG.warn("Unable to connect to any NSQ Lookup servers, servers tried: {}", this.addresses);
        }
        return addresses;
    }
}
