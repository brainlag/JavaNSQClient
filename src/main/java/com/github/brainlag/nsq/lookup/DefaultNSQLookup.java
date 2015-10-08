package com.github.brainlag.nsq.lookup;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.brainlag.nsq.ServerAddress;
import com.google.common.collect.Sets;

import java.io.IOException;
import java.net.URL;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
public class DefaultNSQLookup implements NSQLookup {
    Set<String> addresses = Sets.newHashSet();

    @Override
    public void addLookupAddress(String addr, int port) {
        if (!addr.startsWith("http")) {
            addr = "http://" + addr;
        }
        addr = addr + ":" + port;
        this.addresses.add(addr);
    }

    @Override
    public Set<ServerAddress> lookup(String topic) throws IOException {
        Set<ServerAddress> addresses = Sets.newHashSet();

        for (String addr : this.addresses) {
            try {
                ObjectMapper mapper = new ObjectMapper();

                JsonNode jsonNode = mapper.readTree(new URL(addr + "/lookup?topic=" + topic));
                LogManager.getLogger(this).warn("Server connection information: " + jsonNode.toString());
                JsonNode producers = jsonNode.get("data").get("producers");
                for (JsonNode node : producers) {
                    String host = node.get("broadcast_address").asText();
                    ServerAddress address = new ServerAddress(host, node.get("tcp_port").asInt());
                    addresses.add(address);
                }
            } catch (IOException e) {
                LogManager.getLogger(this).warn("Unable to connect to address " + addr);
            }
        }
        if (addresses.isEmpty()) {
            throw new IOException("Unable to connect to any NSQ Lookup servers, servers tried: " + this.addresses.toString());
        } else {
            return addresses;
        }
    }

    public Set<String> getLookupAddresses() {
        return addresses;
    }
}
