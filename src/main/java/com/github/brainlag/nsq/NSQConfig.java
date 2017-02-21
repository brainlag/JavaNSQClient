package com.github.brainlag.nsq;

import com.google.common.base.Preconditions;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.ssl.SslContext;
import org.apache.logging.log4j.LogManager;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Optional;

public class NSQConfig {


    public enum Compression {NO_COMPRESSION, DEFLATE, SNAPPY}

    private String clientId;
    private String hostname;
    private boolean featureNegotiation = true;
    private Integer heartbeatInterval = null;
    private Integer outputBufferSize = null;
    private Integer outputBufferTimeout = null;
    private boolean tlsV1 = false;
    private Compression compression = Compression.NO_COMPRESSION;
    private Integer deflateLevel = null;
    private Integer sampleRate = null;
    private Optional<Integer> maxInFlight = Optional.empty();
    private String userAgent = null;
    private Integer msgTimeout = null;
    private SslContext sslContext = null;
    private EventLoopGroup eventLoopGroup = null;

    public NSQConfig() {
        try {
            clientId = InetAddress.getLocalHost().getHostName();
            hostname = InetAddress.getLocalHost().getCanonicalHostName();
            userAgent = "JavaNSQClient";
        } catch (UnknownHostException e) {
            LogManager.getLogger(this).error("Local host name could not resolved", e);
        }
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public boolean isFeatureNegotiation() {
        return featureNegotiation;
    }

    public void setFeatureNegotiation(final boolean featureNegotiation) {
        this.featureNegotiation = featureNegotiation;
    }

    public Integer getHeartbeatInterval() {
        return heartbeatInterval;
    }

    public void setHeartbeatInterval(final Integer heartbeatInterval) {
        this.heartbeatInterval = heartbeatInterval;
    }

    public Integer getOutputBufferSize() {
        return outputBufferSize;
    }

    public NSQConfig setMaxInFlight(final int maxInFlight) {
        this.maxInFlight = Optional.of(maxInFlight);
        return this;
    }

    public Optional<Integer> getMaxInFlight() {
        return maxInFlight;
    }

    public void setOutputBufferSize(final Integer outputBufferSize) {
        this.outputBufferSize = outputBufferSize;
    }

    public Integer getOutputBufferTimeout() {
        return outputBufferTimeout;
    }

    public void setOutputBufferTimeout(final Integer outputBufferTimeout) {
        this.outputBufferTimeout = outputBufferTimeout;
    }

    public boolean isTlsV1() {
        return tlsV1;
    }

    public Compression getCompression() {
        return compression;
    }

    public void setCompression(final Compression compression) {
        this.compression = compression;
    }

    public Integer getDeflateLevel() {
        return deflateLevel;
    }

    public void setDeflateLevel(final Integer deflateLevel) {
        this.deflateLevel = deflateLevel;
    }

    public Integer getSampleRate() {
        return sampleRate;
    }

    public void setSampleRate(final Integer sampleRate) {
        this.sampleRate = sampleRate;
    }

    public String getUserAgent() {
        return userAgent;
    }

    public void setUserAgent(final String userAgent) {
        this.userAgent = userAgent;
    }

    public Integer getMsgTimeout() {
        return msgTimeout;
    }

    public void setMsgTimeout(final Integer msgTimeout) {
        this.msgTimeout = msgTimeout;
    }

    public SslContext getSslContext() {
        return sslContext;
    }

    public void setSslContext(SslContext sslContext) {
        Preconditions.checkNotNull(sslContext);
        tlsV1 = true;
        this.sslContext = sslContext;
    }

    public EventLoopGroup getEventLoopGroup() {
        return eventLoopGroup;
    }

    public void setEventLoopGroup(final EventLoopGroup eventLoopGroup) {
        this.eventLoopGroup = eventLoopGroup;
    }

    @Override
    public String toString() {
        StringBuffer buffer = new StringBuffer();
        buffer.append("{\"client_id\":\"" + clientId + "\", ");
        buffer.append("\"hostname\":\"" + hostname + "\", ");
        buffer.append("\"feature_negotiation\": true, ");
        if (getHeartbeatInterval() != null) {
            buffer.append("\"heartbeat_interval\":" + getHeartbeatInterval().toString() + ", ");
        }
        if (getOutputBufferSize() != null) {
            buffer.append("\"output_buffer_size\":" + getOutputBufferSize().toString() + ", ");
        }
        if (getOutputBufferTimeout() != null) {
            buffer.append("\"output_buffer_timeout\":" + getOutputBufferTimeout().toString() + ", ");
        }
        if (isTlsV1()) {
            buffer.append("\"tls_v1\":" + isTlsV1() + ", ");
        }
        if (getCompression() == Compression.SNAPPY) {
            buffer.append("\"snappy\": true, ");
        }
        if (getCompression() == Compression.DEFLATE) {
            buffer.append("\"deflate\": true, ");
        }
        if (getDeflateLevel() != null) {
            buffer.append("\"deflate_level\":" + getDeflateLevel().toString() + ", ");
        }
        if (getSampleRate() != null) {
            buffer.append("\"sample_rate\":" + getSampleRate().toString() + ", ");
        }
        if (getMsgTimeout() != null) {
            buffer.append("\"msg_timeout\":" + getMsgTimeout().toString() + ", ");
        }
        buffer.append("\"user_agent\": \"" + userAgent + "\"}");

        return buffer.toString();
    }
}
