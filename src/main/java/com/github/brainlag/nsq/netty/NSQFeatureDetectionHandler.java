package com.github.brainlag.nsq.netty;

import javax.net.ssl.SSLEngine;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.compression.SnappyFramedDecoder;
import io.netty.handler.codec.compression.SnappyFramedEncoder;
import io.netty.handler.codec.compression.ZlibCodecFactory;
import io.netty.handler.codec.compression.ZlibWrapper;
import io.netty.handler.ssl.SslHandler;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.brainlag.nsq.NSQConnection;
import com.github.brainlag.nsq.frames.NSQFrame;
import com.github.brainlag.nsq.frames.ResponseFrame;
import com.google.common.base.Charsets;

@Slf4j
public class NSQFeatureDetectionHandler extends SimpleChannelInboundHandler<NSQFrame> {
    private boolean ssl;
    private boolean compression;
    private boolean snappy;
    private boolean deflate;
    private boolean finished;

    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final NSQFrame msg) throws Exception {
        if (log.isTraceEnabled()) {
            log.trace("IdentifyResponse: {}", new String(msg.getData(), Charsets.US_ASCII));
        }

        boolean reinstallDefaultDecoder = true;
        if (msg instanceof ResponseFrame) {
            ResponseFrame response = (ResponseFrame) msg;
            ChannelPipeline pipeline = ctx.channel().pipeline();
            val con = ctx.channel().attr(NSQConnection.STATE).get();
            parseIdentify(response.getMessage());

            if (response.getMessage().equals("OK")) {
                if (finished) {
                    return;
                }
                //round 2
                if (snappy) {
                    reinstallDefaultDecoder = installSnappyDecoder(pipeline);
                }
                if (deflate) {
                    reinstallDefaultDecoder = installDeflateDecoder(pipeline);
                }
                eject(reinstallDefaultDecoder, pipeline);
                if (ssl) {
                    ((SslHandler) pipeline.get("SSLHandler")).setSingleDecode(false);
                }
                return;
            }
            if (ssl) {
                log.info("Adding SSL to pipline");
                SSLEngine sslEngine = con.getConfig().getSslContext().newEngine(ctx.channel().alloc());
                sslEngine.setUseClientMode(true);
                SslHandler sslHandler = new SslHandler(sslEngine, false);
                sslHandler.setSingleDecode(true);
                pipeline.addBefore("LengthFieldBasedFrameDecoder", "SSLHandler", sslHandler);
                if (snappy) {
                    pipeline.addBefore("NSQEncoder", "SnappyEncoder", new SnappyFramedEncoder());
                }
                if (deflate) {
                    pipeline.addBefore("NSQEncoder", "DeflateEncoder",
                            ZlibCodecFactory.newZlibEncoder(ZlibWrapper.NONE,con.getConfig().getDeflateLevel()));
                }
            }
            if (!ssl && snappy) {
                pipeline.addBefore("NSQEncoder", "SnappyEncoder", new SnappyFramedEncoder());
                reinstallDefaultDecoder = installSnappyDecoder(pipeline);
            }
            if (!ssl && deflate) {
                pipeline.addBefore("NSQEncoder", "DeflateEncoder",
                        ZlibCodecFactory.newZlibEncoder(ZlibWrapper.NONE, con.getConfig().getDeflateLevel()));
                reinstallDefaultDecoder = installDeflateDecoder(pipeline);
            }
            if (response.getMessage().contains("version") && finished) {
                eject(reinstallDefaultDecoder, pipeline);
            }
        }
        ctx.fireChannelRead(msg);
    }

    private void eject(final boolean reinstallDefaultDecoder, final ChannelPipeline pipeline) {
        // ok we read only the the first message to set up the pipline, ejecting now!
        pipeline.remove(this);
        if (reinstallDefaultDecoder) {
            log.info("reinstall LengthFieldBasedFrameDecoder");
            pipeline.replace("LengthFieldBasedFrameDecoder", "LengthFieldBasedFrameDecoder",
                    new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, Integer.BYTES));
        }
    }

    private boolean installDeflateDecoder(final ChannelPipeline pipeline) {
        finished = true;
        log.info("Adding deflate to pipeline");
        pipeline.replace("LengthFieldBasedFrameDecoder", "DeflateDecoder", ZlibCodecFactory.newZlibDecoder(ZlibWrapper.NONE));
        return false;
    }

    private boolean installSnappyDecoder(final ChannelPipeline pipeline) {
        finished = true;
        log.info("Adding snappy to pipeline");
        pipeline.replace("LengthFieldBasedFrameDecoder", "SnappyDecoder", new SnappyFramedDecoder());
        return false;
    }

    private void parseIdentify(final String message) {
        if (message.equals("OK")) {
            return;
        }
        if (message.contains("\"tls_v1\":true")) {
            ssl = true;
        }
        if (message.contains("\"snappy\":true")) {
            snappy = true;
            compression = true;
        }
        if (message.contains("\"deflate\":true")) {
            deflate = true;
            compression = true;
        }
        if (!ssl && !compression) {
            finished = true;
        }
    }
}
