package com.github.brainlag.nsq.netty;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.val;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.brainlag.nsq.frames.NSQFrame;

import static com.github.brainlag.nsq.AbstractNSQConnection.STATE;


public class NSQHandler extends SimpleChannelInboundHandler<NSQFrame> {
    private static final Logger LOG = LoggerFactory.getLogger(NSQHandler.class);

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        val con = ctx.channel().attr(STATE).get();
        if (con != null) {
            LOG.info("Channel disconnected! {}", con);
        } else {
            LOG.error("No connection set for {}", ctx.channel());
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        super.exceptionCaught(ctx, cause);
        LOG.error("NSQHandler exception caught", cause);

        ctx.channel().close();
        val con = ctx.channel().attr(STATE).get();
        if (con != null) {
            con.close();
        } else {
            LOG.warn("No connection set for {}", ctx.channel());
        }
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, NSQFrame msg) throws Exception {
        val con = ctx.channel().attr(STATE).get();
        if (con != null) {
            ctx.channel().eventLoop().execute(() -> con.incoming(msg));
        } else {
            LOG.warn("No connection set for ", ctx.channel());
        }
    }
}
