package com.movableink.nsq.netty;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

public class NSQClientInitializer extends ChannelInitializer<SocketChannel> {
    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();
        LengthFieldBasedFrameDecoder dec = new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, Integer.BYTES);
        dec.setSingleDecode(true);

        pipeline.addLast("LengthFieldBasedFrameDecoder", dec);
        pipeline.addLast("NSQDecoder", new NSQDecoder()); // in
        pipeline.addLast("NSQEncoder", new NSQEncoder()); // out
        pipeline.addLast("FeatureDetectionHandler", new NSQFeatureDetectionHandler());
        pipeline.addLast("NSQHandler", new NSQHandler()); // in
    }
}
