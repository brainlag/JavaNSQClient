package com.github.brainlag.nsq.netty;

import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;

import com.github.brainlag.nsq.frames.NSQFrame;

public class NSQDecoder extends MessageToMessageDecoder<ByteBuf> {

    private int size;
    private NSQFrame frame;

    public NSQDecoder() {
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        this.size = in.readInt();
        int id = in.readInt();
        this.frame = NSQFrame.instance(id);
        if (this.frame == null) {
            //uhh, bad response from server..  what should we do?
            throw new IllegalStateException("Bad frame id from server (" + id + ").  disconnect!");
        }
        this.frame.setSize(this.size);
        ByteBuf bytes = in.readBytes(this.frame.getSize() - 4); //subtract 4 because the frame id is included
        this.frame.setData(bytes.array());
        out.add(this.frame);
    }

}
