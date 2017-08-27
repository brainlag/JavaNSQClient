package com.github.brainlag.nsq.netty;

import com.github.brainlag.nsq.frames.NSQFrame;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;

import java.util.List;

public class NSQDecoder extends MessageToMessageDecoder<ByteBuf> {

    private int size;
    private NSQFrame frame;

    public NSQDecoder() {
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        size = in.readInt();
        int id = in.readInt();
        frame = NSQFrame.instance(id);
        if (frame == null) {
            //uhh, bad response from server..  what should we do?
            throw new Exception("Bad frame id from server (" + id + ").  disconnect!");
        }
        frame.setSize(size);
        ByteBuf bytes = in.readBytes(frame.getSize() - 4); //subtract 4 because the frame id is included
        byte[] array = new byte[bytes.readableBytes()];
        bytes.readBytes(array);
        frame.setData(array);

        out.add(frame);
        bytes.release();
    }

}
