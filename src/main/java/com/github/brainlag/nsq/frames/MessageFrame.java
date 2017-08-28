package com.github.brainlag.nsq.frames;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class MessageFrame extends NSQFrame {
    private long timestamp;
    private int attempts;
    private byte[] messageId = new byte[16];
    private byte[] messageBody;

    @Override
    public void setData(byte[] bytes) {
        //parse the bytes
        super.setData(bytes);

        ByteBuf buf = Unpooled.wrappedBuffer(bytes);
        timestamp = buf.readLong();
        attempts = buf.readShort();
        buf.readBytes(messageId);

        byte[] array = new byte[buf.readableBytes()];
        buf.readBytes(array);
        messageBody = array;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public int getAttempts() {
        return attempts;
    }

    public byte[] getMessageId() {
        return messageId;
    }

    public byte[] getMessageBody() {
        return messageBody;
    }

}
