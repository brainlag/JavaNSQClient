package com.github.brainlag.nsq.frames;

public abstract class NSQFrame {

    private int size;
    private byte[] data;


    public static NSQFrame instance(int type) {
        switch (type) {
            case 0:
                return new ResponseFrame();
            case 1:
                return new ErrorFrame();
            case 2:
                return new MessageFrame();
        }
        return null;
    }


    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }
}
