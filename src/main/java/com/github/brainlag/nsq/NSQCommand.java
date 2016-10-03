package com.github.brainlag.nsq;

import java.util.ArrayList;
import java.util.List;

public class NSQCommand {

    String line;
    List<byte[]> data = new ArrayList<>();

    public static NSQCommand instance(String line) {
        NSQCommand n = new NSQCommand();
        n.setLine(line);
        return n;
    }

    public static NSQCommand instance(String line, byte[] bytes) {
        NSQCommand n = instance(line);
        n.addBytes(bytes);
        return n;
    }

    public void addBytes(byte[] bytes) {
        data.add(bytes);
    }

    public String getLine() {
        return line;
    }

    public void setLine(String line) {
        if (!line.endsWith("\n")) {
            line = line + "\n";
        }

        this.line = line;
    }

    public List<byte[]> getData() {
        return data;
    }

    public void setData(List<byte[]> data) {
        this.data = data;
    }

    public String toString() {
        return this.getLine().trim();
    }
}
