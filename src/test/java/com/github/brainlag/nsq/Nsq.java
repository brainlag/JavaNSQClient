package com.github.brainlag.nsq;

public class Nsq {

    public static String getNsqdHost() {
        String hostName = System.getenv("NSQD_HOST");
        if (hostName == null) {
            hostName = "localhost";
        }
        return hostName;
    }

    public static String getNsqLookupdHost() {
        String hostName = System.getenv("NSQLOOKUPD_HOST");
        if (hostName == null) {
            hostName = "localhost";
        }
        return hostName;
    }

}
