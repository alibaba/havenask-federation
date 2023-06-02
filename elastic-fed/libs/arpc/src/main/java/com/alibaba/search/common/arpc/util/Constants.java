package com.alibaba.search.common.arpc.util;

public interface Constants {
    String IO_COMPONENT = "io_component";

    // packet related
    int ANET_PACKET_FLAG = 0x416e4574; // AnEt
    int MAX_PACKET_SIZE = 0x10000000; // 256M

    // databuffer related
    int DEFAULT_DATABUFFER_SIZE = 64 * 1024; // 64k
    int MAX_DATABUFFER_SIZE = 2 * 1024 * 1024 + 64 * 1024; // 2M + 64k

    // timeout related
    int DEFAULT_PACKET_TIMEOUT = 5 * 1000; // 5s
    int CHECK_TIMEOUT_INTEVAL = 50; // 50ms

    // connection related
    int DEFAULT_OUTPUT_QUEUE_LIMIT = 50;
}
