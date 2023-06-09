/*
 * Copyright (c) 2021, Alibaba Group;
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

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
