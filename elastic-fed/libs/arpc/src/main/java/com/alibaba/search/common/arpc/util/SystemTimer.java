package com.alibaba.search.common.arpc.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SystemTimer {
    private final static Logger logger = LoggerFactory
            .getLogger(SystemTimer.class);
    
    public long getCurrentTime() {
        return System.currentTimeMillis();
    }
    
    public long getExpireTime(long timeout) {
        return getCurrentTime() + timeout;
    }
}
