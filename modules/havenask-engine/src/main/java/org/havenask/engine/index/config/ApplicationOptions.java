package org.havenask.engine.index.config;

public class ApplicationOptions {
    @Deprecated
    public String qrsConfigPath;
    public String zoneName;
    public Integer partCount;
    public Integer partId;
    // arpc的端口， tcp:xxxx:2020
    public String binaryPath;
    public String searchSpec;
    // support domain socket
    public String domainPath;

    // alog
    public String loggerConfig;

    // for build qps limit;
    public Boolean enableBuildLimit;
    // global_limit_qps
    public Integer globalLimitQps;
    // pid_limit_qps
    public Integer pidLimitQps;
    // build_retry_count
    public Integer buildRetryCount;

    public ArpcOptions arpc;

    public static class ArpcOptions {
        public Integer queueSize;
        public Long connectTimeout;
        public Long searchTimeout;
        public Long buildTimeout;
    }
}
