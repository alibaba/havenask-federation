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
