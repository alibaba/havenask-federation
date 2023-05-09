/*
*Copyright (c) 2021, Alibaba Group;
*Licensed under the Apache License, Version 2.0 (the "License");
*you may not use this file except in compliance with the License.
*You may obtain a copy of the License at

*   http://www.apache.org/licenses/LICENSE-2.0

*Unless required by applicable law or agreed to in writing, software
*distributed under the License is distributed on an "AS IS" BASIS,
*WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*See the License for the specific language governing permissions and
*limitations under the License.
*/

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright Havenask Contributors. See
 * GitHub history for details.
 */

package org.havenask.transport.nio;

import org.havenask.Version;
import org.havenask.common.io.stream.NamedWriteableRegistry;
import org.havenask.common.network.NetworkService;
import org.havenask.common.settings.Settings;
import org.havenask.common.util.PageCacheRecycler;
import org.havenask.indices.breaker.CircuitBreakerService;
import org.havenask.plugins.NetworkPlugin;
import org.havenask.plugins.Plugin;
import org.havenask.threadpool.ThreadPool;
import org.havenask.transport.Transport;

import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

public class MockNioTransportPlugin extends Plugin implements NetworkPlugin {

    public static final String MOCK_NIO_TRANSPORT_NAME = "mock-nio";

    @Override
    public Map<String, Supplier<Transport>> getTransports(Settings settings, ThreadPool threadPool, PageCacheRecycler pageCacheRecycler,
                                                          CircuitBreakerService circuitBreakerService,
                                                          NamedWriteableRegistry namedWriteableRegistry, NetworkService networkService) {
        return Collections.singletonMap(MOCK_NIO_TRANSPORT_NAME,
            () -> new MockNioTransport(settings, Version.CURRENT, threadPool, networkService, pageCacheRecycler,
                namedWriteableRegistry, circuitBreakerService));
    }
}
