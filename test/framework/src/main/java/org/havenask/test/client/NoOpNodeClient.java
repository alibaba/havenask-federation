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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.havenask.test.client;

import org.havenask.HavenaskException;
import org.havenask.action.ActionListener;
import org.havenask.action.ActionRequest;
import org.havenask.action.ActionResponse;
import org.havenask.action.ActionType;
import org.havenask.action.support.TransportAction;
import org.havenask.client.Client;
import org.havenask.client.node.NodeClient;
import org.havenask.common.io.stream.NamedWriteableRegistry;
import org.havenask.common.settings.Settings;
import org.havenask.tasks.Task;
import org.havenask.tasks.TaskListener;
import org.havenask.threadpool.TestThreadPool;
import org.havenask.threadpool.ThreadPool;
import org.havenask.transport.RemoteClusterService;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Client that always response with {@code null} to every request. Override {@link #doExecute(ActionType, ActionRequest, ActionListener)},
 * {@link #executeLocally(ActionType, ActionRequest, ActionListener)}, or {@link #executeLocally(ActionType, ActionRequest, TaskListener)}
 * for testing.
 *
 * See also {@link NoOpClient} if you do not specifically need a {@link NodeClient}.
 */
public class NoOpNodeClient extends NodeClient {

    /**
     * Build with {@link ThreadPool}. This {@linkplain ThreadPool} is terminated on {@link #close()}.
     */
    public NoOpNodeClient(ThreadPool threadPool) {
        super(Settings.EMPTY, threadPool);
    }

    /**
     * Create a new {@link TestThreadPool} for this client. This {@linkplain TestThreadPool} is terminated on {@link #close()}.
     */
    public NoOpNodeClient(String testName) {
        super(Settings.EMPTY, new TestThreadPool(testName));
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse>
    void doExecute(ActionType<Response> action, Request request, ActionListener<Response> listener) {
        listener.onResponse(null);
    }

    @Override
    public void initialize(Map<ActionType, TransportAction> actions, Supplier<String> localNodeId,
                           RemoteClusterService remoteClusterService, NamedWriteableRegistry namedWriteableRegistry) {
        throw new UnsupportedOperationException("cannot initialize " + this.getClass().getSimpleName());
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse>
    Task executeLocally(ActionType<Response> action, Request request, ActionListener<Response> listener) {
        listener.onResponse(null);
        return null;
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse>
    Task executeLocally(ActionType<Response> action, Request request, TaskListener<Response> listener) {
        listener.onResponse(null, null);
        return null;
    }

    @Override
    public String getLocalNodeId() {
        return null;
    }

    @Override
    public Client getRemoteClusterClient(String clusterAlias) {
        return null;
    }

    @Override
    public void close() {
        try {
            ThreadPool.terminate(threadPool(), 10, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new HavenaskException(e.getMessage(), e);
        }
    }
}
