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

package org.havenask.http;

import org.havenask.common.component.LifecycleComponent;
import org.havenask.common.transport.BoundTransportAddress;
import org.havenask.common.util.concurrent.ThreadContext;
import org.havenask.node.ReportingService;
import org.havenask.rest.RestChannel;
import org.havenask.rest.RestRequest;

public interface HttpServerTransport extends LifecycleComponent, ReportingService<HttpInfo> {

    String HTTP_SERVER_WORKER_THREAD_NAME_PREFIX = "http_server_worker";

    BoundTransportAddress boundAddress();

    @Override
    HttpInfo info();

    HttpStats stats();

    /**
     * Dispatches HTTP requests.
     */
    interface Dispatcher {

        /**
         * Dispatches the {@link RestRequest} to the relevant request handler or responds to the given rest channel directly if
         * the request can't be handled by any request handler.
         *
         * @param request       the request to dispatch
         * @param channel       the response channel of this request
         * @param threadContext the thread context
         */
        void dispatchRequest(RestRequest request, RestChannel channel, ThreadContext threadContext);

        /**
         * Dispatches a bad request. For example, if a request is malformed it will be dispatched via this method with the cause of the bad
         * request.
         *
         * @param channel       the response channel of this request
         * @param threadContext the thread context
         * @param cause         the cause of the bad request
         */
        void dispatchBadRequest(RestChannel channel, ThreadContext threadContext, Throwable cause);

    }
}
