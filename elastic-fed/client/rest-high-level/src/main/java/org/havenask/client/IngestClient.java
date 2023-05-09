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

package org.havenask.client;

import org.havenask.client.Cancellable;
import org.havenask.client.RequestOptions;
import org.havenask.action.ActionListener;
import org.havenask.action.ingest.DeletePipelineRequest;
import org.havenask.action.ingest.GetPipelineRequest;
import org.havenask.action.ingest.GetPipelineResponse;
import org.havenask.action.ingest.PutPipelineRequest;
import org.havenask.action.ingest.SimulatePipelineRequest;
import org.havenask.action.ingest.SimulatePipelineResponse;
import org.havenask.action.support.master.AcknowledgedResponse;

import java.io.IOException;
import java.util.Collections;

import static java.util.Collections.emptySet;

/**
 * A wrapper for the {@link RestHighLevelClient} that provides methods for accessing the Ingest API.
 *
 */
public final class IngestClient {

    private final RestHighLevelClient restHighLevelClient;

    IngestClient(RestHighLevelClient restHighLevelClient) {
        this.restHighLevelClient = restHighLevelClient;
    }

    /**
     * Add a pipeline or update an existing pipeline.
     *
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public AcknowledgedResponse putPipeline(PutPipelineRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity( request, IngestRequestConverters::putPipeline, options,
            AcknowledgedResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously add a pipeline or update an existing pipeline.
     *
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable putPipelineAsync(PutPipelineRequest request, RequestOptions options, ActionListener<AcknowledgedResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity( request, IngestRequestConverters::putPipeline, options,
            AcknowledgedResponse::fromXContent, listener, emptySet());
    }

    /**
     * Get an existing pipeline.
     *
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public GetPipelineResponse getPipeline(GetPipelineRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity( request, IngestRequestConverters::getPipeline, options,
            GetPipelineResponse::fromXContent, Collections.singleton(404));
    }

    /**
     * Asynchronously get an existing pipeline.
     *
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable getPipelineAsync(GetPipelineRequest request, RequestOptions options, ActionListener<GetPipelineResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity( request, IngestRequestConverters::getPipeline, options,
            GetPipelineResponse::fromXContent, listener, Collections.singleton(404));
    }

    /**
     * Delete an existing pipeline.
     *
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public AcknowledgedResponse deletePipeline(DeletePipelineRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity( request, IngestRequestConverters::deletePipeline, options,
            AcknowledgedResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously delete an existing pipeline.
     *
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable deletePipelineAsync(DeletePipelineRequest request, RequestOptions options,
                                           ActionListener<AcknowledgedResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity( request,
            IngestRequestConverters::deletePipeline, options,
            AcknowledgedResponse::fromXContent, listener, emptySet());
    }

    /**
     * Simulate a pipeline on a set of documents provided in the request
     * <p>
     *
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public SimulatePipelineResponse simulate(SimulatePipelineRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity( request, IngestRequestConverters::simulatePipeline, options,
            SimulatePipelineResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously simulate a pipeline on a set of documents provided in the request
     * <p>
     *
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable simulateAsync(SimulatePipelineRequest request,
                                     RequestOptions options,
                                     ActionListener<SimulatePipelineResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity( request, IngestRequestConverters::simulatePipeline, options,
            SimulatePipelineResponse::fromXContent, listener, emptySet());
    }
}
