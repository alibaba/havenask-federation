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

package org.havenask.action.search;

import org.havenask.action.ActionListener;
import org.havenask.action.support.ActionFilters;
import org.havenask.action.support.HandledTransportAction;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.inject.Inject;
import org.havenask.common.io.stream.Writeable;
import org.havenask.tasks.Task;
import org.havenask.transport.TransportService;

public class TransportSearchScrollAction extends HandledTransportAction<SearchScrollRequest, SearchResponse> {

    private final ClusterService clusterService;
    private final SearchTransportService searchTransportService;
    private final SearchPhaseController searchPhaseController;

    @Inject
    public TransportSearchScrollAction(TransportService transportService, ClusterService clusterService, ActionFilters actionFilters,
                                       SearchTransportService searchTransportService, SearchPhaseController searchPhaseController) {
        super(SearchScrollAction.NAME, transportService, actionFilters,
            (Writeable.Reader<SearchScrollRequest>) SearchScrollRequest::new);
        this.clusterService = clusterService;
        this.searchTransportService = searchTransportService;
        this.searchPhaseController = searchPhaseController;
    }

    @Override
    protected void doExecute(Task task, SearchScrollRequest request, ActionListener<SearchResponse> listener) {
        try {
            ParsedScrollId scrollId = TransportSearchHelper.parseScrollId(request.scrollId());
            Runnable action;
            switch (scrollId.getType()) {
                case ParsedScrollId.QUERY_THEN_FETCH_TYPE:
                    action = new SearchScrollQueryThenFetchAsyncAction(logger, clusterService, searchTransportService,
                        searchPhaseController, request, (SearchTask)task, scrollId, listener);
                    break;
                case ParsedScrollId.QUERY_AND_FETCH_TYPE: // TODO can we get rid of this?
                    action = new SearchScrollQueryAndFetchAsyncAction(logger, clusterService, searchTransportService,
                        searchPhaseController, request, (SearchTask)task, scrollId, listener);
                    break;
                default:
                    throw new IllegalArgumentException("Scroll id type [" + scrollId.getType() + "] unrecognized");
            }
            action.run();
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
