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

package org.havenask.action.admin.indices.flush;

import org.havenask.action.ActionListener;
import org.havenask.action.support.ActionFilters;
import org.havenask.action.support.HandledTransportAction;
import org.havenask.common.inject.Inject;
import org.havenask.indices.flush.SyncedFlushService;
import org.havenask.tasks.Task;
import org.havenask.transport.TransportService;

/**
 * Synced flush ActionType.
 */
public class TransportSyncedFlushAction extends HandledTransportAction<SyncedFlushRequest, SyncedFlushResponse> {

    SyncedFlushService syncedFlushService;

    @Inject
    public TransportSyncedFlushAction(TransportService transportService, ActionFilters actionFilters,
                                      SyncedFlushService syncedFlushService) {
        super(SyncedFlushAction.NAME, transportService, actionFilters, SyncedFlushRequest::new);
        this.syncedFlushService = syncedFlushService;
    }

    @Override
    protected void doExecute(Task task, SyncedFlushRequest request, ActionListener<SyncedFlushResponse> listener) {
        syncedFlushService.attemptSyncedFlush(request.indices(), request.indicesOptions(), listener);
    }
}
