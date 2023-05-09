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

package org.havenask.plugin.noop.action.bulk;

import org.havenask.action.ActionListener;
import org.havenask.action.DocWriteRequest;
import org.havenask.action.DocWriteResponse;
import org.havenask.action.bulk.BulkItemResponse;
import org.havenask.action.bulk.BulkRequest;
import org.havenask.action.bulk.BulkResponse;
import org.havenask.action.support.ActionFilters;
import org.havenask.action.support.HandledTransportAction;
import org.havenask.action.update.UpdateResponse;
import org.havenask.common.inject.Inject;
import org.havenask.index.shard.ShardId;
import org.havenask.tasks.Task;
import org.havenask.transport.TransportService;

public class TransportNoopBulkAction extends HandledTransportAction<BulkRequest, BulkResponse> {
    private static final BulkItemResponse ITEM_RESPONSE = new BulkItemResponse(1, DocWriteRequest.OpType.UPDATE,
        new UpdateResponse(new ShardId("mock", "", 1), "mock_type", "1", 0L, 1L, 1L, DocWriteResponse.Result.CREATED));

    @Inject
    public TransportNoopBulkAction(TransportService transportService, ActionFilters actionFilters) {
        super(NoopBulkAction.NAME, transportService, actionFilters, BulkRequest::new);
    }

    @Override
    protected void doExecute(Task task, BulkRequest request, ActionListener<BulkResponse> listener) {
        final int itemCount = request.requests().size();
        // simulate at least a realistic amount of data that gets serialized
        BulkItemResponse[] bulkItemResponses = new BulkItemResponse[itemCount];
        for (int idx = 0; idx < itemCount; idx++) {
            bulkItemResponses[idx] = ITEM_RESPONSE;
        }
        listener.onResponse(new BulkResponse(bulkItemResponses, 0));
    }
}
