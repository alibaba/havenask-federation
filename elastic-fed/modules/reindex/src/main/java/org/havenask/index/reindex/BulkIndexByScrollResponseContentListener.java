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

package org.havenask.index.reindex;

import org.havenask.action.bulk.BulkItemResponse.Failure;
import org.havenask.common.xcontent.ToXContent;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.index.reindex.ScrollableHitSource.SearchFailure;
import org.havenask.rest.BytesRestResponse;
import org.havenask.rest.RestChannel;
import org.havenask.rest.RestResponse;
import org.havenask.rest.RestStatus;
import org.havenask.rest.action.RestBuilderListener;

import java.util.Map;

/**
 * RestBuilderListener that returns higher than 200 status if there are any failures and allows to set XContent.Params.
 */
public class BulkIndexByScrollResponseContentListener extends RestBuilderListener<BulkByScrollResponse> {

    private final Map<String, String> params;

    public BulkIndexByScrollResponseContentListener(RestChannel channel, Map<String, String> params) {
        super(channel);
        this.params = params;
    }

    @Override
    public RestResponse buildResponse(BulkByScrollResponse response, XContentBuilder builder) throws Exception {
        builder.startObject();
        response.toXContent(builder, new ToXContent.DelegatingMapParams(params, channel.request()));
        builder.endObject();
        return new BytesRestResponse(getStatus(response), builder);
    }

    private RestStatus getStatus(BulkByScrollResponse response) {
        /*
         * Return the highest numbered rest status under the assumption that higher numbered statuses are "more error" and thus more
         * interesting to the user.
         */
        RestStatus status = RestStatus.OK;
        if (response.isTimedOut()) {
            status = RestStatus.REQUEST_TIMEOUT;
        }
        for (Failure failure : response.getBulkFailures()) {
            if (failure.getStatus().getStatus() > status.getStatus()) {
                status = failure.getStatus();
            }
        }
        for (SearchFailure failure: response.getSearchFailures()) {
            RestStatus failureStatus = failure.getStatus();
            if (failureStatus.getStatus() > status.getStatus()) {
                status = failureStatus;
            }
        }
        return status;
    }
}
