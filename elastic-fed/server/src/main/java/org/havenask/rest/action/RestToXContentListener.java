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

package org.havenask.rest.action;

import org.havenask.common.xcontent.ToXContent;
import org.havenask.common.xcontent.ToXContentObject;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.rest.BytesRestResponse;
import org.havenask.rest.RestChannel;
import org.havenask.rest.RestResponse;
import org.havenask.rest.RestStatus;

/**
 * A REST based action listener that assumes the response is of type {@link ToXContent} and automatically
 * builds an XContent based response (wrapping the toXContent in startObject/endObject).
 */
public class RestToXContentListener<Response extends ToXContentObject> extends RestResponseListener<Response> {

    public RestToXContentListener(RestChannel channel) {
        super(channel);
    }

    @Override
    public final RestResponse buildResponse(Response response) throws Exception {
        return buildResponse(response, channel.newBuilder());
    }

    public RestResponse buildResponse(Response response, XContentBuilder builder) throws Exception {
        assert response.isFragment() == false; //would be nice if we could make default methods final
        response.toXContent(builder, channel.request());
        return new BytesRestResponse(getStatus(response), builder);
    }

    protected RestStatus getStatus(Response response) {
        return RestStatus.OK;
    }
}
