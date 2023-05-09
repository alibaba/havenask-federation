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

package org.havenask.transport;

import org.havenask.LegacyESVersion;
import org.havenask.Version;
import org.havenask.common.collect.Tuple;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.util.concurrent.ThreadContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class Header {

    private static final String RESPONSE_NAME = "NO_ACTION_NAME_FOR_RESPONSES";

    private final int networkMessageSize;
    private final Version version;
    private final long requestId;
    private final byte status;
    // These are directly set by tests
    String actionName;
    Tuple<Map<String, String>, Map<String, Set<String>>> headers;
    Set<String> features;

    Header(int networkMessageSize, long requestId, byte status, Version version) {
        this.networkMessageSize = networkMessageSize;
        this.version = version;
        this.requestId = requestId;
        this.status = status;
    }

    public int getNetworkMessageSize() {
        return networkMessageSize;
    }

    Version getVersion() {
        return version;
    }

    long getRequestId() {
        return requestId;
    }

    byte getStatus() {
        return status;
    }

    boolean isRequest() {
        return TransportStatus.isRequest(status);
    }

    boolean isResponse() {
        return TransportStatus.isRequest(status) == false;
    }

    boolean isError() {
        return TransportStatus.isError(status);
    }

    boolean isHandshake() {
        return TransportStatus.isHandshake(status);
    }

    boolean isCompressed() {
        return TransportStatus.isCompress(status);
    }

    public String getActionName() {
        return actionName;
    }

    boolean needsToReadVariableHeader() {
        return headers == null;
    }

    public Set<String> getFeatures() {
        return features;
    }

    Tuple<Map<String, String>, Map<String, Set<String>>> getHeaders() {
        return headers;
    }

    void finishParsingHeader(StreamInput input) throws IOException {
        this.headers = ThreadContext.readHeadersFromStream(input);

        if (isRequest()) {
            if (version.onOrAfter(LegacyESVersion.V_6_3_0)) {
                final String[] featuresFound = input.readStringArray();
                if (featuresFound.length == 0) {
                    features = Collections.emptySet();
                } else {
                    features = Collections.unmodifiableSet(new TreeSet<>(Arrays.asList(featuresFound)));
                }
            } else {
                features = Collections.emptySet();
            }
            this.actionName = input.readString();
        } else {
            this.actionName = RESPONSE_NAME;
        }
    }

    @Override
    public String toString() {
        return "Header{" + networkMessageSize + "}{" + version + "}{" + requestId + "}{" + isRequest() + "}{" + isError() + "}{"
                + isHandshake() + "}{" + isCompressed() + "}{" + actionName + "}";
    }
}
