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
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;
import org.havenask.common.io.stream.Writeable;
import org.havenask.common.unit.ByteSizeValue;
import org.havenask.common.xcontent.ToXContentFragment;
import org.havenask.common.xcontent.XContentBuilder;

import java.io.IOException;

public class TransportStats implements Writeable, ToXContentFragment {

    private final long serverOpen;
    private final long totalOutboundConnections;
    private final long rxCount;
    private final long rxSize;
    private final long txCount;
    private final long txSize;

    public TransportStats(long serverOpen, long totalOutboundConnections, long rxCount, long rxSize, long txCount, long txSize) {
        this.serverOpen = serverOpen;
        this.totalOutboundConnections = totalOutboundConnections;
        this.rxCount = rxCount;
        this.rxSize = rxSize;
        this.txCount = txCount;
        this.txSize = txSize;
    }

    public TransportStats(StreamInput in) throws IOException {
        serverOpen = in.readVLong();
        if (in.getVersion().onOrAfter(LegacyESVersion.V_7_10_0)) {
            totalOutboundConnections = in.readVLong();
        } else {
            totalOutboundConnections = 0L;
        }
        rxCount = in.readVLong();
        rxSize = in.readVLong();
        txCount = in.readVLong();
        txSize = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(serverOpen);
        if (out.getVersion().onOrAfter(LegacyESVersion.V_7_10_0)) {
            out.writeVLong(totalOutboundConnections);
        }
        out.writeVLong(rxCount);
        out.writeVLong(rxSize);
        out.writeVLong(txCount);
        out.writeVLong(txSize);
    }

    public long serverOpen() {
        return this.serverOpen;
    }

    public long getServerOpen() {
        return serverOpen();
    }

    public long rxCount() {
        return rxCount;
    }

    public long getRxCount() {
        return rxCount();
    }

    public ByteSizeValue rxSize() {
        return new ByteSizeValue(rxSize);
    }

    public ByteSizeValue getRxSize() {
        return rxSize();
    }

    public long txCount() {
        return txCount;
    }

    public long getTxCount() {
        return txCount();
    }

    public ByteSizeValue txSize() {
        return new ByteSizeValue(txSize);
    }

    public ByteSizeValue getTxSize() {
        return txSize();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.TRANSPORT);
        builder.field(Fields.SERVER_OPEN, serverOpen);
        builder.field(Fields.TOTAL_OUTBOUND_CONNECTIONS, totalOutboundConnections);
        builder.field(Fields.RX_COUNT, rxCount);
        builder.humanReadableField(Fields.RX_SIZE_IN_BYTES, Fields.RX_SIZE, new ByteSizeValue(rxSize));
        builder.field(Fields.TX_COUNT, txCount);
        builder.humanReadableField(Fields.TX_SIZE_IN_BYTES, Fields.TX_SIZE, new ByteSizeValue(txSize));
        builder.endObject();
        return builder;
    }

    static final class Fields {
        static final String TRANSPORT = "transport";
        static final String SERVER_OPEN = "server_open";
        static final String TOTAL_OUTBOUND_CONNECTIONS = "total_outbound_connections";
        static final String RX_COUNT = "rx_count";
        static final String RX_SIZE = "rx_size";
        static final String RX_SIZE_IN_BYTES = "rx_size_in_bytes";
        static final String TX_COUNT = "tx_count";
        static final String TX_SIZE = "tx_size";
        static final String TX_SIZE_IN_BYTES = "tx_size_in_bytes";
    }
}
