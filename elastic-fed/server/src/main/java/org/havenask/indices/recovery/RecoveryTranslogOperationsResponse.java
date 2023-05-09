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

package org.havenask.indices.recovery;

import org.havenask.LegacyESVersion;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;
import org.havenask.index.seqno.SequenceNumbers;
import org.havenask.transport.TransportResponse;

import java.io.IOException;

final class RecoveryTranslogOperationsResponse extends TransportResponse {
    final long localCheckpoint;

    RecoveryTranslogOperationsResponse(final long localCheckpoint) {
        this.localCheckpoint = localCheckpoint;
    }

    RecoveryTranslogOperationsResponse(final StreamInput in) throws IOException {
        super(in);
        // before 6.0.0 we received an empty response so we have to maintain that
        if (in.getVersion().onOrAfter(LegacyESVersion.V_6_0_0_alpha1)) {
            localCheckpoint = in.readZLong();
        }
        else {
            localCheckpoint = SequenceNumbers.UNASSIGNED_SEQ_NO;
        }
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        // before 6.0.0 we responded with an empty response so we have to maintain that
        if (out.getVersion().onOrAfter(LegacyESVersion.V_6_0_0_alpha1)) {
            out.writeZLong(localCheckpoint);
        }
    }
}
