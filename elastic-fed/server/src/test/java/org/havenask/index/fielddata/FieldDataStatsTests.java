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

package org.havenask.index.fielddata;

import org.havenask.common.FieldMemoryStats;
import org.havenask.common.FieldMemoryStatsTests;
import org.havenask.common.io.stream.BytesStreamOutput;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.test.HavenaskTestCase;

import java.io.IOException;

public class FieldDataStatsTests extends HavenaskTestCase {

    public void testSerialize() throws IOException {
        FieldMemoryStats map = randomBoolean() ? null : FieldMemoryStatsTests.randomFieldMemoryStats();
        FieldDataStats stats = new FieldDataStats(randomNonNegativeLong(), randomNonNegativeLong(), map);
        BytesStreamOutput out = new BytesStreamOutput();
        stats.writeTo(out);
        StreamInput input = out.bytes().streamInput();
        FieldDataStats read = new FieldDataStats(input);
        assertEquals(-1, input.read());
        assertEquals(stats.getEvictions(), read.getEvictions());
        assertEquals(stats.getMemorySize(), read.getMemorySize());
        assertEquals(stats.getFields(), read.getFields());
    }
}
