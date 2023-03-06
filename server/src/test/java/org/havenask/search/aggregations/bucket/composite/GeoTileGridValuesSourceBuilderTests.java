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

package org.havenask.search.aggregations.bucket.composite;

import org.havenask.LegacyESVersion;
import org.havenask.Version;
import org.havenask.common.geo.GeoBoundingBox;
import org.havenask.common.geo.GeoBoundingBoxTests;
import org.havenask.common.geo.GeoPoint;
import org.havenask.common.io.stream.BytesStreamOutput;
import org.havenask.common.io.stream.NamedWriteableAwareStreamInput;
import org.havenask.common.io.stream.NamedWriteableRegistry;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.test.HavenaskTestCase;
import org.havenask.test.VersionUtils;

import java.io.IOException;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;

public class GeoTileGridValuesSourceBuilderTests extends HavenaskTestCase {

    public void testSetFormat() {
        CompositeValuesSourceBuilder<?> builder = new GeoTileGridValuesSourceBuilder("name");
        expectThrows(IllegalArgumentException.class, () -> builder.format("format"));
    }

    public void testBWCBounds() throws IOException {
        Version noBoundsSupportVersion = VersionUtils.randomVersionBetween(random(), LegacyESVersion.V_6_0_0, LegacyESVersion.V_7_5_0);
        GeoTileGridValuesSourceBuilder builder = new GeoTileGridValuesSourceBuilder("name");
        if (randomBoolean()) {
            builder.geoBoundingBox(GeoBoundingBoxTests.randomBBox());
        }
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.setVersion(LegacyESVersion.V_7_6_0);
            builder.writeTo(output);
            try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(),
                new NamedWriteableRegistry(Collections.emptyList()))) {
                in.setVersion(noBoundsSupportVersion);
                GeoTileGridValuesSourceBuilder readBuilder = new GeoTileGridValuesSourceBuilder(in);
                assertThat(readBuilder.geoBoundingBox(), equalTo(new GeoBoundingBox(
                    new GeoPoint(Double.NaN, Double.NaN), new GeoPoint(Double.NaN, Double.NaN))));
            }
        }
    }
}
