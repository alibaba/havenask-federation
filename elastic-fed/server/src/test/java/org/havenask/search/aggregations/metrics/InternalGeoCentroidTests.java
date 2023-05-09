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

package org.havenask.search.aggregations.metrics;

import org.apache.lucene.geo.GeoEncodingUtils;
import org.havenask.common.geo.GeoPoint;
import org.havenask.search.aggregations.ParsedAggregation;
import org.havenask.test.InternalAggregationTestCase;
import org.havenask.test.geo.RandomGeoGenerator;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class InternalGeoCentroidTests extends InternalAggregationTestCase<InternalGeoCentroid> {

    @Override
    protected InternalGeoCentroid createTestInstance(String name, Map<String, Object> metadata) {
        GeoPoint centroid = RandomGeoGenerator.randomPoint(random());

        // Re-encode lat/longs to avoid rounding issue when testing InternalGeoCentroid#hashCode() and
        // InternalGeoCentroid#equals()
        int encodedLon = GeoEncodingUtils.encodeLongitude(centroid.lon());
        centroid.resetLon(GeoEncodingUtils.decodeLongitude(encodedLon));
        int encodedLat = GeoEncodingUtils.encodeLatitude(centroid.lat());
        centroid.resetLat(GeoEncodingUtils.decodeLatitude(encodedLat));
        long count = randomIntBetween(0, 1000);
        if (count == 0) {
            centroid = null;
        }
        return new InternalGeoCentroid(name, centroid, count, Collections.emptyMap());
    }

    @Override
    protected void assertReduced(InternalGeoCentroid reduced, List<InternalGeoCentroid> inputs) {
        double lonSum = 0;
        double latSum = 0;
        long totalCount = 0;
        for (InternalGeoCentroid input : inputs) {
            if (input.count() > 0) {
                lonSum += (input.count() * input.centroid().getLon());
                latSum += (input.count() * input.centroid().getLat());
            }
            totalCount += input.count();
        }
        if (totalCount > 0) {
            assertEquals(latSum/totalCount, reduced.centroid().getLat(), 1E-5D);
            assertEquals(lonSum/totalCount, reduced.centroid().getLon(), 1E-5D);
        }
        assertEquals(totalCount, reduced.count());
    }

    public void testReduceMaxCount() {
        InternalGeoCentroid maxValueGeoCentroid = new InternalGeoCentroid("agg", new GeoPoint(10, 0),
            Long.MAX_VALUE, Collections.emptyMap());
        InternalGeoCentroid reducedGeoCentroid = maxValueGeoCentroid
            .reduce(Collections.singletonList(maxValueGeoCentroid), null);
        assertThat(reducedGeoCentroid.count(), equalTo(Long.MAX_VALUE));
    }

    @Override
    protected void assertFromXContent(InternalGeoCentroid aggregation, ParsedAggregation parsedAggregation) {
        assertTrue(parsedAggregation instanceof ParsedGeoCentroid);
        ParsedGeoCentroid parsed = (ParsedGeoCentroid) parsedAggregation;

        assertEquals(aggregation.centroid(), parsed.centroid());
        assertEquals(aggregation.count(), parsed.count());
    }

    @Override
    protected InternalGeoCentroid mutateInstance(InternalGeoCentroid instance) {
        String name = instance.getName();
        GeoPoint centroid = instance.centroid();
        long count = instance.count();
        Map<String, Object> metadata = instance.getMetadata();
        switch (between(0, 2)) {
        case 0:
            name += randomAlphaOfLength(5);
            break;
        case 1:
            count += between(1, 100);
            if (centroid == null) {
                // if the new count is > 0 then we need to make sure there is a
                // centroid or the constructor will throw an exception
                centroid = new GeoPoint(randomDoubleBetween(-90, 90, false), randomDoubleBetween(-180, 180, false));
            }
            break;
        case 2:
            if (centroid == null) {
                centroid = new GeoPoint(randomDoubleBetween(-90, 90, false), randomDoubleBetween(-180, 180, false));
                count = between(1, 100);
            } else {
                GeoPoint newCentroid = new GeoPoint(centroid);
                if (randomBoolean()) {
                    newCentroid.resetLat(centroid.getLat() / 2.0);
                } else {
                    newCentroid.resetLon(centroid.getLon() / 2.0);
                }
                centroid = newCentroid;
            }
            break;
        case 3:
            if (metadata == null) {
                metadata = new HashMap<>(1);
            } else {
                metadata = new HashMap<>(instance.getMetadata());
            }
            metadata.put(randomAlphaOfLength(15), randomInt());
            break;
        default:
            throw new AssertionError("Illegal randomisation branch");
        }
        return new InternalGeoCentroid(name, centroid, count, metadata);
    }
}
