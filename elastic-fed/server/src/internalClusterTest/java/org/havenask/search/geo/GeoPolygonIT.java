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

package org.havenask.search.geo;

import org.havenask.LegacyESVersion;
import org.havenask.Version;
import org.havenask.action.search.SearchResponse;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.common.geo.GeoPoint;
import org.havenask.common.settings.Settings;
import org.havenask.search.SearchHit;
import org.havenask.test.HavenaskIntegTestCase;
import org.havenask.test.VersionUtils;

import java.util.ArrayList;
import java.util.List;

import static org.havenask.common.xcontent.XContentFactory.jsonBuilder;
import static org.havenask.index.query.QueryBuilders.boolQuery;
import static org.havenask.index.query.QueryBuilders.geoPolygonQuery;
import static org.havenask.test.hamcrest.HavenaskAssertions.assertAcked;
import static org.havenask.test.hamcrest.HavenaskAssertions.assertHitCount;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;

@HavenaskIntegTestCase.SuiteScopeTestCase
public class GeoPolygonIT extends HavenaskIntegTestCase {

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    @Override
    protected void setupSuiteScopeCluster() throws Exception {
        Version version = VersionUtils.randomVersionBetween(random(), LegacyESVersion.V_6_0_0,
                Version.CURRENT);
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, version).build();

        assertAcked(prepareCreate("test").setSettings(settings).addMapping("type1", "location",
            "type=geo_point", "alias",
            "type=alias,path=location"));
        ensureGreen();

        indexRandom(true, client().prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject()
                .field("name", "New York")
                .startObject("location").field("lat", 40.714).field("lon", -74.006).endObject()
                .endObject()),
        // to NY: 5.286 km
        client().prepareIndex("test", "type1", "2").setSource(jsonBuilder().startObject()
                .field("name", "Times Square")
                .startObject("location").field("lat", 40.759).field("lon", -73.984).endObject()
                .endObject()),
        // to NY: 0.4621 km
        client().prepareIndex("test", "type1", "3").setSource(jsonBuilder().startObject()
                .field("name", "Tribeca")
                .startObject("location").field("lat", 40.718).field("lon", -74.008).endObject()
                .endObject()),
        // to NY: 1.055 km
        client().prepareIndex("test", "type1", "4").setSource(jsonBuilder().startObject()
                .field("name", "Wall Street")
                .startObject("location").field("lat", 40.705).field("lon", -74.009).endObject()
                .endObject()),
        // to NY: 1.258 km
        client().prepareIndex("test", "type1", "5").setSource(jsonBuilder().startObject()
                .field("name", "Soho")
                .startObject("location").field("lat", 40.725).field("lon", -74).endObject()
                .endObject()),
        // to NY: 2.029 km
        client().prepareIndex("test", "type1", "6").setSource(jsonBuilder().startObject()
                .field("name", "Greenwich Village")
                .startObject("location").field("lat", 40.731).field("lon", -73.996).endObject()
                .endObject()),
        // to NY: 8.572 km
        client().prepareIndex("test", "type1", "7").setSource(jsonBuilder().startObject()
                .field("name", "Brooklyn")
                .startObject("location").field("lat", 40.65).field("lon", -73.95).endObject()
                .endObject()));
        ensureSearchable("test");
    }

    public void testSimplePolygon() throws Exception {
        List<GeoPoint> points = new ArrayList<>();
        points.add(new GeoPoint(40.7, -74.0));
        points.add(new GeoPoint(40.7, -74.1));
        points.add(new GeoPoint(40.8, -74.1));
        points.add(new GeoPoint(40.8, -74.0));
        points.add(new GeoPoint(40.7, -74.0));
        SearchResponse searchResponse = client().prepareSearch("test") // from NY
                .setQuery(boolQuery().must(geoPolygonQuery("location", points)))
                .get();
        assertHitCount(searchResponse, 4);
        assertThat(searchResponse.getHits().getHits().length, equalTo(4));
        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(hit.getId(), anyOf(equalTo("1"), equalTo("3"), equalTo("4"), equalTo("5")));
        }
    }

    public void testSimpleUnclosedPolygon() throws Exception {
        List<GeoPoint> points = new ArrayList<>();
        points.add(new GeoPoint(40.7, -74.0));
        points.add(new GeoPoint(40.7, -74.1));
        points.add(new GeoPoint(40.8, -74.1));
        points.add(new GeoPoint(40.8, -74.0));
        SearchResponse searchResponse = client().prepareSearch("test") // from NY
                .setQuery(boolQuery().must(geoPolygonQuery("location", points))).get();
        assertHitCount(searchResponse, 4);
        assertThat(searchResponse.getHits().getHits().length, equalTo(4));
        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(hit.getId(), anyOf(equalTo("1"), equalTo("3"), equalTo("4"), equalTo("5")));
        }
    }

    public void testFieldAlias() {
        List<GeoPoint> points = new ArrayList<>();
        points.add(new GeoPoint(40.7, -74.0));
        points.add(new GeoPoint(40.7, -74.1));
        points.add(new GeoPoint(40.8, -74.1));
        points.add(new GeoPoint(40.8, -74.0));
        points.add(new GeoPoint(40.7, -74.0));
        SearchResponse searchResponse = client().prepareSearch("test") // from NY
            .setQuery(boolQuery().must(geoPolygonQuery("alias", points)))
            .get();
        assertHitCount(searchResponse, 4);
    }
}
