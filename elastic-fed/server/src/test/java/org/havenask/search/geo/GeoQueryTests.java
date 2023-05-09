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

import org.havenask.action.get.GetResponse;
import org.havenask.action.search.SearchResponse;
import org.havenask.common.geo.GeoShapeType;
import org.havenask.common.geo.ShapeRelation;
import org.havenask.common.geo.builders.CircleBuilder;
import org.havenask.common.geo.builders.CoordinatesBuilder;
import org.havenask.common.geo.builders.EnvelopeBuilder;
import org.havenask.common.geo.builders.GeometryCollectionBuilder;
import org.havenask.common.geo.builders.MultiPolygonBuilder;
import org.havenask.common.geo.builders.PolygonBuilder;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.XContentFactory;
import org.havenask.common.xcontent.XContentType;
import org.havenask.geometry.Geometry;
import org.havenask.geometry.Rectangle;
import org.havenask.index.query.GeoShapeQueryBuilder;
import org.havenask.index.query.QueryBuilders;
import org.havenask.plugins.Plugin;
import org.havenask.search.SearchHits;
import org.havenask.test.HavenaskSingleNodeTestCase;
import org.havenask.test.TestGeoShapeFieldMapperPlugin;
import org.locationtech.jts.geom.Coordinate;

import java.util.Collection;
import java.util.Collections;

import static org.havenask.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.havenask.common.xcontent.XContentFactory.jsonBuilder;
import static org.havenask.test.hamcrest.HavenaskAssertions.assertHitCount;
import static org.havenask.test.hamcrest.HavenaskAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public abstract class GeoQueryTests extends HavenaskSingleNodeTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singleton(TestGeoShapeFieldMapperPlugin.class);
    }

    protected abstract XContentBuilder createTypedMapping() throws Exception;

    protected abstract XContentBuilder createDefaultMapping() throws Exception;

    static String defaultGeoFieldName = "geo";
    static String defaultIndexName = "test";

    public void testNullShape() throws Exception {
        XContentBuilder xcb = createDefaultMapping();
        client().admin().indices().prepareCreate(defaultIndexName).addMapping("_doc", xcb).get();
        ensureGreen();

        client().prepareIndex(defaultIndexName,"_doc")
            .setId("aNullshape")
            .setSource("{\"geo\": null}", XContentType.JSON)
            .setRefreshPolicy(IMMEDIATE).get();
        GetResponse result = client().prepareGet(defaultIndexName,"_doc","aNullshape").get();
        assertThat(result.getField("location"), nullValue());
    }

    public void testIndexPointsFilterRectangle() throws Exception {
        XContentBuilder xcb = createDefaultMapping();
        client().admin().indices().prepareCreate(defaultIndexName).addMapping("_doc", xcb).get();
        ensureGreen();

        client().prepareIndex(defaultIndexName,"_doc").setId("1").setSource(jsonBuilder()
            .startObject()
              .field("name", "Document 1")
              .field(defaultGeoFieldName, "POINT(-30 -30)")
            .endObject()).setRefreshPolicy(IMMEDIATE).get();

        client().prepareIndex(defaultIndexName,"_doc").setId("2").setSource(jsonBuilder()
            .startObject()
              .field("name", "Document 2")
              .field(defaultGeoFieldName, "POINT(-45 -50)")
            .endObject()).setRefreshPolicy(IMMEDIATE).get();

        EnvelopeBuilder shape = new EnvelopeBuilder(new Coordinate(-45, 45), new Coordinate(45, -45));
        GeometryCollectionBuilder builder = new GeometryCollectionBuilder().shape(shape);
        Geometry geometry = builder.buildGeometry().get(0);
        SearchResponse searchResponse = client().prepareSearch(defaultIndexName)
            .setQuery(QueryBuilders.geoShapeQuery(defaultGeoFieldName, geometry)
                .relation(ShapeRelation.INTERSECTS))
            .get();

        assertSearchResponse(searchResponse);
        assertHitCount(searchResponse, 1);
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));

        // default query, without specifying relation (expect intersects)
        searchResponse = client().prepareSearch(defaultIndexName)
            .setQuery(QueryBuilders.geoShapeQuery(defaultGeoFieldName, geometry))
            .get();

        assertSearchResponse(searchResponse);
        assertHitCount(searchResponse, 1);
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
    }

    public void testIndexPointsCircle() throws Exception {
        XContentBuilder xcb = createDefaultMapping();
        client().admin().indices().prepareCreate(defaultIndexName).addMapping("_doc", xcb).get();
        ensureGreen();

        client().prepareIndex(defaultIndexName,"_doc").setId("1").setSource(jsonBuilder()
            .startObject()
            .field("name", "Document 1")
            .field(defaultGeoFieldName, "POINT(-30 -30)")
            .endObject()).setRefreshPolicy(IMMEDIATE).get();

        client().prepareIndex(defaultIndexName,"_doc").setId("2").setSource(jsonBuilder()
            .startObject()
            .field("name", "Document 2")
            .field(defaultGeoFieldName, "POINT(-45 -50)")
            .endObject()).setRefreshPolicy(IMMEDIATE).get();

        CircleBuilder shape = new CircleBuilder().center(new Coordinate(-30, -30)).radius("100m");
        GeometryCollectionBuilder builder = new GeometryCollectionBuilder().shape(shape);
        Geometry geometry = builder.buildGeometry().get(0);

        try {
            client().prepareSearch(defaultIndexName)
                .setQuery(QueryBuilders.geoShapeQuery(defaultGeoFieldName, geometry)
                    .relation(ShapeRelation.INTERSECTS))
                .get();
        } catch (
            Exception e) {
            assertThat(e.getCause().getMessage(),
                containsString("failed to create query: "
                    + GeoShapeType.CIRCLE + " geometry is not supported"));
        }
    }

    public void testIndexPointsPolygon() throws Exception {
        XContentBuilder xcb = createDefaultMapping();
        client().admin().indices().prepareCreate(defaultIndexName).addMapping("_doc", xcb).get();
        ensureGreen();

        client().prepareIndex(defaultIndexName,"_doc").setId("1").setSource(jsonBuilder()
            .startObject()
            .field(defaultGeoFieldName, "POINT(-30 -30)")
            .endObject()).setRefreshPolicy(IMMEDIATE).get();

        client().prepareIndex(defaultIndexName,"_doc").setId("2").setSource(jsonBuilder()
            .startObject()
            .field(defaultGeoFieldName, "POINT(-45 -50)")
            .endObject()).setRefreshPolicy(IMMEDIATE).get();

        CoordinatesBuilder cb = new CoordinatesBuilder();
        cb.coordinate(new Coordinate(-35, -35))
            .coordinate(new Coordinate(-35, -25))
            .coordinate(new Coordinate(-25, -25))
            .coordinate(new Coordinate(-25, -35))
            .coordinate(new Coordinate(-35, -35));
        PolygonBuilder shape = new PolygonBuilder(cb);
        GeometryCollectionBuilder builder = new GeometryCollectionBuilder().shape(shape);
        Geometry geometry = builder.buildGeometry();
        SearchResponse searchResponse = client().prepareSearch(defaultIndexName)
            .setQuery(QueryBuilders.geoShapeQuery(defaultGeoFieldName, geometry)
                .relation(ShapeRelation.INTERSECTS))
            .get();

        assertSearchResponse(searchResponse);
        assertHitCount(searchResponse, 1);
        SearchHits searchHits = searchResponse.getHits();
        assertThat(searchHits.getAt(0).getId(), equalTo("1"));
    }

    public void testIndexPointsMultiPolygon() throws Exception {
        XContentBuilder xcb = createDefaultMapping();
        client().admin().indices().prepareCreate(defaultIndexName).addMapping("_doc", xcb).get();
        ensureGreen();

        client().prepareIndex(defaultIndexName,"_doc").setId("1").setSource(jsonBuilder()
            .startObject()
            .field("name", "Document 1")
            .field(defaultGeoFieldName, "POINT(-30 -30)")
            .endObject()).setRefreshPolicy(IMMEDIATE).get();

        client().prepareIndex(defaultIndexName,"_doc").setId("2").setSource(jsonBuilder()
            .startObject()
            .field("name", "Document 2")
            .field(defaultGeoFieldName, "POINT(-40 -40)")
            .endObject()).setRefreshPolicy(IMMEDIATE).get();

        client().prepareIndex(defaultIndexName,"_doc").setId("3").setSource(jsonBuilder()
            .startObject()
            .field("name", "Document 3")
            .field(defaultGeoFieldName, "POINT(-50 -50)")
            .endObject()).setRefreshPolicy(IMMEDIATE).get();

        CoordinatesBuilder encloseDocument1Cb = new CoordinatesBuilder();
        encloseDocument1Cb.coordinate(new Coordinate(-35, -35))
            .coordinate(new Coordinate(-35, -25))
            .coordinate(new Coordinate(-25, -25))
            .coordinate(new Coordinate(-25, -35))
            .coordinate(new Coordinate(-35, -35));
        PolygonBuilder encloseDocument1Shape = new PolygonBuilder(encloseDocument1Cb);

        CoordinatesBuilder encloseDocument2Cb = new CoordinatesBuilder();
        encloseDocument2Cb.coordinate(new Coordinate(-55, -55))
            .coordinate(new Coordinate(-55, -45))
            .coordinate(new Coordinate(-45, -45))
            .coordinate(new Coordinate(-45, -55))
            .coordinate(new Coordinate(-55, -55));
        PolygonBuilder encloseDocument2Shape = new PolygonBuilder(encloseDocument2Cb);

        MultiPolygonBuilder mp = new MultiPolygonBuilder();
        mp.polygon(encloseDocument1Shape).polygon(encloseDocument2Shape);

        GeometryCollectionBuilder builder = new GeometryCollectionBuilder().shape(mp);
        Geometry geometry = builder.buildGeometry();
        SearchResponse searchResponse = client().prepareSearch(defaultIndexName)
            .setQuery(QueryBuilders.geoShapeQuery(defaultGeoFieldName, geometry)
                .relation(ShapeRelation.INTERSECTS))
            .get();

        assertSearchResponse(searchResponse);
        assertHitCount(searchResponse, 2);
        assertThat(searchResponse.getHits().getAt(0).getId(), not(equalTo("2")));
        assertThat(searchResponse.getHits().getAt(1).getId(), not(equalTo("2")));
    }

    public void testIndexPointsRectangle() throws Exception {
        XContentBuilder xcb = createDefaultMapping();
        client().admin().indices().prepareCreate(defaultIndexName).addMapping("_doc", xcb).get();
        ensureGreen();

        client().prepareIndex(defaultIndexName,"_doc").setId("1").setSource(jsonBuilder()
            .startObject()
            .field("name", "Document 1")
            .field(defaultGeoFieldName, "POINT(-30 -30)")
            .endObject()).setRefreshPolicy(IMMEDIATE).get();

        client().prepareIndex(defaultIndexName,"_doc").setId("2").setSource(jsonBuilder()
            .startObject()
            .field("name", "Document 2")
            .field(defaultGeoFieldName, "POINT(-45 -50)")
            .endObject()).setRefreshPolicy(IMMEDIATE).get();

        Rectangle rectangle = new Rectangle(-50, -40, -45, -55);

        SearchResponse searchResponse = client().prepareSearch(defaultIndexName)
            .setQuery(QueryBuilders.geoShapeQuery(defaultGeoFieldName, rectangle)
                .relation(ShapeRelation.INTERSECTS))
            .get();

        assertSearchResponse(searchResponse);
        assertHitCount(searchResponse, 1);
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("2"));
    }

    public void testIndexPointsIndexedRectangle() throws Exception {
        XContentBuilder xcb = createDefaultMapping();
        client().admin().indices().prepareCreate(defaultIndexName).addMapping(defaultIndexName, xcb).get();
        ensureGreen();

        client().prepareIndex(defaultIndexName,"_doc").setId("point1").setSource(jsonBuilder()
            .startObject()
            .field(defaultGeoFieldName, "POINT(-30 -30)")
            .endObject()).setRefreshPolicy(IMMEDIATE).get();

        client().prepareIndex(defaultIndexName,"_doc").setId("point2").setSource(jsonBuilder()
            .startObject()
            .field(defaultGeoFieldName, "POINT(-45 -50)")
            .endObject()).setRefreshPolicy(IMMEDIATE).get();

        String indexedShapeIndex = "indexed_query_shapes";
        String indexedShapePath = "shape";
        xcb = XContentFactory.jsonBuilder().startObject()
            .startObject("properties").startObject(indexedShapePath)
            .field("type", "geo_shape")
            .endObject()
            .endObject()
            .endObject();
        client().admin().indices().prepareCreate(indexedShapeIndex).addMapping(defaultIndexName, xcb).get();
        ensureGreen();

        client().prepareIndex(indexedShapeIndex,"_doc").setId("shape1").setSource(jsonBuilder()
            .startObject()
            .field(indexedShapePath, "BBOX(-50, -40, -45, -55)")
            .endObject()).setRefreshPolicy(IMMEDIATE).get();

        client().prepareIndex(indexedShapeIndex,"_doc").setId("shape2").setSource(jsonBuilder()
            .startObject()
            .field(indexedShapePath, "BBOX(-60, -50, -50, -60)")
            .endObject()).setRefreshPolicy(IMMEDIATE).get();

        SearchResponse searchResponse = client().prepareSearch(defaultIndexName)
            .setQuery(QueryBuilders.geoShapeQuery(defaultGeoFieldName, "shape1")
                .relation(ShapeRelation.INTERSECTS)
                .indexedShapeIndex(indexedShapeIndex)
                .indexedShapePath(indexedShapePath))
            .get();

        assertSearchResponse(searchResponse);
        assertHitCount(searchResponse, 1);
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("point2"));

        searchResponse = client().prepareSearch(defaultIndexName)
            .setQuery(QueryBuilders.geoShapeQuery(defaultGeoFieldName, "shape2")
                .relation(ShapeRelation.INTERSECTS)
                .indexedShapeIndex(indexedShapeIndex)
                .indexedShapePath(indexedShapePath))
            .get();
        assertSearchResponse(searchResponse);
        assertHitCount(searchResponse, 0);
    }

    public void testRectangleSpanningDateline() throws Exception {
        XContentBuilder xcb = createDefaultMapping();
        client().admin().indices().prepareCreate("test").addMapping("_doc", xcb).get();
        ensureGreen();

        client().prepareIndex(defaultIndexName,"_doc").setId("1").setSource(jsonBuilder()
            .startObject()
            .field(defaultGeoFieldName, "POINT(-169 0)")
            .endObject()).setRefreshPolicy(IMMEDIATE).get();

        client().prepareIndex(defaultIndexName,"_doc").setId("2").setSource(jsonBuilder()
            .startObject()
            .field(defaultGeoFieldName, "POINT(-179 0)")
            .endObject()).setRefreshPolicy(IMMEDIATE).get();

        client().prepareIndex(defaultIndexName,"_doc").setId("3").setSource(jsonBuilder()
            .startObject()
            .field(defaultGeoFieldName, "POINT(171 0)")
            .endObject()).setRefreshPolicy(IMMEDIATE).get();

        Rectangle rectangle = new Rectangle(
            169, -178, 1, -1);

        GeoShapeQueryBuilder geoShapeQueryBuilder = QueryBuilders.geoShapeQuery("geo", rectangle);
        SearchResponse response = client().prepareSearch("test").setQuery(geoShapeQueryBuilder).get();
        assertHitCount(response, 2);
        SearchHits searchHits = response.getHits();
        assertThat(searchHits.getAt(0).getId(),not(equalTo("1")));
        assertThat(searchHits.getAt(1).getId(),not(equalTo("1")));
    }

    public void testPolygonSpanningDateline() throws Exception {
        XContentBuilder xcb = createDefaultMapping();
        client().admin().indices().prepareCreate("test").addMapping("_doc", xcb).get();
        ensureGreen();

        client().prepareIndex(defaultIndexName,"_doc").setId("1").setSource(jsonBuilder()
            .startObject()
            .field(defaultGeoFieldName, "POINT(-169 7)")
            .endObject()).setRefreshPolicy(IMMEDIATE).get();

        client().prepareIndex(defaultIndexName,"_doc").setId("2").setSource(jsonBuilder()
            .startObject()
            .field(defaultGeoFieldName, "POINT(-179 7)")
            .endObject()).setRefreshPolicy(IMMEDIATE).get();

        client().prepareIndex(defaultIndexName,"_doc").setId("3").setSource(jsonBuilder()
            .startObject()
            .field(defaultGeoFieldName, "POINT(179 7)")
            .endObject()).setRefreshPolicy(IMMEDIATE).get();

        client().prepareIndex(defaultIndexName,"_doc").setId("4").setSource(jsonBuilder()
            .startObject()
            .field(defaultGeoFieldName, "POINT(171 7)")
            .endObject()).setRefreshPolicy(IMMEDIATE).get();

        PolygonBuilder polygon = new PolygonBuilder(new CoordinatesBuilder()
                    .coordinate(-177, 10)
                    .coordinate(177, 10)
                    .coordinate(177, 5)
                    .coordinate(-177, 5)
                    .coordinate(-177, 10));

        GeoShapeQueryBuilder geoShapeQueryBuilder = QueryBuilders.geoShapeQuery("geo", polygon.buildGeometry());
        geoShapeQueryBuilder.relation(ShapeRelation.INTERSECTS);
        SearchResponse response = client().prepareSearch("test").setQuery(geoShapeQueryBuilder).get();
        assertHitCount(response, 2);
        SearchHits searchHits = response.getHits();
        assertThat(searchHits.getAt(0).getId(),not(equalTo("1")));
        assertThat(searchHits.getAt(1).getId(),not(equalTo("1")));
        assertThat(searchHits.getAt(0).getId(),not(equalTo("4")));
        assertThat(searchHits.getAt(1).getId(),not(equalTo("4")));
    }

    public void testMultiPolygonSpanningDateline() throws Exception {
        XContentBuilder xcb = createDefaultMapping();
        client().admin().indices().prepareCreate("test").addMapping("_doc", xcb).get();
        ensureGreen();

        client().prepareIndex(defaultIndexName,"_doc").setId("1").setSource(jsonBuilder()
            .startObject()
            .field(defaultGeoFieldName, "POINT(-169 7)")
            .endObject()).setRefreshPolicy(IMMEDIATE).get();

        client().prepareIndex(defaultIndexName,"_doc").setId("2").setSource(jsonBuilder()
            .startObject()
            .field(defaultGeoFieldName, "POINT(-179 7)")
            .endObject()).setRefreshPolicy(IMMEDIATE).get();

        client().prepareIndex(defaultIndexName,"_doc").setId("3").setSource(jsonBuilder()
            .startObject()
            .field(defaultGeoFieldName, "POINT(171 7)")
            .endObject()).setRefreshPolicy(IMMEDIATE).get();

        MultiPolygonBuilder multiPolygon = new MultiPolygonBuilder()
            .polygon(new PolygonBuilder(new CoordinatesBuilder()
                .coordinate(-167, 10)
                .coordinate(-171, 10)
                .coordinate(171, 5)
                .coordinate(-167, 5)
                .coordinate(-167, 10)))
            .polygon(new PolygonBuilder(new CoordinatesBuilder()
                .coordinate(-177, 10)
                .coordinate(177, 10)
                .coordinate(177, 5)
                .coordinate(-177, 5)
                .coordinate(-177, 10)));

        GeoShapeQueryBuilder geoShapeQueryBuilder = QueryBuilders.geoShapeQuery(
            "geo",
            multiPolygon.buildGeometry());
        geoShapeQueryBuilder.relation(ShapeRelation.INTERSECTS);
        SearchResponse response = client().prepareSearch("test").setQuery(geoShapeQueryBuilder).get();
        assertHitCount(response, 2);
        SearchHits searchHits = response.getHits();
        assertThat(searchHits.getAt(0).getId(),not(equalTo("3")));
        assertThat(searchHits.getAt(1).getId(),not(equalTo("3")));
    }
}
