/*
 * Copyright (c) 2021, Alibaba Group;
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.havenask.engine.search.rest;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.alibaba.fastjson.JSONObject;
import org.havenask.common.bytes.BytesArray;
import org.havenask.common.xcontent.XContentType;
import org.havenask.engine.search.action.HavenaskSqlRequest;
import org.havenask.rest.RestRequest;
import org.havenask.test.HavenaskTestCase;
import org.havenask.test.rest.FakeRestRequest;

import static org.havenask.engine.search.rest.RestHavenaskSqlAction.SQL_DATABASE;

public class RestHavenaskSqlActionTests extends HavenaskTestCase {

    public void testBuildKvpair() {
        Map<String, String> params = new HashMap<>();
        params.put("trace", "true");
        params.put("format", "json");
        params.put("timeout", "1000");
        params.put("searchInfo", "true");
        params.put("sqlPlan", "true");
        params.put("forbitMergeSearchInfo", "true");
        params.put("resultReadable", "true");
        params.put("parallel", "2");
        params.put("parallelTables", "t1,t2");
        params.put("databaseName", "db1");
        params.put("lackResultEnable", "true");
        params.put("optimizerDebug", "true");
        params.put("sortLimitTogether", "false");
        params.put("forceLimit", "true");
        params.put("joinConditionCheck", "false");
        params.put("forceJoinHask", "true");
        params.put("planLevel", "true");
        params.put("cacheEnable", "true");
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(params).build();

        String kvpair = RestHavenaskSqlAction.buildKvpair(request);
        assertEquals(
            "databaseName:db1;trace:true;format:json;timeout:1000;searchInfo:true;sqlPlan:true;"
                + "forbitMergeSearchInfo:true;resultReadable:true;parallel:2;"
                + "parallelTables:t1,t2;lackResultEnable:true;optimizerDebug:true;sortLimitTogether:false;"
                + "forceLimit:true;"
                + "joinConditionCheck:false;forceJoinHask:true;planLevel:true;cacheEnable:true",
            kvpair
        );
    }

    public void testBuildKvpair2() {
        Map<String, String> params = new HashMap<>();
        params.put("trace", null);
        params.put("format", "json");
        params.put("timeout", "1000");
        params.put("searchInfo", null);
        params.put("sqlPlan", null);
        params.put("forbitMergeSearchInfo", null);
        params.put("resultReadable", null);
        params.put("parallel", null);
        params.put("parallelTables", "t1,t2");
        params.put("databaseName", "db1");
        params.put("lackResultEnable", null);
        params.put("optimizerDebug", null);
        params.put("sortLimitTogether", null);
        params.put("forceLimit", null);
        params.put("joinConditionCheck", null);
        params.put("forceJoinHask", null);
        params.put("planLevel", null);
        params.put("cacheEnable", null);

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(params).build();

        String kvpair = RestHavenaskSqlAction.buildKvpair(request);
        assertEquals("databaseName:db1;format:json;timeout:1000;parallelTables:t1,t2", kvpair);
    }

    public void testBuildKvpairNull() {
        Map<String, String> params = new HashMap<>();

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(params).build();

        String kvpair = RestHavenaskSqlAction.buildKvpair(request);
        assertEquals("databaseName:" + SQL_DATABASE, kvpair);
    }

    public void testBuildKvpairRandom() {
        Map<String, String> params = new HashMap<>();
        params.put("trace", randomBoolean() ? "true" : null);
        params.put("format", randomBoolean() ? "json" : null);
        params.put("timeout", randomBoolean() ? "1000" : null);
        params.put("searchInfo", randomBoolean() ? "true" : "false");
        params.put("sqlPlan", randomBoolean() ? "true" : "false");
        params.put("forbitMergeSearchInfo", randomBoolean() ? "true" : "false");
        params.put("resultReadable", randomBoolean() ? "true" : "false");
        params.put("parallel", randomBoolean() ? "2" : null);
        params.put("parallelTables", randomBoolean() ? "t1,t2" : null);
        params.put("databaseName", randomBoolean() ? "db1" : null);
        params.put("lackResultEnable", randomBoolean() ? "true" : "false");
        params.put("optimizerDebug", randomBoolean() ? "true" : "false");
        params.put("sortLimitTogether", randomBoolean() ? "false" : "true");
        params.put("forceLimit", randomBoolean() ? "true" : "false");
        params.put("joinConditionCheck", randomBoolean() ? "false" : "true");
        params.put("forceJoinHask", randomBoolean() ? "true" : "false");
        params.put("planLevel", randomBoolean() ? "true" : "false");
        params.put("cacheEnable", randomBoolean() ? "true" : "false");

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(params).build();

        String kvpair = RestHavenaskSqlAction.buildKvpair(request);
        String expected = "";
        if (params.get("databaseName") != null) {
            expected += "databaseName:" + params.get("databaseName") + ";";
        } else {
            expected += "databaseName:" + SQL_DATABASE + ";";
        }
        if (params.get("trace") != null) {
            expected += "trace:" + params.get("trace") + ";";
        }
        if (params.get("format") != null) {
            expected += "format:" + params.get("format") + ";";
        }
        if (params.get("timeout") != null) {
            expected += "timeout:" + params.get("timeout") + ";";
        }
        if (params.get("searchInfo") != "false") {
            expected += "searchInfo:" + params.get("searchInfo") + ";";
        }
        if (params.get("sqlPlan") != "false") {
            expected += "sqlPlan:" + params.get("sqlPlan") + ";";
        }
        if (params.get("forbitMergeSearchInfo") != "false") {
            expected += "forbitMergeSearchInfo:" + params.get("forbitMergeSearchInfo") + ";";
        }
        if (params.get("resultReadable") != "false") {
            expected += "resultReadable:" + params.get("resultReadable") + ";";
        }
        if (params.get("parallel") != null) {
            expected += "parallel:" + params.get("parallel") + ";";
        }
        if (params.get("parallelTables") != null) {
            expected += "parallelTables:" + params.get("parallelTables") + ";";
        }
        if (params.get("lackResultEnable") != "false") {
            expected += "lackResultEnable:" + params.get("lackResultEnable") + ";";
        }
        if (params.get("optimizerDebug") != "false") {
            expected += "optimizerDebug:" + params.get("optimizerDebug") + ";";
        }
        if (params.get("sortLimitTogether") != "true") {
            expected += "sortLimitTogether:" + params.get("sortLimitTogether") + ";";
        }
        if (params.get("forceLimit") != "false") {
            expected += "forceLimit:" + params.get("forceLimit") + ";";
        }
        if (params.get("joinConditionCheck") != "true") {
            expected += "joinConditionCheck:" + params.get("joinConditionCheck") + ";";
        }
        if (params.get("forceJoinHask") != "false") {
            expected += "forceJoinHask:" + params.get("forceJoinHask") + ";";
        }
        if (params.get("planLevel") != "false") {
            expected += "planLevel:" + params.get("planLevel") + ";";
        }
        if (params.get("cacheEnable") != "false") {
            expected += "cacheEnable:" + params.get("cacheEnable") + ";";
        }
        if (expected.endsWith(";")) {
            expected = expected.substring(0, expected.length() - 1);
        }
        if (expected.equals("")) {
            expected = null;
        }

        assertEquals(expected, kvpair);
    }

    public void testQueryInPostBody() throws IOException {
        String jsonContent = new JSONObject().fluentPut("query", "select * from `test`").toJSONString();

        Map<String, String> kvpair = buildKvpairRandom();
        String expectedKvpair = buildExpectedKvpair(kvpair);

        Map<String, String> params = new HashMap<>();
        params.putAll(kvpair);

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_sql")
            .withParams(params)
            .withContent(new BytesArray(jsonContent), XContentType.JSON)
            .build();

        HavenaskSqlRequest havenaskSqlRequest = RestHavenaskSqlAction.createHavenaskSqlRequest(request);
        assertEquals("select * from `test`", havenaskSqlRequest.getSql());

        assertEquals(expectedKvpair, havenaskSqlRequest.getKvpair());
    }

    public void testQueryInUrlParams() throws IOException {
        Map<String, String> kvpair = buildKvpairRandom();
        String expectedKvpair = buildExpectedKvpair(kvpair);

        Map<String, String> params = new HashMap<>();
        params.put("query", "select * from `test`");
        params.putAll(kvpair);

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(params).build();

        HavenaskSqlRequest havenaskSqlRequest = RestHavenaskSqlAction.createHavenaskSqlRequest(request);
        assertEquals("select * from `test`", havenaskSqlRequest.getSql());

        assertEquals(expectedKvpair, havenaskSqlRequest.getKvpair());
    }

    public void testQueryBothInUrlParamsAndPostBody() throws IOException {
        String jsonContent = new JSONObject().fluentPut("query", "select * from `post_body_test`").toJSONString();

        Map<String, String> kvpair = buildKvpairRandom();
        String expectedKvpair = buildExpectedKvpair(kvpair);

        Map<String, String> params = new HashMap<>();
        params.put("query", "select * from `url_params_test`");
        params.putAll(kvpair);

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(params)
            .withContent(new BytesArray(jsonContent), XContentType.JSON)
            .build();

        HavenaskSqlRequest havenaskSqlRequest = RestHavenaskSqlAction.createHavenaskSqlRequest(request);
        assertEquals("select * from `post_body_test`", havenaskSqlRequest.getSql());

        assertEquals(expectedKvpair, havenaskSqlRequest.getKvpair());
    }

    public void testQueryNotExist() {
        Map<String, String> kvpair = buildKvpairRandom();

        Map<String, String> params = new HashMap<>();
        params.putAll(kvpair);

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(params).build();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> RestHavenaskSqlAction.createHavenaskSqlRequest(request)
        );
        assertEquals("query is null", e.getMessage());
    }

    private Map<String, String> buildKvpairRandom() {
        Map<String, String> params = new HashMap<>();
        params.put("trace", randomBoolean() ? "true" : null);
        params.put("format", randomBoolean() ? "json" : null);
        params.put("timeout", randomBoolean() ? "1000" : null);
        params.put("searchInfo", randomBoolean() ? "true" : "false");
        params.put("sqlPlan", randomBoolean() ? "true" : "false");
        params.put("forbitMergeSearchInfo", randomBoolean() ? "true" : "false");
        params.put("resultReadable", randomBoolean() ? "true" : "false");
        params.put("parallel", randomBoolean() ? "2" : null);
        params.put("parallelTables", randomBoolean() ? "t1,t2" : null);
        params.put("databaseName", randomBoolean() ? "db1" : null);
        params.put("lackResultEnable", randomBoolean() ? "true" : "false");
        params.put("optimizerDebug", randomBoolean() ? "true" : "false");
        params.put("sortLimitTogether", randomBoolean() ? "false" : "true");
        params.put("forceLimit", randomBoolean() ? "true" : "false");
        params.put("joinConditionCheck", randomBoolean() ? "false" : "true");
        params.put("forceJoinHask", randomBoolean() ? "true" : "false");
        params.put("planLevel", randomBoolean() ? "true" : "false");
        params.put("cacheEnable", randomBoolean() ? "true" : "false");
        return params;
    }

    private String buildExpectedKvpair(Map<String, String> params) {
        String expected = "";
        if (params.get("databaseName") != null) {
            expected += "databaseName:" + params.get("databaseName") + ";";
        } else {
            expected += "databaseName:" + SQL_DATABASE + ";";
        }
        if (params.get("trace") != null) {
            expected += "trace:" + params.get("trace") + ";";
        }
        if (params.get("format") != null) {
            expected += "format:" + params.get("format") + ";";
        }
        if (params.get("timeout") != null) {
            expected += "timeout:" + params.get("timeout") + ";";
        }
        if (params.get("searchInfo") != "false") {
            expected += "searchInfo:" + params.get("searchInfo") + ";";
        }
        if (params.get("sqlPlan") != "false") {
            expected += "sqlPlan:" + params.get("sqlPlan") + ";";
        }
        if (params.get("forbitMergeSearchInfo") != "false") {
            expected += "forbitMergeSearchInfo:" + params.get("forbitMergeSearchInfo") + ";";
        }
        if (params.get("resultReadable") != "false") {
            expected += "resultReadable:" + params.get("resultReadable") + ";";
        }
        if (params.get("parallel") != null) {
            expected += "parallel:" + params.get("parallel") + ";";
        }
        if (params.get("parallelTables") != null) {
            expected += "parallelTables:" + params.get("parallelTables") + ";";
        }
        if (params.get("lackResultEnable") != "false") {
            expected += "lackResultEnable:" + params.get("lackResultEnable") + ";";
        }
        if (params.get("optimizerDebug") != "false") {
            expected += "optimizerDebug:" + params.get("optimizerDebug") + ";";
        }
        if (params.get("sortLimitTogether") != "true") {
            expected += "sortLimitTogether:" + params.get("sortLimitTogether") + ";";
        }
        if (params.get("forceLimit") != "false") {
            expected += "forceLimit:" + params.get("forceLimit") + ";";
        }
        if (params.get("joinConditionCheck") != "true") {
            expected += "joinConditionCheck:" + params.get("joinConditionCheck") + ";";
        }
        if (params.get("forceJoinHask") != "false") {
            expected += "forceJoinHask:" + params.get("forceJoinHask") + ";";
        }
        if (params.get("planLevel") != "false") {
            expected += "planLevel:" + params.get("planLevel") + ";";
        }
        if (params.get("cacheEnable") != "false") {
            expected += "cacheEnable:" + params.get("cacheEnable") + ";";
        }
        if (expected.endsWith(";")) {
            expected = expected.substring(0, expected.length() - 1);
        }
        if (expected.equals("")) {
            expected = null;
        }
        return expected;
    }
}
