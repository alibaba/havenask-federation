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

package org.havenask.action.admin.indices.segments;

import org.havenask.action.support.IndicesOptions;
import org.havenask.common.settings.Settings;
import org.havenask.index.engine.Segment;
import org.havenask.index.MergePolicyConfig;
import org.havenask.indices.IndexClosedException;
import org.havenask.plugins.Plugin;
import org.havenask.test.HavenaskSingleNodeTestCase;
import org.havenask.test.InternalSettingsPlugin;
import org.junit.Before;

import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.is;

public class IndicesSegmentsRequestTests extends HavenaskSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class);
    }

    @Before
    public void setupIndex() {
        Settings settings = Settings.builder()
            // don't allow any merges so that the num docs is the expected segments
            .put(MergePolicyConfig.INDEX_MERGE_ENABLED, false)
            .build();
        createIndex("test", settings);

        int numDocs = scaledRandomIntBetween(100, 1000);
        for (int j = 0; j < numDocs; ++j) {
            String id = Integer.toString(j);
            client().prepareIndex("test", "type1", id).setSource("text", "sometext").get();
        }
        client().admin().indices().prepareFlush("test").get();
        client().admin().indices().prepareRefresh().get();
    }

    public void testBasic() {
        IndicesSegmentResponse rsp = client().admin().indices().prepareSegments("test").get();
        List<Segment> segments = rsp.getIndices().get("test").iterator().next().getShards()[0].getSegments();
        assertNull(segments.get(0).toString(), segments.get(0).ramTree);
    }

    public void testVerbose() {
        IndicesSegmentResponse rsp = client().admin().indices().prepareSegments("test").setVerbose(true).get();
        List<Segment> segments = rsp.getIndices().get("test").iterator().next().getShards()[0].getSegments();
        assertNotNull(segments.get(0).toString(), segments.get(0).ramTree);
    }

    /**
     * with the default IndicesOptions inherited from BroadcastOperationRequest this will raise an exception
     */
    public void testRequestOnClosedIndex() {
        client().admin().indices().prepareClose("test").get();
        try {
            client().admin().indices().prepareSegments("test").get();
            fail("Expected IndexClosedException");
        } catch (IndexClosedException e) {
            assertThat(e.getMessage(), is("closed"));
        }
    }

    /**
     * setting the "ignoreUnavailable" option prevents IndexClosedException
     */
    public void testRequestOnClosedIndexIgnoreUnavailable() {
        client().admin().indices().prepareClose("test").get();
        IndicesOptions defaultOptions = new IndicesSegmentsRequest().indicesOptions();
        IndicesOptions testOptions = IndicesOptions.fromOptions(true, true, true, false, defaultOptions);
        IndicesSegmentResponse rsp = client().admin().indices().prepareSegments("test").setIndicesOptions(testOptions).get();
        assertEquals(0, rsp.getIndices().size());
    }

    /**
     * by default IndicesOptions setting IndicesSegmentsRequest should not throw exception when no index present
     */
    public void testAllowNoIndex() {
        client().admin().indices().prepareDelete("test").get();
        IndicesSegmentResponse rsp = client().admin().indices().prepareSegments().get();
        assertEquals(0, rsp.getIndices().size());
    }
}
