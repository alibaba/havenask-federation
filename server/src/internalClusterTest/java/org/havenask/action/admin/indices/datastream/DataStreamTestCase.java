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

package org.havenask.action.admin.indices.datastream;

import org.havenask.action.admin.indices.rollover.RolloverRequest;
import org.havenask.action.admin.indices.rollover.RolloverResponse;
import org.havenask.action.admin.indices.template.delete.DeleteComposableIndexTemplateAction;
import org.havenask.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.havenask.action.support.master.AcknowledgedResponse;
import org.havenask.cluster.metadata.ComposableIndexTemplate;
import org.havenask.cluster.metadata.DataStream;
import org.havenask.cluster.metadata.Template;
import org.havenask.common.bytes.BytesArray;
import org.havenask.common.settings.Settings;
import org.havenask.common.xcontent.XContentHelper;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.common.xcontent.XContentType;
import org.havenask.test.HavenaskIntegTestCase;

import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.is;
import static org.havenask.test.HavenaskIntegTestCase.ClusterScope;
import static org.havenask.test.HavenaskIntegTestCase.Scope;

@ClusterScope(scope = Scope.TEST, numDataNodes = 2)
public class DataStreamTestCase extends HavenaskIntegTestCase {

    public AcknowledgedResponse createDataStream(String name) throws Exception {
        CreateDataStreamAction.Request request = new CreateDataStreamAction.Request(name);
        AcknowledgedResponse response = client().admin().indices().createDataStream(request).get();
        assertThat(response.isAcknowledged(), is(true));
        return response;
    }

    public AcknowledgedResponse deleteDataStreams(String... names) throws Exception {
        DeleteDataStreamAction.Request request = new DeleteDataStreamAction.Request(names);
        AcknowledgedResponse response = client().admin().indices().deleteDataStream(request).get();
        assertThat(response.isAcknowledged(), is(true));
        return response;
    }

    public GetDataStreamAction.Response getDataStreams(String... names) throws Exception {
        GetDataStreamAction.Request request = new GetDataStreamAction.Request(names);
        return client().admin().indices().getDataStreams(request).get();
    }

    public List<String> getDataStreamsNames(String... names) throws Exception {
        return getDataStreams(names)
            .getDataStreams()
            .stream()
            .map(dsInfo -> dsInfo.getDataStream().getName())
            .collect(Collectors.toList());
    }

    public DataStreamsStatsAction.Response getDataStreamsStats(String... names) throws Exception {
        DataStreamsStatsAction.Request request = new DataStreamsStatsAction.Request();
        request.indices(names);
        return client().execute(DataStreamsStatsAction.INSTANCE, request).get();
    }

    public RolloverResponse rolloverDataStream(String name) throws Exception {
        RolloverRequest request = new RolloverRequest(name, null);
        RolloverResponse response = client().admin().indices().rolloverIndex(request).get();
        assertThat(response.isAcknowledged(), is(true));
        assertThat(response.isRolledOver(), is(true));
        return response;
    }

    public AcknowledgedResponse createDataStreamIndexTemplate(String name, List<String> indexPatterns) throws Exception {
        return createDataStreamIndexTemplate(name, indexPatterns, "@timestamp");
    }

    public AcknowledgedResponse createDataStreamIndexTemplate(String name,
                                                              List<String> indexPatterns,
                                                              String timestampFieldName) throws Exception {
        ComposableIndexTemplate template = new ComposableIndexTemplate(
            indexPatterns,
            new Template(
                Settings.builder().put("number_of_shards", 2).put("number_of_replicas", 1).build(),
                null,
                null
            ),
            null,
            null,
            null,
            null,
            new ComposableIndexTemplate.DataStreamTemplate(new DataStream.TimestampField(timestampFieldName))
        );

        return createIndexTemplate(name, template);
    }

    public AcknowledgedResponse createIndexTemplate(String name, String jsonContent) throws Exception {
        XContentParser parser = XContentHelper.createParser(
            xContentRegistry(),
            null,
            new BytesArray(jsonContent),
            XContentType.JSON
        );

        return createIndexTemplate(name, ComposableIndexTemplate.parse(parser));
    }

    private AcknowledgedResponse createIndexTemplate(String name, ComposableIndexTemplate template) throws Exception {
        PutComposableIndexTemplateAction.Request request = new PutComposableIndexTemplateAction.Request(name);
        request.indexTemplate(template);
        AcknowledgedResponse response = client().execute(PutComposableIndexTemplateAction.INSTANCE, request).get();
        assertThat(response.isAcknowledged(), is(true));
        return response;
    }

    public AcknowledgedResponse deleteIndexTemplate(String name) throws Exception {
        DeleteComposableIndexTemplateAction.Request request = new DeleteComposableIndexTemplateAction.Request(name);
        AcknowledgedResponse response = client().execute(DeleteComposableIndexTemplateAction.INSTANCE, request).get();
        assertThat(response.isAcknowledged(), is(true));
        return response;
    }

}
