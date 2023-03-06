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

import org.havenask.common.collect.List;

import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.containsString;

public class DataStreamIndexTemplateIT extends DataStreamTestCase {

    public void testCreateDataStreamIndexTemplate() throws Exception {
        // Without the data stream metadata field mapper, data_stream would have been an unknown field in
        // the index template and would have thrown an error.
        createIndexTemplate(
            "demo-template",
            "{" +
                "\"index_patterns\": [ \"logs-*\" ]," +
                "\"data_stream\": { }" +
            "}"
        );

        // Data stream index template with a custom timestamp field name.
        createIndexTemplate(
            "demo-template",
            "{" +
                "\"index_patterns\": [ \"logs-*\" ]," +
                "\"data_stream\": {" +
                    "\"timestamp_field\": { \"name\": \"created_at\" }" +
                "}" +
            "}"
        );
    }

    public void testDeleteIndexTemplate() throws Exception {
        createDataStreamIndexTemplate("demo-template", List.of("logs-*"));
        createDataStream("logs-demo");

        // Index template deletion should fail if there is a data stream using it.
        ExecutionException exception = expectThrows(ExecutionException.class, () -> deleteIndexTemplate("demo-template"));
        assertThat(
            exception.getMessage(),
            containsString("unable to remove composable templates [demo-template] as they are in use by a data streams")
        );

        // Index template can be deleted when all matching data streams are also deleted first.
        deleteDataStreams("logs-demo");
        deleteIndexTemplate("demo-template");
    }

}
