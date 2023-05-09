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

package org.havenask.backwards;

import org.apache.http.util.EntityUtils;
import org.havenask.Version;
import org.havenask.client.Node;
import org.havenask.client.Request;
import org.havenask.client.Response;
import org.havenask.client.ResponseException;
import org.havenask.test.rest.HavenaskRestTestCase;
import org.havenask.test.rest.yaml.ObjectPath;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.apache.http.HttpStatus.SC_NOT_FOUND;

public class ExceptionIT extends HavenaskRestTestCase {
    public void testHavenaskException() throws Exception {
        logClusterNodes();

        Request request = new Request("GET", "/no_such_index");

        for (Node node : client().getNodes()) {
            try {
                client().setNodes(Collections.singletonList(node));
                logger.info("node: {}", node.getHost());
                client().performRequest(request);
                fail();
            } catch (ResponseException e) {
                logger.debug(e.getMessage());
                Response response = e.getResponse();
                assertEquals(SC_NOT_FOUND, response.getStatusLine().getStatusCode());
                assertEquals("no_such_index", ObjectPath.createFromResponse(response).evaluate("error.index"));
            }
        }
    }

    private void logClusterNodes() throws IOException {
        ObjectPath objectPath = ObjectPath.createFromResponse(client().performRequest(new Request("GET", "_nodes")));
        Map<String, ?> nodes = objectPath.evaluate("nodes");
        String master = EntityUtils.toString(client().performRequest(new Request("GET", "_cat/master?h=id")).getEntity()).trim();
        logger.info("cluster discovered: master id='{}'", master);
        for (String id : nodes.keySet()) {
            logger.info("{}: id='{}', name='{}', version={}",
                objectPath.evaluate("nodes." + id + ".http.publish_address"),
                id,
                objectPath.evaluate("nodes." + id + ".name"),
                Version.fromString(objectPath.evaluate("nodes." + id + ".version"))
            );
        }
    }
}
