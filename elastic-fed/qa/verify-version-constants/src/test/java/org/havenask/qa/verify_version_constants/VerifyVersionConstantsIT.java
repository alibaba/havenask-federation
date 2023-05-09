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

package org.havenask.qa.verify_version_constants;

import org.havenask.Version;
import org.havenask.client.Request;
import org.havenask.client.Response;
import org.havenask.test.rest.HavenaskRestTestCase;
import org.havenask.test.rest.yaml.ObjectPath;

import java.io.IOException;
import java.text.ParseException;

import static org.hamcrest.CoreMatchers.equalTo;

public class VerifyVersionConstantsIT extends HavenaskRestTestCase {

    public void testLuceneVersionConstant() throws IOException, ParseException {
        final Response response = client().performRequest(new Request("GET", "/"));
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        final ObjectPath objectPath = ObjectPath.createFromResponse(response);
        final String havenaskVersionString = objectPath.evaluate("version.number").toString();
        final Version havenaskVersion = Version.fromString(havenaskVersionString.replace("-SNAPSHOT", ""));
        final String luceneVersionString = objectPath.evaluate("version.lucene_version").toString();
        final org.apache.lucene.util.Version luceneVersion = org.apache.lucene.util.Version.parse(luceneVersionString);
        assertThat(havenaskVersion.luceneVersion, equalTo(luceneVersion));
    }

    @Override
    public boolean preserveClusterUponCompletion() {
        /*
         * We don't perform any writes to the cluster so there won't be anything
         * to clean up. Also, our cleanup code is really only compatible with
         * *write* compatible versions but this runs with *index* compatible
         * versions.
         */
        return true;
    }
}
