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

package org.havenask.engine;

import java.io.IOException;
import java.util.Collections;

import org.havenask.client.RestClient;
import org.havenask.client.RestHighLevelClient;
import org.havenask.common.settings.Settings;
import org.havenask.core.internal.io.IOUtils;
import org.havenask.search.SearchModule;
import org.havenask.test.rest.HavenaskRestTestCase;
import org.junit.AfterClass;
import org.junit.Before;

public abstract class AbstractHavenaskRestTestCase extends HavenaskRestTestCase {
    private static RestHighLevelClient restHighLevelClient;

    @Before
    public void initHighLevelClient() throws IOException {
        super.initClient();
        if (restHighLevelClient == null) {
            restHighLevelClient = new HighLevelClient(client());
        }
    }

    @AfterClass
    public static void cleanupClient() throws IOException {
        IOUtils.close(restHighLevelClient);
        restHighLevelClient = null;
    }

    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    public static RestHighLevelClient highLevelClient() {
        return restHighLevelClient;
    }

    private static class HighLevelClient extends RestHighLevelClient {
        private HighLevelClient(RestClient restClient) {
            super(restClient, (client) -> {}, new SearchModule(Settings.EMPTY, false, Collections.emptyList()).getNamedXContents());
        }
    }
}
