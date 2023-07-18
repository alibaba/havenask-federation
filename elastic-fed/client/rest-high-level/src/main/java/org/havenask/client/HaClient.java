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

package org.havenask.client;

import java.io.IOException;

import org.havenask.client.ha.SqlClientInfoRequest;
import org.havenask.client.ha.SqlClientInfoResponse;
import org.havenask.client.ha.SqlRequest;
import org.havenask.client.ha.SqlResponse;

import static java.util.Collections.emptySet;

public class HaClient {
    private final RestHighLevelClient restHighLevelClient;

    HaClient(RestHighLevelClient restHighLevelClient) {
        this.restHighLevelClient = restHighLevelClient;
    }

    public SqlResponse sql(SqlRequest sqlRequest, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(sqlRequest, HaRequestConverters::sql, options,
            SqlResponse::fromXContent, emptySet());
    }

    public SqlClientInfoResponse sqlClientInfo(SqlClientInfoRequest sqlClientInfo, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(sqlClientInfo, HaRequestConverters::sqlClientInfo, options,
            SqlClientInfoResponse::fromXContent, emptySet());
    }
}
