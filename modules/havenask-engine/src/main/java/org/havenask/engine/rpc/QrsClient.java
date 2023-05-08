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

package org.havenask.engine.rpc;

import java.io.IOException;

public interface QrsClient extends HavenaskClient {

    /**
     * Execute havenask sql
     *
     * @param request havenask sql request
     * @return havenask sql response
     * @throws IOException TODO
     */
    QrsSqlResponse executeSql(QrsSqlRequest request) throws IOException;

    /**
     * Execute havenask sql client info api
     *
     * @return havenask sql client info response
     * @throws IOException TODO
     */
    String executeSqlClientInfo() throws IOException;
}
