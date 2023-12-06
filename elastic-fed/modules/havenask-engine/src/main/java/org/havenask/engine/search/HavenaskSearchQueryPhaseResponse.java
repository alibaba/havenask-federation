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

package org.havenask.engine.search;

import org.havenask.client.ha.SqlResponse;
import org.havenask.engine.rpc.QrsSqlResponse;

public class HavenaskSearchQueryPhaseResponse {
    private static final String COVERED_PERCENT = "\"covered_percent\":";
    private static final Double DEFAULT_COVERED_PERCENT = 1.0;
    private QrsSqlResponse queryPhaseQrsSqlResponse;
    private SqlResponse queryPhaseSqlResponse;

    public HavenaskSearchQueryPhaseResponse(QrsSqlResponse queryPhaseQrsSqlResponse, SqlResponse queryPhaseSqlResponse) {
        this.queryPhaseQrsSqlResponse = queryPhaseQrsSqlResponse;
        this.queryPhaseSqlResponse = queryPhaseSqlResponse;
    }

    public QrsSqlResponse getQueryPhaseQrsSqlResponse() {
        return queryPhaseQrsSqlResponse;
    }

    public SqlResponse getQueryPhaseSqlResponse() {
        return queryPhaseSqlResponse;
    }

    public double getCoveredPercent() {
        if (queryPhaseQrsSqlResponse != null) {
            String result = queryPhaseQrsSqlResponse.getResult();
            int offset = result.indexOf(COVERED_PERCENT) + COVERED_PERCENT.length();
            int end = result.indexOf(",", offset);
            return Double.parseDouble(result.substring(offset, end));
        }
        return DEFAULT_COVERED_PERCENT;
    }
}
