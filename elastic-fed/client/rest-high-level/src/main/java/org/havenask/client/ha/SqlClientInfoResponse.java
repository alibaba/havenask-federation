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

package org.havenask.client.ha;

import java.io.IOException;
import java.util.Map;

import org.havenask.common.xcontent.XContentParser;

public class SqlClientInfoResponse {
    private final String errorMessage;
    private final int errorCode;
    private final Map<String, Object> result;

    public SqlClientInfoResponse(String errorMessage, int errorCode, Map<String, Object> result) {
        this.errorMessage = errorMessage;
        this.errorCode = errorCode;
        this.result = result;
    }

    public static SqlClientInfoResponse fromXContent(XContentParser parser) throws IOException {
        String errorMessage = null;
        int errorCode = 0;
        Map<String, Object> result = null;
        XContentParser.Token token = parser.nextToken();
        if (token == XContentParser.Token.START_OBJECT) {
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    String currentFieldName = parser.currentName();
                    if ("error_message".equals(currentFieldName)) {
                        token = parser.nextToken();
                        errorMessage = parser.text();
                    } else if ("error_code".equals(currentFieldName)) {
                        token = parser.nextToken();
                        errorCode = parser.intValue();
                    } else if ("result".equals(currentFieldName)) {
                        token = parser.nextToken();
                        result = parser.map();
                    }
                }
            }
        }
        return new SqlClientInfoResponse(errorMessage, errorCode, result);
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public int getErrorCode() {
        return errorCode;
    }

    public Map<String, Object> getResult() {
        return result;
    }
}
