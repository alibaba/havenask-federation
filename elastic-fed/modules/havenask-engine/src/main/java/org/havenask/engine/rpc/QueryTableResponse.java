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

import suez.service.proto.DocValue;
import suez.service.proto.ErrorCode;

import java.util.List;

public class QueryTableResponse {
    List<DocValue> docValues;
    private final ErrorCode errorCode;
    private final String errorMessage;

    public QueryTableResponse(List<DocValue> docValues) {
        this.docValues = docValues;
        this.errorCode = null;
        this.errorMessage = "";
    }

    public QueryTableResponse(ErrorCode errorCode, String errorMessage) {
        this.docValues = null;
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
    }

    public List<DocValue> getDocValues() {
        return docValues;
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }

    public String getErrorMessage() {
        return errorMessage;
    }
}
