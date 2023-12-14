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

public class QueryTableResponse extends ArpcResponse {
    List<DocValue> docValues;

    public QueryTableResponse(List<DocValue> docValues) {
        super(null, "");
        this.docValues = docValues;
    }

    public QueryTableResponse(ErrorCode errorCode, String errorMessage) {
        super(errorCode, errorMessage);
        this.docValues = null;
    }

    public List<DocValue> getDocValues() {
        return docValues;
    }
}
