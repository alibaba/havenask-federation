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

package org.havenask.engine.search.action;

import java.io.IOException;

import org.havenask.action.ActionRequest;
import org.havenask.action.ActionRequestValidationException;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;

import static org.havenask.action.ValidateActions.addValidationError;

public class HavenaskSqlRequest extends ActionRequest {

    private final String sql;
    private final String kvpair;

    public HavenaskSqlRequest(String sql, String kvpair) {
        this.sql = sql;
        this.kvpair = kvpair;
    }

    public HavenaskSqlRequest(StreamInput in) throws IOException {
        super(in);
        sql = in.readString();
        kvpair = in.readOptionalString();
    }

    public String getSql() {
        return sql;
    }

    public String getKvpair() {
        return kvpair;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        // sql不能为null
        if (sql == null) {
            validationException = addValidationError("sql is null", validationException);
        }
        return validationException;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(sql);
        out.writeOptionalString(kvpair);
    }

    @Override
    public String toString() {
        return "HavenaskSqlRequest{" + "sql='" + sql + '\'' + ", kvpair='" + kvpair + '\'' + '}';
    }

}
