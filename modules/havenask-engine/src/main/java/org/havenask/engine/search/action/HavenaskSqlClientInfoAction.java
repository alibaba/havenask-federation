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
import java.util.Objects;

import org.havenask.action.ActionRequest;
import org.havenask.action.ActionRequestValidationException;
import org.havenask.action.ActionResponse;
import org.havenask.action.ActionType;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;
import org.havenask.common.xcontent.ToXContentObject;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.engine.search.action.HavenaskSqlClientInfoAction.Response;

public class HavenaskSqlClientInfoAction extends ActionType<Response> {

    public static final HavenaskSqlClientInfoAction INSTANCE = new HavenaskSqlClientInfoAction();
    public static final String NAME = "indices:monitor/havenask/sql/clientinfo";

    private HavenaskSqlClientInfoAction() {
        super(NAME, Response::new);
    }

    public static class Request extends ActionRequest {

        public Request() {}

        public Request(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {
        private final String errorMessage;
        private final int errorCode;
        private final String result;

        public Response(String errorMessage, int errorCode) {
            this.errorMessage = errorMessage;
            this.errorCode = errorCode;
            this.result = null;
        }

        public Response(String result) {
            this.result = result;
            this.errorCode = 0;
            this.errorMessage = "";
        }

        public String getErrorMessage() {
            return errorMessage;
        }

        public int getErrorCode() {
            return errorCode;
        }

        public String getResult() {
            return result;
        }

        public Response(StreamInput in) throws IOException {
            errorMessage = in.readOptionalString();
            errorCode = in.readInt();
            result = in.readOptionalString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(errorMessage);
            out.writeInt(errorCode);
            out.writeOptionalString(result);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (Objects.nonNull(errorMessage)) {
                builder.field("error_message", errorMessage);
            }
            builder.field("error_code", errorCode);
            if (Objects.nonNull(result)) {
                builder.field("result", result);
            }
            builder.endObject();
            return builder;
        }
    }
}
