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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.havenask.common;

import org.havenask.HavenaskException;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.XContentLocation;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.rest.RestStatus;

import java.io.IOException;

/**
 * Exception that can be used when parsing queries with a given {@link
 * XContentParser}.
 * Can contain information about location of the error.
 */
public class ParsingException extends HavenaskException {

    protected static final int UNKNOWN_POSITION = -1;
    private final int lineNumber;
    private final int columnNumber;

    public ParsingException(XContentLocation contentLocation, String msg, Object... args) {
        this(contentLocation, msg, null, args);
    }

    public ParsingException(XContentLocation contentLocation, String msg, Throwable cause, Object... args) {
        super(msg, cause, args);
        int lineNumber = UNKNOWN_POSITION;
        int columnNumber = UNKNOWN_POSITION;
        if (contentLocation != null) {
            lineNumber = contentLocation.lineNumber;
            columnNumber = contentLocation.columnNumber;
        }
        this.columnNumber = columnNumber;
        this.lineNumber = lineNumber;
    }

    /**
     * This constructor is provided for use in unit tests where a
     * {@link XContentParser} may not be available
     */
    public ParsingException(int line, int col, String msg, Throwable cause) {
        super(msg, cause);
        this.lineNumber = line;
        this.columnNumber = col;
    }

    public ParsingException(StreamInput in) throws IOException{
        super(in);
        lineNumber = in.readInt();
        columnNumber = in.readInt();
    }

    /**
     * Line number of the location of the error
     *
     * @return the line number or -1 if unknown
     */
    public int getLineNumber() {
        return lineNumber;
    }

    /**
     * Column number of the location of the error
     *
     * @return the column number or -1 if unknown
     */
    public int getColumnNumber() {
        return columnNumber;
    }

    @Override
    public RestStatus status() {
        return RestStatus.BAD_REQUEST;
    }

    @Override
    protected void metadataToXContent(XContentBuilder builder, Params params) throws IOException {
        if (lineNumber != UNKNOWN_POSITION) {
            builder.field("line", lineNumber);
            builder.field("col", columnNumber);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeInt(lineNumber);
        out.writeInt(columnNumber);
    }
}
