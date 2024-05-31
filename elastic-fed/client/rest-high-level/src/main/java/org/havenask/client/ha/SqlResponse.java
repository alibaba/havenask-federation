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
import java.util.ArrayList;
import java.util.List;

import org.havenask.common.xcontent.DeprecationHandler;
import org.havenask.common.xcontent.NamedXContentRegistry;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.common.xcontent.XContentType;

public class SqlResponse {
    private final double totalTime;
    private final boolean hasSoftFailure;
    private final double coveredPercent;
    private final int rowCount;
    private final SqlResult sqlResult;
    private final ErrorInfo errorInfo;

    public static class SqlResult {
        private final Object[][] data;
        private final String[] columnName;
        private final String[] columnType;

        public SqlResult(Object[][] data, String[] columnName, String[] columnType) {
            this.data = data;
            this.columnName = columnName;
            this.columnType = columnType;
        }

        public Object[][] getData() {
            return data;
        }

        public String[] getColumnName() {
            return columnName;
        }

        public String[] getColumnType() {
            return columnType;
        }
    }

    public static class ErrorInfo {
        private final int errorCode;
        private final String error;
        private final String message;

        public ErrorInfo(int errorCode, String error, String message) {
            this.errorCode = errorCode;
            this.error = error;
            this.message = message;
        }

        public int getErrorCode() {
            return errorCode;
        }
        public String getError() {
            return error;
        }
        public String getMessage() {
            return message;
        }
    }

    public SqlResponse(
            double totalTime,
            boolean hasSoftFailure,
            double coveredPercent,
            int rowCount,
            SqlResult sqlResult, 
            ErrorInfo errorInfo) {
        this.totalTime = totalTime;
        this.hasSoftFailure = hasSoftFailure;
        this.coveredPercent = coveredPercent;
        this.rowCount = rowCount;
        this.sqlResult = sqlResult;
        this.errorInfo = errorInfo;
    }

    public double getTotalTime() {
        return totalTime;
    }

    public boolean isHasSoftFailure() {
        return hasSoftFailure;
    }

    public double getCoveredPercent() {
        return coveredPercent;
    }

    public int getRowCount() {
        return rowCount;
    }

    public SqlResult getSqlResult() {
        return sqlResult;
    }

    public ErrorInfo getErrorInfo() {
        return errorInfo;
    }

    public static SqlResponse fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token;
        double totalTime = 0;
        boolean hasSoftFailure = false;
        double coveredPercent = 0;
        int rowCount = 0;
        SqlResult sqlResult = null;
        ErrorInfo errorInfo = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                String fieldName = parser.currentName();
                parser.nextToken();
                switch (fieldName) {
                    case "total_time":
                        totalTime = parser.doubleValue();
                        break;
                    case "has_soft_failure":
                        hasSoftFailure = parser.booleanValue();
                        break;
                    case "covered_percent":
                        coveredPercent = parser.doubleValue();
                        break;
                    case "row_count":
                        rowCount = parser.intValue();
                        break;
                    case "sql_result":
                        Object[][] data = null;
                        String[] columnName = null;
                        String[] columnType = null;
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            if (token == XContentParser.Token.FIELD_NAME) {
                                String sqlResultFieldName = parser.currentName();
                                parser.nextToken();
                                switch (sqlResultFieldName) {
                                    case "data":
                                        List<Object[]> dataList = new ArrayList<>();
                                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                                            if (token == XContentParser.Token.START_ARRAY) {
                                                List<Object> row = new ArrayList<>();
                                                while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                                                    switch (token) {
                                                        case VALUE_STRING:
                                                            row.add(parser.text());
                                                            break;
                                                        case VALUE_NUMBER:
                                                            row.add(parser.numberValue());
                                                            break;
                                                        case VALUE_BOOLEAN:
                                                            row.add(parser.booleanValue());
                                                            break;
                                                        case VALUE_NULL:
                                                            row.add(null);
                                                            break;
                                                        default:
                                                            break;
                                                    }
                                                }
                                                dataList.add(row.toArray());
                                            }
                                        }
                                        data = dataList.toArray(new Object[0][]);
                                        break;
                                    case "column_name":
                                        columnName = parser.list().toArray(new String[0]);
                                        break;
                                    case "column_type":
                                        columnType = parser.list().toArray(new String[0]);
                                        break;
                                    default:
                                        break;
                                }
                            }
                        }
                        sqlResult = new SqlResult(data, columnName, columnType);
                        break;
                    case "error_info":
                        int errorCode = 0;
                        String error = "";
                        String message = "";
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            if (token == XContentParser.Token.FIELD_NAME) {
                                String errorInfoFieldName = parser.currentName();
                                parser.nextToken();
                                switch (errorInfoFieldName) {
                                    case "ErrorCode":
                                        errorCode = parser.intValue();
                                        break;
                                    case "Error":
                                        error = parser.text();
                                        break;
                                    case "Message":
                                        message = parser.text();
                                        break;
                                    default:
                                        break;
                                }
                            }
                        }
                        errorInfo = new ErrorInfo(errorCode, error, message);
                        break;
                    default:
                        parser.skipChildren();
                        break;
                }
            }
        }
        return new SqlResponse(totalTime, hasSoftFailure, coveredPercent, rowCount, sqlResult, errorInfo);
    }

    public static SqlResponse parse(String strResponse) throws IOException {
        XContentParser parser = XContentType.JSON.xContent()
            .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.IGNORE_DEPRECATIONS, strResponse);
        return SqlResponse.fromXContent(parser);
    }
}
