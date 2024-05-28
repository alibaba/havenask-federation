package com.example.demo;

import com.alibaba.dashscope.aigc.generation.Generation;
import com.alibaba.dashscope.aigc.generation.GenerationResult;
import com.alibaba.dashscope.aigc.generation.models.QwenParam;
import com.alibaba.dashscope.exception.ApiException;
import com.alibaba.dashscope.exception.InputRequiredException;
import com.alibaba.dashscope.exception.NoApiKeyException;
import com.alibaba.dashscope.utils.JsonUtils;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.aliyun.opensearch.OpenSearchClient;
import com.aliyun.opensearch.sdk.generated.OpenSearch;
import com.aliyun.opensearch.sdk.generated.commons.OpenSearchClientException;
import com.aliyun.opensearch.sdk.generated.commons.OpenSearchException;
import com.aliyun.opensearch.sdk.generated.commons.OpenSearchResult;
import jakarta.annotation.PreDestroy;
import org.apache.http.HttpHost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Service
public class ChatService {
    private static String appName;
    private static String host;
    private static String fedHost;
    private static String path;
    private static String llmPath;
    private static String accesskey;
    private static String secret;

    private OpenSearchClient openSearchClient;
    private RestHighLevelClient client;

    public ChatService() throws IOException {
        appName = EnvConfig.getInstance().getProperty("appName");
        host = EnvConfig.getInstance().getProperty("host");
        fedHost = EnvConfig.getInstance().getProperty("fedHost");
        path = EnvConfig.getInstance().getProperty("path");
        llmPath = EnvConfig.getInstance().getProperty("llmPath");
        accesskey = EnvConfig.getInstance().getProperty("accesskey");
        secret = EnvConfig.getInstance().getProperty("secret");
        //ApiReadTimeOut
        OpenSearch openSearch = new OpenSearch(accesskey, secret, host);
        openSearch.setTimeout(62000);

        openSearchClient = new OpenSearchClient(openSearch);

        // 创建RestClientBuilder实例, 通过builder创建rest client
        RestClientBuilder builder = RestClient.builder(
                new HttpHost(fedHost, 9200, "http")
        );
        client = new RestHighLevelClient(builder);
    }

    public String getAnswer(String question) throws IOException {
        Map<String, String> params = new HashMap<String, String>() {{
            put("format", "full_json");
            put("_POST_BODY", "{\"content\":\""+ question +"\",\"query\":false}");
        }};
        try {
            OpenSearchResult openSearchResult = openSearchClient
                    .callAndDecodeResult(path, params, "POST");
            // System.out.println("RequestID=" + openSearchResult.getTraceInfo().getRequestId());
            // System.out.println(openSearchResult.getResult());

            String sqlSearchStr = "select _id, (1/(1+vector_score('content_vector'))) as _score from `elastic_doc` where MATCHINDEX('content_vector', '"+ openSearchResult.getResult() +"&n=10')" +
                    " AND MATCHINDEX('content', '"+question+"', 'default_op:OR') order by _score desc limit 5 offset 0";
            SqlResponse sqlResponse = searchWithSql(client, sqlSearchStr);
            List<Object> ids = new ArrayList<>();
            for (Object[] row : sqlResponse.getSqlResult().getData()) {
                ids.add(row[0]);
            }

            // ids to string, e.g: 'a', 'b', 'c'
            StringBuilder idsStr = new StringBuilder();
            for (Object id : ids) {
                idsStr.append("'").append(id).append("',");
            }
            idsStr.deleteCharAt(idsStr.length() - 1);

            String getSourceSql = "select _source from `elastic_doc_summary_` where _id in (" + idsStr.toString()  + ")";
            SqlResponse sourceSqlResponse = searchWithSql(client, getSourceSql);
            StringBuilder stringBuilder = new StringBuilder();
            int pos = 0;
            for (Object[] row : sourceSqlResponse.getSqlResult().getData()) {
                String source = row[0].toString();
                JSONObject json = JSON.parseObject(source);
                stringBuilder.append("content" + pos++ + "：" + json.getString("content") + "\n");
            }

            String result = llmSearch(openSearchClient, stringBuilder.toString(), question);
            if (Objects.isNull(result)) {
                return "not found or error";
            }
            return result;
        } catch (
                OpenSearchException e) {
            System.out.println("RequestID=" + e.getRequestId());
            System.out.println("ErrorCode=" + e.getCode());
            System.out.println("ErrorMessage=" + e.getMessage());
        } catch (OpenSearchClientException e) {
            System.out.println("ErrorMessage=" + e.getMessage());
        } catch (Throwable e) {
            e.printStackTrace();
        }
        return null;
    }

    @PreDestroy
    public void cleanUp() throws IOException {
        client.close();
    }

    public static String llmSearch(OpenSearchClient openSearchClient, String content, String question) {
        if (content.length() > 2048) {
            content = content.substring(0, 2048);
        }
        String prompt = "\n\nHuman:假设你是Elasticsearch专家, 请根据下面知识回答问题：\n" +
                "知识：" + content + "\n" +
                "问题：" + question + "\n\nAssistant:";

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("prompt", prompt);
        // jsonObject.put("model", "opensearch-wen / opensearch-llama2-13b,70b  qwen-turbo, ");
        jsonObject.put("model", "opensearch-qwen");
        jsonObject.put("options", new JSONObject() {{
            put("stream", false);
        }});
        jsonObject.put("csi_level", "none");

        Map<String, String> params = new HashMap<String, String>() {{
            put("format", "full_json");
            put("_POST_BODY", jsonObject.toJSONString());
        }};
        try {
            OpenSearchResult openSearchResult = openSearchClient
                    .callAndDecodeResult(llmPath, params, "POST");
//            System.out.println("RequestID=" + openSearchResult.getTraceInfo().getRequestId());
//            System.out.println(openSearchResult.getResult());

            StringBuilder sb = new StringBuilder();
            sb.append("RequestID=" + openSearchResult.getTraceInfo().getRequestId());

            JSONObject resultJson = JSON.parseObject(openSearchResult.getResult());
            JSONArray dataJson = resultJson.getJSONArray("data");
            for (int i = 0; i < dataJson.size(); i++) {
                JSONObject data = dataJson.getJSONObject(i);
                if (data.containsKey("answer")) {
                    sb.append("\nanswer:" + data.getString("answer"));
                }
            }

            return sb.toString();
        } catch (
                OpenSearchException e) {
            System.out.println("RequestID=" + e.getRequestId());
            System.out.println("ErrorCode=" + e.getCode());
            System.out.println("ErrorMessage=" + e.getMessage());
        } catch (OpenSearchClientException e) {
            System.out.println("ErrorMessage=" + e.getMessage());
        } catch (Throwable e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void qwenQuickStart(String content, String question)
            throws NoApiKeyException, ApiException, InputRequiredException {
        String prompt = "假设你是Elasticsearch专家, 请根据下面知识回答问题：\n" +
                "知识：" + content + "\n" +
                "问题：" + question + "\n";
        Generation gen = new Generation();
        QwenParam param = QwenParam.builder()
                .apiKey("QWEN_API_KEY")
                .model("qwen-max-longcontext").prompt(prompt)
                .topP(0.8).build();
        GenerationResult result = gen.call(param);
        System.out.println(JsonUtils.toJson(result));
    }

    public static SqlResponse searchWithSql(RestHighLevelClient client, String sqlSearchStr) throws IOException {
        Request sqlSearchRequest = new Request("GET", "/_havenask/sql");
        sqlSearchRequest.addParameter("query", sqlSearchStr);
        sqlSearchRequest.addParameter("format", "full_json");

        Response SearchResponse = client.getLowLevelClient().performRequest(sqlSearchRequest);

        SqlResponse sqlResponse;
        try (XContentParser parser = XContentType.JSON.xContent().createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.IGNORE_DEPRECATIONS, SearchResponse.getEntity().getContent())) {
            sqlResponse = SqlResponse.fromXContent(parser);
        }
        return sqlResponse;
    }
}

class SqlResponse {
    private final double totalTime;
    private final boolean hasSoftFailure;
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
    }

    public SqlResponse(double totalTime, boolean hasSoftFailure, int rowCount, SqlResult sqlResult, ErrorInfo errorInfo) {
        this.totalTime = totalTime;
        this.hasSoftFailure = hasSoftFailure;
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
        return new SqlResponse(totalTime, hasSoftFailure, rowCount, sqlResult, errorInfo);
    }
}




