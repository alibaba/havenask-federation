package com.example.demo;

import com.alibaba.fastjson.JSONObject;
import com.aliyun.opensearch.OpenSearchClient;
import com.aliyun.opensearch.sdk.generated.OpenSearch;
import com.aliyun.opensearch.sdk.generated.commons.OpenSearchClientException;
import com.aliyun.opensearch.sdk.generated.commons.OpenSearchException;
import com.aliyun.opensearch.sdk.generated.commons.OpenSearchResult;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class LoadDoc {
    private static String appName;
    private static String host;
    private static String fedHost;
    private static String path;
    private static String accesskey;
    private static String secret;
    private static String sourceFileDir;

    // 仅将文本进行向量化后写入fed
    public static void main(String[] args) throws IOException {
        appName = EnvConfig.getInstance().getProperty("appName");
        host = EnvConfig.getInstance().getProperty("host");
        fedHost = EnvConfig.getInstance().getProperty("fedHost");
        path = EnvConfig.getInstance().getProperty("path");
        accesskey = EnvConfig.getInstance().getProperty("accesskey");
        secret = EnvConfig.getInstance().getProperty("secret");
        sourceFileDir = EnvConfig.getInstance().getProperty("sourceFileDir");

        //ApiReadTimeOut
        OpenSearch openSearch = new OpenSearch(accesskey, secret, host);
        openSearch.setTimeout(62000);

        OpenSearchClient openSearchClient = new OpenSearchClient(openSearch);

        // 创建RestClientBuilder实例, 通过builder创建rest client
        RestClientBuilder builder = RestClient.builder(
                new HttpHost(fedHost, 9200, "http")
        );
        RestHighLevelClient client = new RestHighLevelClient(builder);


        List<Path> allFiles = getAllFiles(sourceFileDir);
        for (Path file : allFiles) {
            System.out.println("File: " + file.getFileName());
            String content = Files.readString(file);
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("content", content);
            jsonObject.put("query", false);

            Map<String, String> params = new HashMap<String, String>() {{
                put("format", "full_json");
                put("_POST_BODY", jsonObject.toJSONString());
            }};
            try {
                // 调用embedding接口, 对文本进行向量化
                OpenSearchResult openSearchResult = openSearchClient
                        .callAndDecodeResult(path, params, "POST");

                // System.out.println("RequestID=" + openSearchResult.getTraceInfo().getRequestId());
                // System.out.println(openSearchResult.getResult());

                // em为文本向量化后的结果
                openSearchResult.getResult().split(",");
                List<Double> em =  Arrays.stream(Strings.splitStringByCommaToArray(openSearchResult.getResult())).map(Double::valueOf).collect(Collectors.toList());

                // 将文档的title、content以及向量化后的content_vector写入fed
                Map<String, Object> source = new HashMap<>();
                source.put("content", content);
                source.put("title", file.getFileName().toString());
                source.put("content_vector", em);
                IndexRequest indexRequest = new IndexRequest("elastic_doc", "_doc", file.getFileName().toString()).source(source);
                IndexResponse response = client.index(indexRequest, RequestOptions.DEFAULT);
                System.out.println(response);

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
        }

        client.close();
    }

    // 遍历文件夹,返回全部文件,如果有文件夹则递归
    static List<Path> getAllFiles(String dir) throws IOException {
        List<Path> paths = Files.walk(Paths.get(dir))
                .filter(Files::isRegularFile).collect(Collectors.toList());
        return paths.stream().filter(p -> p.toString().endsWith(".asciidoc")).collect(Collectors.toList());
    }
}
