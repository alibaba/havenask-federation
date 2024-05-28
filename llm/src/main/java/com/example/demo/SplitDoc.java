package com.example.demo;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
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
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SplitDoc {
    private static String appName;
    private static String host;
    private static String fedHost;
    private static String splitPath;
    private static String accesskey;
    private static String secret;
    private static String sourceFileDir;

    // 将文本进行切分和向量化后写入fed
    public static void main(String[] args) throws IOException {
        appName = EnvConfig.getInstance().getProperty("appName");
        host = EnvConfig.getInstance().getProperty("host");
        fedHost = EnvConfig.getInstance().getProperty("fedHost");
        splitPath = EnvConfig.getInstance().getProperty("splitPath");
        accesskey = EnvConfig.getInstance().getProperty("accesskey");
        secret = EnvConfig.getInstance().getProperty("secret");
        sourceFileDir = EnvConfig.getInstance().getProperty("sourceFileDir");

        //ApiReadTimeOut
        OpenSearch openSearch = new OpenSearch(accesskey, secret, host);
        openSearch.setTimeout(62000);

        OpenSearchClient openSearchClient = new OpenSearchClient(openSearch);

        // 创建RestClientBuilder实例, 通过builder创建rest client
        RestClientBuilder builder = RestClient.builder(
                new HttpHost(fedHost, 9200, "http") //本地访问实例，若有远程集群则填远程集群的IP地址即可。
        );
        RestHighLevelClient client = new RestHighLevelClient(builder);

        List<Path> allFiles = getAllFiles(sourceFileDir);
        for (Path file : allFiles) {
            System.out.println("File: " + file.getFileName());
            String content = Files.readString(file);
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("content", content);
            jsonObject.put("use_embedding", true);

            Map<String, String> params = new HashMap<String, String>() {{
                put("format", "full_json");
                put("_POST_BODY", jsonObject.toJSONString());
            }};
            try {
                // 调用split接口, 对文本进行切分和向量化
                OpenSearchResult openSearchResult = openSearchClient
                        .callAndDecodeResult(splitPath, params, "POST");

                String result = openSearchResult.getResult();
                JSONArray resultJsonArray = JSON.parseArray(result);
                for (Object res : resultJsonArray) {
                    ChunkContext chunkContext = ChunkContext.parse(res.toString());

                    String id = file.getFileName().toString() + "_chunck_id:" + chunkContext.getChunk_id();

                    // 将切分后的title、content以及content_vector写入fed
                    Map<String, Object> source = new HashMap<>();
                    source.put("content", chunkContext.getChunk());
                    source.put("title", id);
                    source.put("content_vector", chunkContext.getEm());
                    IndexRequest indexRequest = new IndexRequest("elastic_doc", "_doc", id).source(source);
                    IndexResponse response = client.index(indexRequest, RequestOptions.DEFAULT);
                    System.out.println(response);
                }
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

class ChunkContext {
    private String chunk;
    private List<Double> em;
    private String type;
    private String chunk_id;

    public String getChunk() {
        return chunk;
    }

    public List<Double> getEm() {
        return em;
    }

    public String getType() {
        return type;
    }

    public String getChunk_id() {
        return chunk_id;
    }

    public ChunkContext(String chunk, List<Double> em, String type, String chunk_id) {
        this.chunk = chunk;
        this.em = em;
        this.type = type;
        this.chunk_id = chunk_id;
    }

    public static ChunkContext fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token;
        String chunk = null;
        List<Double> em = null;
        String type = null;
        String chunk_id = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                String fieldName = parser.currentName();
                parser.nextToken();
                switch (fieldName) {
                    case "chunk":
                        chunk = parser.text();
                        break;
                    case "embedding":
                        String embedding = parser.text();
                        em = Arrays.stream(Strings.splitStringByCommaToArray(embedding)).map(Double::valueOf).collect(Collectors.toList());
                        break;
                    case "type":
                        type = parser.text();
                        break;
                    case "chunk_id":
                        chunk_id = parser.text();
                        break;
                    default:
                        // do nothing
                        // throw new IllegalArgumentException("Unknown field: [" + fieldName + "]");
                }
            }
        }
        return new ChunkContext(chunk, em, type, chunk_id);
    }

    public static ChunkContext parse(String chunkStr) throws IOException{
        XContentParser parser = XContentType.JSON.xContent()
                .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.IGNORE_DEPRECATIONS, chunkStr);
        return ChunkContext.fromXContent(parser);
    }
}
