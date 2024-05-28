# 开始
havenask-federation可以与大模型结合，使用fed作为向量数据库，将相关文档构建成prompt送到LLM如OpenSearch-LLM智能问答版、ChatGLM/OpenAI中，实现基于文档的问答系统。

流程图如下：

![image](https://github.com/Huaixinww/havenask-federation/assets/141887897/324dd9ba-f120-4bfd-af8f-6cb3f9b451ed)

这里对流程做一些简单介绍：

*   对文本切分、向量化并写入fed：
    
    *   这部分通过调用OpenSearch的SplitDoc接口，将目标文档切分、向量化后写入fed，作为后续检索的向量数据库。
        
*   根据用户的问题，从fed中查询关联度最高的top n个文档：
    
    *   对于用户的query，调用OpenSearch的输入内容向量化接口，将query向量化；
        
    *   根据query以及query的向量查询fed，返回与query以及query向量关联度最高的top n个content文本；
        

*   构造promt，调用OpenSearch大模型接口：
    
    *   根据query以及查询到的content构造prompt，调用OpenSearch大模型对话接口，返回answer。
        

## 文本分割与向量化

由于LLM存在输入token的限制，且输入信息过多过杂也会影响LLM的回答效果，因此文本分割对基于文档的问答系统的效果有较大的影响。文本分割需要尽可能地考虑语义，并将相关文本聚合在一起，好的文本分割可以提高检索效果，有助于LLM更好地“阅读理解”知识。

将文本分割后，需要对文本进行向量化操作，文本向量化的主要目的是将文本表示成一系列能够表达文本语义的向量，文本向量化后可以捕获单词和短语的语义信息，单词和短语被转换成向量，这些向量在多维空间中的距离可以表示它们之间的语义相似度。

OpenSearch-LLM智能问答版提供了切分文本以及向量化文本的SDK：[文本向量化及切片向量化](https://help.aliyun.com/zh/open-search/llm-intelligent-q-a-version/text-vectorization-and-slice-vectorization?spm=a2c4g.11186623.0.0.47b153bfHcsoR5)；

# 使用示例

本文以OpenSearch-LLM智能问答版为例，将fed作为向量数据库实现基于本地知识的问答系统。

fed负责：

*   存储用户原始文档数据、向量数据；
    
*   召回用户原始文档数据、向量数据；
    

OpenSearch-LLM智能问答版负责：

*   对用户原始文档进行切片和向量化；
    
*   对用户原始query进行向量化；
    
*   对召回结果进行推理总结；
    

## 准备fed实例

*   创建fed实例，详细可参考：[快速开始](https://github.com/alibaba/havenask-federation/wiki/%E5%BF%AB%E9%80%9F%E5%BC%80%E5%A7%8B)
    
*   创建索引：  
    我们需要在fed实例上创建与写入的文档相对应的索引，demo的索引共三个字段，分别是title、content、content\_vector。  
    title字段存储doc的标题，content字段存储文档的原文内容，content\_vector字段存储content字段向量化的结果。
    
```
    PUT elastic_doc
    {
      "settings": {
        "index.engine":"havenask",
        "number_of_replicas": 0
      },
      "mappings": {
        "properties": {
          "title":{
            "type":"keyword"
          },
          "content":{
            "type":"text",
            "analyzer": "jieba_analyzer"
          },
          "content_vector":{
            "type":"vector",
            "dims":1536,
            "similarity": "dot_product"
          }
        }
      }
    }
```

## 准备OpenSearch-LLM智能问答版实例

*   购买与创建OpenSearch-LLM智能问答版可以参考：[购买智能问答版实例](https://help.aliyun.com/zh/open-search/llm-intelligent-q-a-version/buy-smart-q-a-instance?spm=a2c4g.11186623.0.preDoc.7e582644fUNOpJ)、[快速入门](https://help.aliyun.com/zh/open-search/llm-intelligent-q-a-version/getting-started/?spm=a2c4g.11186623.0.nextDoc.68274071qlwQrL)
    

## 配置文件

在`.env`文件中配置相关信息：

本文使用的示例文本为[elasticsearch-definitive-guide](https://github.com/elasticsearch-cn/elasticsearch-definitive-guide)。

```
    # AppName，即创建的OpenSearch-LLM智能问答版应用的名称
    appName=${AppName}
    
    # 创建的OpenSearch-LLM智能问答版应用的API访问地址
    host= 
    
    # fed实例的访问地址，本地访问实例为127.0.0.1，若有远程集群则填远程集群的IP地址即可。
    fedHost=
    
    # OpenSearch-LLM智能问答版的相关接口配置
    path=/apps/${AppName}/actions/knowledge-embedding
    splitPath=/apps/${AppName}/actions/knowledge-split
    llmPath=/apps/${AppName}/actions/knowledge-llm
    
    # 需要写入的文件目录
    sourceFileDir=
    
    # "ALIBABA_CLOUD_ACCESS_KEY_ID"
    accesskey=
    
    # "ALIBABA_CLOUD_ACCESS_KEY_SECRET"
    secret=
```


* appName为创建的OpenSearch-LLM智能问答版应用的名称，可在[OpenSearch的控制台-实例管理](https://opensearch.console.aliyun.com/cn-shanghai/openknowledge/instances)中查看;
![image](https://github.com/Huaixinww/havenask-federation/assets/141887897/55b1f96a-e66a-45ba-b6cc-d03cb86a1744)
* host为创建的OpenSearch-LLM智能问答版应用的API访问地址，可在OpenSearch的控制台-实例管理-管理中查看;
![image](https://github.com/Huaixinww/havenask-federation/assets/141887897/bdc19760-e33d-4e06-ab73-ac6cb3dcbcda)
![image](https://github.com/Huaixinww/havenask-federation/assets/141887897/7e689c24-cbfa-4029-ad8a-cd26f5033ada)

### 创建RAM用户，获取accesskey以及secret

accesskey以及secret可以通过创建RAM用户来获取，具体见：[创建RAM用户](https://help.aliyun.com/zh/ram/user-guide/create-a-ram-user?spm=a2c8b.20231166.0.0.10c6336auL7fZN)，[创建AccessKey](https://help.aliyun.com/zh/ram/user-guide/create-an-accesskey-pair#task-2245479)。

操作步骤：

1.  使用阿里云账号（主账号）或RAM管理员登录[RAM控制台](https://ram.console.aliyun.com/)。
    
2.  在左侧导航栏，选择**身份管理 > 用户**。
    
3.  在**用户**页面，单击**创建用户**。
    
4.  在**访问方式**区域，选择访问方式，这里我们选择**OpenAPI调用访问**。
    
5.  单击**确定**。
    
6.  根据界面提示，完成安全验证。
    
7.  成功创建后，可以查看AccessKey对话框，查看AccessKey ID和AccessKey Secret。注意：RAM用户的AccessKey Secret只在创建时显示，不支持查看，请妥善保管。

## 运行demo

### 文本切片、向量化并导入fed

运行`SplitDoc.java`的main方法，对文本进行切分和向量化并写入fed。

如果想要不对文本进行切分，仅对文本进行向量化，那么可以调用`LoadDoc.java`的main方法，对文本进行向量化后写入fed；

### 启动问答服务

#### api部署

运行`DemoApplication.java`的main方法，即可启动问答服务。

测试请求

    curl -H "Content-Type: application/json" http://127.0.0.1:8080/ask -d '{"question": "查询与过滤的区别"}'

输出：

    RequestID=171205087716800655673567
    answer:查询与过滤在Elasticsearch中是两个不同的概念，它们在搜索和数据处理中扮演着不同的角色。
    
    查询（Query）是用于在Elasticsearch中找到匹配特定条件的文档的过程。查询可以是简单的term查询，也可以是复杂的match查询，甚至包括聚合查询。查询可以是评分查询（scoring queries），这种查询不仅会找出匹配的文档，还会计算每个匹配文档的相关性。查询结果通常不会被缓存，因为相关性计算是动态的。
    
    过滤（Filter）则是用于在查询结果中进一步筛选文档的过程。过滤器可以是内部过滤器（internal filter），也可以是post过滤器（post filter）。内部过滤器在查询执行时执行，可以影响搜索结果和聚合结果。例如，可以使用term查询在倒排索引中查找特定的term，并创建一个bitset来描述哪些文档包含该term。然后，Elasticsearch会循环迭代这些bitset以找到满足所有过滤条件的匹配文档集合。post过滤器则是在查询执行之后对搜索结果进行过滤，它只影响搜索结果，不会影响聚合结果。post过滤器通常用于UI的连贯一致性，例如，用户在界面中选择了一类颜色，期望搜索结果已经被过滤，而不是过滤界面上的选项。
    
    总结来说，查询是用于找到匹配文档的过程，而过滤是在查询结果中进一步筛选文档的过程。查询可以是评分查询，计算文档的相关性，而过滤器则是在查询执行后对结果进行筛选。在使用时，应根据需要选择合适的查询和过滤器类型。例如，如果需要对搜索结果和聚合结果做不同的过滤，应使用post_filter，而如果需要同时影响搜索结果和聚合结果，则应使用filter。同时，不推荐直接向用户暴露查询字符串搜索功能，而应更多地使用功能全面的request body查询API。在了解数据在Elasticsearch中是如何被索引的之后，再使用这些API。

#### web demo

    cd ~/havenask-federation/llm
    python web_demo.py

运行该demo会自动打开一个web页面，或者通过访问`http://localhost:7860/`

![image](https://github.com/Huaixinww/havenask-federation/assets/141887897/2e4c0fb3-09a3-4317-815d-578365a5abfe)