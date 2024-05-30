# 项目介绍
[Havenask](https://github.com/alibaba/havenask) 是阿里巴巴集团自主研发的搜索引擎，也是阿里巴巴内部广泛使用的大规模分布式检索系统，为淘宝、天猫、菜鸟、高德、饿了么、全球化等全集团的搜索业务提供高性能、低成本、易用的搜索服务。同时，Havenask具有灵活的定制和开发能力，支持算法快速迭代，帮助客户和开发者量身定做适合自身业务的智能搜索服务，助力业务增长。

基于Havenask，我们推出了分布式联邦集群解决方案——Havenask-federation。方案的设计初衷在于：

* 提升Havenask易用性：Havenask-federation继承了Elasticsearch简单上手，使用方便的优势，通过Havenask-federation来使用Havenask，可以降低Havenask的使用门槛，方便开发者更好的使用Havenask引擎。

* 兼容Elasticsearch生态：Havenask-federation可以支持绝大多数Elasticsearch API，方便使用Elasticsearch的SDK和相关工具访问Havenask-federation，实现快速迁移和扩展。

# Havenask-federation特色

* 无外部依赖：Havenask-federation不依赖任何外部组件，能够直接在容器环境中快速启动服务，提供Havenask引擎完整的分布式、高可用能力。
* 高度兼容 Elasticsearch：Havenask-federation兼容超过90%的Elasticsearch API，让开发者可以便捷地通过现有的Elasticsearch API和SDK直接对接Havenask引擎。
* SQL 语法完全兼容：完整支持Havenask SQL，实现了100%的语法兼容性，支持Join和子查询SQL语法，使得用户能够执行跨多个索引表的复杂联合查询。
* 强大的向量引擎：全面支持Havenask的向量引擎功能，深度集成阿里巴巴达摩院自研向量检索引擎，支持百亿甚至千亿级别的高维度向量的低延迟计算。
* 索引分片与副本自定义：创建索引时，用户可自定义分片和副本的数量。Havenask-federation会智能地规划集群内所有分片的布局，确保分片分布的均衡性。

# Havenask-federation架构

Havenask-federation承担三种角色：master、data、coordinate，其中data角色负责管理Havenask的Searcher进程，coordinate角色负责管理Havenask的Qrs进程，Havenask-federation单个进程可以同时承担master、data、coordinate角色。

## 分布式架构

![image](https://github.com/alibaba/havenask-federation/assets/5070449/b697cb66-79ec-4746-85fe-fcba77ce1449)

## 单机架构

![image](https://github.com/alibaba/havenask-federation/assets/5070449/3e0f9062-924a-437c-9f9d-8a785234179c)


# 使用说明
## 使用依赖
环境要求
* 确保机器内存不少于8G，cpu不少于2核，磁盘大小大于20G。
* 使用前确保设备已经安装和启动Docker服务。

## 启动容器

### 方式一

克隆仓库：

    git clone https://github.com/alibaba/havenask-federation.git
    cd havenask-federation

执行启动命令：

    cd elastic-fed/script
    ./create_container.sh <CONTAINER_NAME> <IMAGE_NAME>

    #示例命令：
    ./create_container.sh test registry.cn-hangzhou.aliyuncs.com/havenask/fed:latest

### 方式二
直接拷贝启动脚本[elastic-fed/script/create_container.sh](https://github.com/alibaba/havenask-federation/blob/main/elastic-fed/script/create_container.sh)到本地，执行启动命令：

    sh create_container.sh <CONTAINER_NAME> <IMAGE_NAME>

    #示例命令：
    sh create_container.sh test registry.cn-hangzhou.aliyuncs.com/havenask/fed:latest

## 进入容器

容器启动后，进入容器命令：

    ./<CONTAINER_NAME>/sshme

    #示例命令：
    ./test/sshme

## 启动fed

进入容器后，在根目录下执行：

    ./bin/havenask

就能启动fed进程，如果要以daemon模式启动，命令为：

    ./bin/havenask -d

容器附带了一个dashboards可以用作可视化操作fed，启动方式为：

     cd dashboards/bin/
     ./havenask-dashboards

如果要以daemon模式启动，命令为：

     nohup ./havenask-dashboards &

## 访问fed

fed启动后，默认端口是9200，假设在本地访问，访问示例如下：

    curl 127.0.0.1:9200
    {
      "name" : "l57e02076.sqa.nu8",
      "cluster_name" : "docker-cluster",
      "cluster_uuid" : "clGgz853S3W6mSaWLCeyCQ",
      "version" : {
        "distribution" : "havenask",
        "number" : "1.0.0-SNAPSHOT",
        "build_type" : "docker",
        "build_hash" : "unknown",
        "build_date" : "2023-05-10T08:05:27.627696220Z",
        "build_snapshot" : true,
        "lucene_version" : "8.7.0",
        "minimum_wire_compatibility_version" : "6.8.0",
        "minimum_index_compatibility_version" : "6.0.0-beta1"
      },
      "tagline" : "The Havenask Federation Project"
    }

可以通过dashboards，在dev_tools中访问fed

    http://127.0.0.1:5601/app/dev_tools#/console

## 更多功能
更多内容请参见：[快速开始](https://github.com/alibaba/havenask-federation/wiki/%E5%BF%AB%E9%80%9F%E5%BC%80%E5%A7%8B)

# 联系我们
官方技术交流钉钉群：

![296146789-ad08444f-4570-45fd-908d-a90a6cca3f79](https://github.com/alibaba/havenask-federation/assets/5070449/55f4d8b9-b997-466d-b525-5e5c382246be)



