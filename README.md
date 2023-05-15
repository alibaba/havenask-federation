# 项目介绍
[Havenask](https://github.com/alibaba/havenask) 是阿里巴巴集团自主研发的搜索引擎，也是阿里巴巴内部广泛使用的大规模分布式检索系统，为淘宝、天猫、菜鸟、高德、饿了么、全球化等全集团的搜索业务提供高性能、低成本、易用的搜索服务。同时，Havenask具有灵活的定制和开发能力，支持算法快速迭代，帮助客户和开发者量身定做适合自身业务的智能搜索服务，助力业务增长。

基于Havenask，我们推出了分布式联邦集群解决方案——Havenask-federation。它在保留Havenask引擎强大功能的同时，还具备以下特点：

* 提升Havenask易用性：Havenask-federation继承了Elasticsearch简单上手，使用方便的优势，通过Havenask-federation来使用Havenask，可以降低Havenask的使用门槛，方便开发者更好的使用Havenask引擎。

* 兼容Elasticsearch生态：Havenask-federation可以支持绝大多数Elasticsearch API，方便使用Elasticsearch的SDK和相关工具访问Havenask-federation，实现快速迁移和扩展。

# Havenask-federation架构

Havenask-federation承担三种角色：master、data、coordinate，其中data角色负责管理Havenask的Searcher进程，coordinate角色负责管理Havenask的Qrs进程，Havenask-federation进程可以同时承担master、data、coordinate角色。

## 分布式架构

![image](https://user-images.githubusercontent.com/5070449/226837096-99f50ae0-f391-48a7-af85-5a916bd335c8.png)

## 单机架构

![image](https://user-images.githubusercontent.com/5070449/226837197-3d591392-9b24-4032-877b-fc4534c2c64a.png)


# 使用说明
## 获取镜像

目前fed还未发布release镜像，所以需要通过编译获得。

编译依赖：

*   jdk15
*   docker

编译方式：

    cd elastic-fed
    ./gradlew buildDockerImage -p distribution/docker

编译过程先是打包fed安装包，再制作镜像，制作镜像第一次执行会去拉取havenask runtime的镜像，这个镜像有8GB左右，所以命令执行时间比较长，命令执行成功后，会生成一个havenask-fed:test的镜像。

## 启动容器

项目附带了启动命令：

    cd elastic-fed/script
    ./create_container.sh <CONTAINER_NAME> havenask-fed:test

这样就启动了一个容器，假设启动一个名为test的容器，示例命令：

    ./create_container.sh test havenask-fed:test

容器启动后，进入容器命令：

    ./<CONTAINER_NAME>/sshme

## 启动fed

进入容器后，在根目录下执行：

    ./bin/havenask

就能启动fed进程，如果要以daemon模式启动，命令为：

    ./bin/havenask -d

容器附带了一个opensearch-dashbards可以用作可视化操作fed，启动方式为：

     cd dashboards/bin/
     ./opensearch-dashboards

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

可以通过opensearch-dashboards，在dev\_tools中访问fed

    http://127.0.0.1:5601/app/dev_tools#/console

## 更多功能
更多内容请参见：[快速开始](https://github.com/alibaba/havenask-federation/wiki/%E5%BF%AB%E9%80%9F%E5%BC%80%E5%A7%8B)

# 联系我们
官方技术交流钉钉群：

![3293821693450208](https://user-images.githubusercontent.com/590717/206684715-5ab1df49-f919-4d8e-85ee-58b364edef31.jpg)

