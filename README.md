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

目前项目正在开发迭代中，具体进展可以关注项目issue：[https://github.com/alibaba/havenask-federation/issues](https://github.com/alibaba/havenask-federation/issues)

# 联系我们
官方技术交流钉钉群：

![3293821693450208](https://user-images.githubusercontent.com/590717/206684715-5ab1df49-f919-4d8e-85ee-58b364edef31.jpg)

