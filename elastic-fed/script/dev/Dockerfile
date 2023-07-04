FROM registry.cn-hangzhou.aliyuncs.com/havenask/ha3_runtime:1.0.0-beta

# 创建一个名为 havenask 的用户，并将其添加到 sudo 组中
RUN useradd -ms /bin/bash havenask && echo "havenask ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers

# 切换到 havenask 用户
USER havenask

WORKDIR /home/havenask

RUN git clone https://github.com/alibaba/havenask-federation.git
# 下载并解压 JDK 11
RUN wget -O jdk.tar.gz https://download.java.net/openjdk/jdk11/ri/openjdk-11+28_linux-x64_bin.tar.gz && tar -xzf jdk.tar.gz && rm jdk.tar.gz && mv jdk* /home/havenask/jdk11

# 设置 Java 环境变量
ENV JAVA_HOME /home/havenask/jdk11
ENV PATH $JAVA_HOME/bin:$PATH

# 预执行gradle命令, 下载相关依赖
RUN  cd havenask-federation/elastic-fed;./gradlew assemble -p distribution/archives/linux-tar/

RUN rm -rf havenask-federation