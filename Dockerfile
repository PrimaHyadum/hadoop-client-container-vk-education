FROM ubuntu:20.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    openjdk-8-jdk \
    wget \
    python3 \
    python3-pip \
    vim \
    curl \
    ca-certificates \
    procps \
    netcat \
 && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Hadoop 3.3.6
RUN wget -q https://downloads.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz && \
    tar -xzvf hadoop-3.3.6.tar.gz && \
    mv hadoop-3.3.6 /usr/local/hadoop && \
    rm hadoop-3.3.6.tar.gz

ENV HADOOP_HOME=/usr/local/hadoop
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
ENV PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH

# Перенесите ваши xml-файлы рядом с Dockerfile (core-site.xml, hdfs-site.xml, mapred-site.xml, yarn-site.xml)
COPY core-site.xml $HADOOP_CONF_DIR/core-site.xml
COPY hdfs-site.xml $HADOOP_CONF_DIR/hdfs-site.xml
COPY mapred-site.xml $HADOOP_CONF_DIR/mapred-site.xml
COPY yarn-site.xml $HADOOP_CONF_DIR/yarn-site.xml

# Скопировать скрипт задач
COPY run_tasks.py /run_tasks.py
RUN chmod +x /run_tasks.py

WORKDIR /

# Запуск без entrypoint — просто запускаем наш скрипт как CMD
CMD ["python3", "/run_tasks.py"]

