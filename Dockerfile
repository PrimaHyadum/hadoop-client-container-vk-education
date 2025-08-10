FROM openjdk:8-jdk-slim

# Установим необходимые утилиты
RUN apt-get update && apt-get install -y wget curl python3 python3-pip

# Скачиваем Hadoop 3.3.6 (клиент)
RUN wget https://downloads.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz && \
    tar -xzf hadoop-3.3.6.tar.gz && \
    mv hadoop-3.3.6 /opt/hadoop && \
    rm hadoop-3.3.6.tar.gz

ENV HADOOP_HOME=/opt/hadoop
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

# Копируем скрипты внутрь контейнера
COPY entrypoint.sh /entrypoint.sh
COPY run_tasks.py /run_tasks.py

RUN chmod +x /entrypoint.sh

# Устанавливаем Python зависимости, если нужны (например, requests)
# В нашем случае - нет

ENTRYPOINT ["/entrypoint.sh"]

