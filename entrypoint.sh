#!/bin/bash

# Задаем адреса HDFS и YARN
export HDFS_URI=hdfs://192.168.34.2:8020
export YARN_RESOURCEMANAGER=192.168.34.2:8032

# Добавим базовую конфигурацию Hadoop (core-site.xml, yarn-site.xml),
# чтобы hadoop cli знал куда обращаться

mkdir -p $HADOOP_HOME/etc/hadoop

# core-site.xml
cat > $HADOOP_HOME/etc/hadoop/core-site.xml <<EOF
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>${HDFS_URI}</value>
    </property>
</configuration>
EOF

# yarn-site.xml
cat > $HADOOP_HOME/etc/hadoop/yarn-site.xml <<EOF
<configuration>
    <property>
        <name>yarn.resourcemanager.address</name>
        <value>${YARN_RESOURCEMANAGER}</value>
    </property>
</configuration>
EOF

# Запускаем Python скрипт с логикой
python3 /run_tasks.py

