#!/bin/bash
set -euo pipefail

# default HADOOP_HOME if not set
: "${HADOOP_HOME:=/opt/hadoop}"

# Задаем адреса HDFS и YARN
export HDFS_URI=hdfs://192.168.34.2:8020
export YARN_RESOURCEMANAGER=192.168.34.2:8032

mkdir -p "$HADOOP_HOME/etc/hadoop"

cat > "$HADOOP_HOME/etc/hadoop/core-site.xml" <<EOF
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>${HDFS_URI}</value>
    </property>
</configuration>
EOF

cat > "$HADOOP_HOME/etc/hadoop/yarn-site.xml" <<EOF
<configuration>
    <property>
        <name>yarn.resourcemanager.address</name>
        <value>${YARN_RESOURCEMANAGER}</value>
    </property>
</configuration>
EOF

# Ensure script is executable
chmod +x /run_tasks.py

echo "Starting run_tasks.py"
exec python3 /run_tasks.py


