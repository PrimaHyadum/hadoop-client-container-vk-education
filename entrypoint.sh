#!/bin/bash
set -euo pipefail

# Значения из задания
HDFS_URI="hdfs://192.168.34.2:8020"
YARN_ADDR="192.168.34.2:8032"

# Если HADOOP_HOME не задан — берём дефолт
: "${HADOOP_HOME:=/opt/hadoop}"

# Конфиги Hadoop
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
        <value>${YARN_ADDR}</value>
    </property>
</configuration>
EOF

chmod +x /run_tasks.py

echo "=== Starting tasks ==="
exec python3 /run_tasks.py


