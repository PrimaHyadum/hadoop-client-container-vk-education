#!/usr/bin/env python3
import subprocess
import os
import sys

HDFS_URI = "hdfs://192.168.34.2:8020"
HADOOP_HOME = os.environ.get("HADOOP_HOME", "/opt/hadoop")
EXAMPLES_JAR = os.path.join(
    HADOOP_HOME,
    "share", "hadoop", "mapreduce",
    "hadoop-mapreduce-examples-3.3.6.jar"
)

def run(cmd, check=True, capture=False):
    print("+", cmd)
    res = subprocess.run(cmd, shell=True, text=True,
                         stdout=(subprocess.PIPE if capture else None),
                         stderr=subprocess.STDOUT)
    if capture:
        out = res.stdout or ""
    else:
        out = ""
    if check and res.returncode != 0:
        print("ERROR:", out)
        sys.exit(1)
    return out

def safe_rm(path):
    run(f"hdfs dfs -rm -r -skipTrash {path} || true", check=False)

def safe_rm_file(path):
    run(f"hdfs dfs -rm -f {path} || true", check=False)

def step1_create_dir():
    run(f"hdfs dfs -mkdir -p {HDFS_URI}/createme")

def step2_delete_dir():
    safe_rm(f"{HDFS_URI}/delme")

def step3_create_file():
    tmp_file = "/tmp/nonnull.txt"
    with open(tmp_file, "w") as f:
        f.write("Some content\n")
    safe_rm_file(f"{HDFS_URI}/nonnull.txt")
    run(f"hdfs dfs -put {tmp_file} {HDFS_URI}/nonnull.txt")

def step4_run_wordcount():
    if not os.path.exists(EXAMPLES_JAR):
        print(f"ERROR: {EXAMPLES_JAR} not found")
        sys.exit(2)

    output_dir = f"{HDFS_URI}/wc_out"
    safe_rm(output_dir)

    run(f"hadoop jar {EXAMPLES_JAR} wordcount "
        f"{HDFS_URI}/shadow.txt {output_dir}")

def step5_write_innsmouth_count():
    output_file = f"{HDFS_URI}/wc_out/part-r-00000"
    text = run(f"hdfs dfs -cat {output_file}", capture=True, check=False)
    count = "0"
    for line in text.splitlines():
        parts = line.strip().split()
        if len(parts) == 2 and parts[0] == "Innsmouth":
            count = parts[1]
            break
    tmp_file = "/tmp/whataboutinsmouth.txt"
    with open(tmp_file, "w") as f:
        f.write(count + "\n")
    safe_rm_file(f"{HDFS_URI}/whataboutinsmouth.txt")
    run(f"hdfs dfs -put {tmp_file} {HDFS_URI}/whataboutinsmouth.txt")

def main():
    step1_create_dir()
    step2_delete_dir()
    step3_create_file()
    step4_run_wordcount()
    step5_write_innsmouth_count()
    print("=== All tasks completed successfully ===")

if __name__ == "__main__":
    main()
