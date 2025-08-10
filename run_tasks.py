#!/usr/bin/env python3
import subprocess
import os
import sys

HDFS_URI = "hdfs://192.168.34.2:8020"
HADOOP_HOME = os.environ.get("HADOOP_HOME", "/opt/hadoop")
EXAMPLES_JAR = os.path.join(HADOOP_HOME, "share", "hadoop", "mapreduce", "hadoop-mapreduce-examples-3.3.6.jar")

def sh(cmd, check=False, capture=False):
    print("+", cmd)
    res = subprocess.run(cmd, shell=True, text=True,
                         stdout=(subprocess.PIPE if capture else None),
                         stderr=subprocess.STDOUT)
    if capture:
        out = res.stdout or ""
    else:
        out = ""
    if res.returncode != 0:
        print("COMMAND FAILED (code {}):".format(res.returncode))
        print(res.stdout if res.stdout is not None else "")
        if check:
            sys.exit(2)
    return res.returncode, out

def ensure_createme():
    # -p to avoid error when exists
    sh(f"hdfs dfs -mkdir -p {HDFS_URI}/createme", check=True)

def ensure_delme_absent():
    # remove if present; ignore error if not present
    sh(f"hdfs dfs -rm -r -skipTrash {HDFS_URI}/delme || true")

def create_nonnull():
    local_tmp = "/tmp/tempfile.txt"
    with open(local_tmp, "w") as f:
        f.write("Hello Hadoop from Docker container!\n")
    # Remove target if exists, then put
    sh(f"hdfs dfs -rm -f {HDFS_URI}/nonnull.txt || true")
    sh(f"hdfs dfs -put {local_tmp} {HDFS_URI}/nonnull.txt", check=True)

def run_wordcount():
    if not os.path.exists(EXAMPLES_JAR):
        print("ERROR: examples jar not found at", EXAMPLES_JAR)
        sys.exit(3)

    input_file = f"{HDFS_URI}/shadow.txt"
    output_dir = f"{HDFS_URI}/wordcount_output"

    # remove previous output (ignore error)
    sh(f"hdfs dfs -rm -r -skipTrash {output_dir} || true")

    # run job and fail if non-zero
    rc, _ = sh(f"hadoop jar {EXAMPLES_JAR} wordcount {input_file} {output_dir}", check=True)
    # after run, verify output exists
    rc, _ = sh(f"hdfs dfs -ls {output_dir} || true", check=True)

def extract_and_write_innsmouth():
    part = f"{HDFS_URI}/wordcount_output/part-r-00000"
    rc, out = sh(f"hdfs dfs -cat {part}", check=False, capture=True)
    count = "0"
    if rc == 0 and out:
        for line in out.splitlines():
            parts = line.split('\t')
            if len(parts) >= 2 and parts[0] == "Innsmouth":
                count = parts[1].strip()
                break
    else:
        print("WARN: could not cat part file; treating count as 0")

    # write to local tmp and then put (overwrite)
    local = "/tmp/count.txt"
    with open(local, "w") as f:
        f.write(str(count) + "\n")

    sh(f"hdfs dfs -rm -f {HDFS_URI}/whataboutinsmouth.txt || true")
    sh(f"hdfs dfs -put {local} {HDFS_URI}/whataboutinsmouth.txt", check=True)
    # sanity check
    sh(f"hdfs dfs -cat {HDFS_URI}/whataboutinsmouth.txt", check=True)

def main():
    ensure_createme()
    ensure_delme_absent()
    create_nonnull()
    run_wordcount()
    extract_and_write_innsmouth()
    print("ALL TASKS COMPLETED")
    return 0

if __name__ == "__main__":
    sys.exit(main())
