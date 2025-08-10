import subprocess

HDFS_URI = "hdfs://192.168.34.2:8020"

def run_cmd(cmd):
    print(f"Executing: {cmd}")
    result = subprocess.run(cmd, shell=True, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if result.returncode != 0:
        print(f"Error: {result.stderr}")
    else:
        print(result.stdout)
    return result

def create_directory():
    cmd = f"hadoop fs -mkdir {HDFS_URI}/createme"
    run_cmd(cmd)

def delete_directory():
    cmd = f"hadoop fs -rm -r -skipTrash {HDFS_URI}/delme"
    run_cmd(cmd)

def create_file():
    # создадим локальный временный файл
    with open("/tmp/tempfile.txt", "w") as f:
        f.write("Hello Hadoop from Docker container!\n")
    # загрузим на HDFS
    cmd = f"hadoop fs -put -f /tmp/tempfile.txt {HDFS_URI}/nonnull.txt"
    run_cmd(cmd)

def run_wordcount():
    # Используем пример MR job из Hadoop (в hadoop-mapreduce-examples.jar)
    # Убедимся, что jar файл есть
    jar_path = f"$HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.6.jar"
    input_file = f"{HDFS_URI}/shadow.txt"
    output_dir = f"{HDFS_URI}/wordcount_output"

    # Удалим старый output, если есть
    run_cmd(f"hadoop fs -rm -r -skipTrash {output_dir}")

    # Запускаем джобу wordcount через yarn
    cmd = f"hadoop jar {jar_path} wordcount {input_file} {output_dir}"
    run_cmd(cmd)

def count_word_in_output():
    output_file = f"{HDFS_URI}/wordcount_output/part-r-00000"
    word_to_find = "Innsmouth"

    # Считаем вхождения слова в output файла
    cmd = f"hadoop fs -cat {output_file}"
    result = subprocess.run(cmd, shell=True, text=True, stdout=subprocess.PIPE)
    count = 0
    for line in result.stdout.splitlines():
        # Формат: слово<tab>число
        parts = line.split('\t')
        if len(parts) == 2 and parts[0] == word_to_find:
            count = parts[1]
            break

    # Записываем результат в файл /whataboutinsmouth.txt на HDFS
    with open("/tmp/count.txt", "w") as f:
        f.write(str(count) + "\n")

    cmd_put = f"hadoop fs -put -f /tmp/count.txt {HDFS_URI}/whataboutinsmouth.txt"
    run_cmd(cmd_put)

def main():
    create_directory()
    delete_directory()
    create_file()
    run_wordcount()
    count_word_in_output()

if __name__ == "__main__":
    main()
