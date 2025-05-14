# Description: Команды для работы с Yandex.Cloud DataProc

# Обновляем переменные окружения в файле .env
# S3_BUCKET - название бакета S3
# PROXY_VM_IP - IP-адрес прокси-виртуальной машины

# Подгружаем переменные окружения
source .env

# Копируем данные в созданный бакет S3
make upload-data

# Подключаемся к прокси виртуальной машине 
ssh ubuntu@${PROXY_VM_IP}

# Подключаемся к мастер-ноде кластера
ssh dataproc-master

# Создаем папку для данных
hdfs dfs -mkdir -p /user/ubuntu/data

# Копируем данные из S3 в HDFS
hadoop distcp s3a://${S3_BUCKET}/titanic/* /user/ubuntu/data/titanic
hadoop distcp s3a://${S3_BUCKET}/* /user/ubuntu/data/

hadoop distcp s3a://otus-bucket-b1g77g5lrul51pldt28m/* /user/ubuntu/data/
# закачиваем данные для тестов
hadoop distcp s3a://otus-mlops-data17/2019-10-21.txt /user/ubuntu/data/
hadoop distcp s3a://fraud-detection-data-otus-2025/parquet/* /user/ubuntu/data/



# Проверяем, что данные скопировались
hdfs dfs -ls /user/ubuntu/data
drwxr-xr-x   - ubuntu hadoop          0 2025-05-14 12:56 /user/ubuntu/data/2020-01-19
####################################################################################################

# Запуск Jupyter Notebook
# Для этого необходимо пробросить порт 8888 с мастер-ноды на прокси-виртуальную машину и на локальную машину

# Подключаемся к прокси виртуальной машине
ssh -L 8888:localhost:8888 ubuntu@${PROXY_VM_IP}


# Подключаемся к мастер-ноде кластера
ssh -L 8888:localhost:8888 dataproc-master

# Запускаем Jupyter Notebook
jupyter notebook \
  --no-browser \
  --port=8888 \
  --ip=0.0.0.0 \
  --allow-root \
  --NotebookApp.token='' \
  --NotebookApp.password=''

# Далее можем поключиться в VS Code к Jupyter Notebook по адресу http://localhost:8888
скачать недостающие данные

hadoop distcp -update s3a://otus-mlops-data17/ hdfs:///user/ubuntu/data/