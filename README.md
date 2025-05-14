# OTUS. Извлечение признаков - Spark MLlib

## План работы

0. Развернем DataProc кластер из репозитория [otus-practice-cloud-infra](https://github.com/NickOsipov/otus-practice-cloud-infra)
    - Клонируем репозиторий
    - Заполняем переменные в `infrastructure/terraform.tfvars`
    - Запускаем:
        ```bash
        cd infrastructure
        terraform init
        terraform plan
        terraform apply -auto-approve
        ```
1. Пройдем по `commands.sh` для развертывания окружения
см в commands.sh  последовательность действий для настройки кластера 
и различные варианты работы с бакетами
    1. для хранения первоначальных данных :otus-mlops-data17
    2. для хранения данных преоразованных в parquett :fraud-detection-data-otus-2025/parquet
    3. Для хранения очищенных данных  :  fraud-detection-data-otus-2025/validated_data
    4. Для хранения данных по Дрифтам :fraud-detection-data-otus-2025


2. Пройдем по notebooks:
    1. feature_DZ3_40convert_to_parquet     - конвертация первоначальных файлов в parquet
    2. featureDZ3_loadParquet               - загрузка данных .parquet для работы
    3. featureDZ3_validation DataParquet    - очистка и исследование данных   
    4. featureDZ3_dataShift                 - Исследование Drifts      


3. Удаляем кластер:
    ```bash
    terraform destroy -auto-approve
    ```