import pandas as pd
import numpy as np
from scipy.stats import ks_2samp
import os
import s3fs
import matplotlib.pyplot as plt
from tqdm import tqdm
from dotenv import load_dotenv

# Загрузка переменных окружения
load_dotenv()

# Настройка подключения к S3
fs = s3fs.S3FileSystem(
    key=os.getenv('S3_ACCESS_KEY'),
    secret=os.getenv('S3_SECRET_KEY'),
    endpoint_url="https://storage.yandexcloud.net",
    client_kwargs={'region_name': 'ru-central1'}
)

S3_BUCKET = "otus-mlops-data17"

def get_file_list():
    """Получаем список всех файлов в бакете"""
    files = fs.ls(S3_BUCKET)
    return sorted([f.split('/')[-1] for f in files if f.endswith('.txt')])

def load_sample(file_path, n_samples=100000):
    """Загрузка выборки данных с автоматическим определением столбцов"""
    with fs.open(f"{S3_BUCKET}/{file_path}", "rb") as f:
        # Сначала читаем заголовки для проверки столбцов
        sample = pd.read_csv(f, nrows=1)
        print(f"\nСтолбцы в файле {file_path}: {list(sample.columns)}")
        f.seek(0)
        
        # Загружаем данные
        chunks = []
        for chunk in pd.read_csv(f, chunksize=10000):
            chunks.append(chunk)
            if sum(len(c) for c in chunks) >= n_samples:
                break
        return pd.concat(chunks)

def find_numeric_column(df):
    """Находит подходящий числовой столбец для анализа"""
    numeric_cols = [col for col in df.columns if pd.api.types.is_numeric_dtype(df[col])]
    
    # Предпочтительные названия столбцов с суммой
    preferred_names = ['amount', 'tx_amount', 'transaction_amount', 'sum', 'value']
    
    for name in preferred_names:
        if name in numeric_cols:
            return name
    
    # Если предпочтительных нет, берем первый числовой
    return numeric_cols[0] if numeric_cols else None

def calculate_drift_metrics(files):
    """Вычисление метрик дрифта между последовательными файлами"""
    drift_results = []
    prev_df = None
    
    for i, file in enumerate(tqdm(files, desc="Анализ файлов")):
        try:
            current_df = load_sample(file)
            if current_df is None:
                continue
                
            target_col = find_numeric_column(current_df)
            if not target_col:
                print(f"В файле {file} нет подходящих числовых столбцов")
                continue
            
            if prev_df is not None and target_col in prev_df.columns:
                # Вычисляем KS статистику
                stat, p_value = ks_2samp(
                    prev_df[target_col].dropna(),
                    current_df[target_col].dropna()
                )
                
                # Сохраняем метрики
                drift_results.append({
                    'file_pair': f"{files[i-1]} → {file}",
                    'column': target_col,
                    'ks_statistic': stat,
                    'p_value': p_value,
                    'prev_mean': prev_df[target_col].mean(),
                    'current_mean': current_df[target_col].mean(),
                    'prev_std': prev_df[target_col].std(),
                    'current_std': current_df[target_col].std()
                })
            
            prev_df = current_df
            
        except Exception as e:
            print(f"\nОшибка обработки файла {file}: {str(e)}")
            continue
    
    return pd.DataFrame(drift_results)

def plot_drift_metrics(results_df):
    """Визуализация результатов анализа дрифта"""
    if results_df.empty:
        print("Нет данных для визуализации")
        return
    
    plt.figure(figsize=(15, 10))
    
    # График KS статистики
    plt.subplot(2, 1, 1)
    plt.plot(results_df['ks_statistic'], 'o-', label='KS статистика')
    plt.axhline(y=0.2, color='r', linestyle='--', label='Порог дрифта (0.2)')
    plt.title(f'Статистика Колмогорова-Смирнова ({results_df["column"].iloc[0]})')
    plt.ylabel('KS статистика')
    plt.xticks(range(len(results_df)), results_df['file_pair'], rotation=45)
    plt.legend()
    plt.grid()
    
    # График средних значений
    plt.subplot(2, 1, 2)
    plt.errorbar(
        range(len(results_df)),
        results_df['current_mean'],
        yerr=results_df['current_std'],
        fmt='o-',
        label='Текущее среднее'
    )
    plt.errorbar(
        range(len(results_df)),
        results_df['prev_mean'],
        yerr=results_df['prev_std'],
        fmt='s--',
        label='Предыдущее среднее'
    )
    plt.title(f'Сравнение средних значений ({results_df["column"].iloc[0]})')
    plt.ylabel('Среднее значение')
    plt.xlabel('Пара файлов')
    plt.xticks(range(len(results_df)), results_df['file_pair'], rotation=45)
    plt.legend()
    plt.grid()
    
    plt.tight_layout()
    plt.savefig('drift_analysis.png')
    plt.show()

if __name__ == "__main__":
    # Получаем список файлов
    files = get_file_list()
    print(f"Найдено файлов для анализа: {len(files)}")
    print("Примеры файлов:", files[:5])
    
    # Вычисляем метрики дрифта
    drift_df = calculate_drift_metrics(files)
    
    if drift_df.empty:
        print("Не удалось вычислить дрифт ни для одной пары файлов")
    else:
        # Сохраняем результаты
        drift_df.to_csv('drift_results.csv', index=False)
        print("\nРезультаты сохранены в drift_results.csv")
        print(f"Анализируемый столбец: {drift_df['column'].iloc[0]}")
        
        # Визуализируем
        plot_drift_metrics(drift_df)
        print("Графики сохранены в drift_analysis.png")
