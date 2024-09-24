import requests
import json
import os
import pandas as pd
from urllib.parse import quote_plus
from warcio.archiveiterator import ArchiveIterator
from bs4 import BeautifulSoup
import time

# Common Crawl Index Server
SERVER = 'http://index.commoncrawl.org/'

# Актуальный индекс Common Crawl  https://commoncrawl.org/the-data/get-started/
INDEX_NAME = 'CC-MAIN-2024-33'  

# Функция поиска в индексе Common Crawl
def search_cc_index(query, index_name):
    """
    Search the Common Crawl Index for a given query.

    Arguments:
        query (str): The search query.
        index_name (str): The name of the Common Crawl Index to search.

    Returns:
        list: A list of JSON objects representing records found in the Common Crawl Index.
    """
    encoded_query = quote_plus(query)
    index_url = f'{SERVER}{index_name}-index?url={encoded_query}&output=json'
    
    try:
        response = requests.get(index_url, timeout=30)
        response.raise_for_status()
        records = response.text.strip().split('\n')
        return [json.loads(record) for record in records]
    except requests.exceptions.Timeout:
        print(f"Тайм-аут при выполнении запроса для {query}")
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP ошибка при выполнении запроса для {query}: {http_err}")
    except Exception as err:
        print(f"Ошибка при выполнении запроса для {query}: {err}")
    
    return []

# Функция для извлечения страницы из WARC-файла
def fetch_single_record(warc_record_filename, offset, length):
    """
    Fetch a single WARC record from Common Crawl.

    Arguments:
        warc_record_filename (str): The filename of the WARC record.
        offset (int): The byte offset in the WARC file.
        length (int): The byte length to fetch.

    Returns:
        str or None: The HTML content of the page, or None if failed.
    """
    s3_url = f'https://data.commoncrawl.org/{warc_record_filename}'
    byte_range = f'bytes={offset}-{offset + length - 1}'
    
    try:
        response = requests.get(
            s3_url,
            headers={'Range': byte_range},
            stream=True,
            timeout=30
        )
        response.raise_for_status()
        
        if response.status_code == 206:
            stream = ArchiveIterator(response.raw)
            for warc_record in stream:
                if warc_record.rec_type == 'response':
                    content = warc_record.content_stream().read().decode('utf-8', errors='ignore')
                    return content
        else:
            print(f"Не удалось получить данные: {response.status_code}")
    except requests.exceptions.Timeout:
        print(f"Тайм-аут при извлечении данных из {warc_record_filename}")
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP ошибка при извлечении данных из {warc_record_filename}: {http_err}")
    except Exception as err:
        print(f"Ошибка при извлечении данных из {warc_record_filename}: {err}")
    
    return None

# Функция для обработки и вывода контента
def process_content(content, query):
    """
    Process and print the HTML content.

    Arguments:
        content (str): The HTML content.
        query (str): The query associated with the content.
    """
    soup = BeautifulSoup(content, 'html.parser')
    title = soup.find('title')
    if title:
        title_text = title.get_text(strip=True)
    else:
        title_text = "Без заголовка"
    
    print(f"\n--- Результат для запроса: {query} ---")
    print(f"Заголовок страницы: {title_text}")
    print(f"URL: {soup.find('meta', attrs={'property': 'og:url'})['content'] if soup.find('meta', attrs={'property': 'og:url'}) else 'Неизвестно'}")
    print("--- Начало контента страницы ---")
    print(soup.get_text()[:1000])  # Выводим первые 1000 символов текста
    print("--- Конец контента страницы ---\n")

# Основная функция для выполнения поиска и обработки результатов
def search_and_print_results(queries, index_name):
    """
    Perform search on Common Crawl and print the results.

    Arguments:
        queries (dict): A dictionary where keys are group names and values are lists of queries.
        index_name (str): The name of the Common Crawl Index to search.
    """
    for group_name, query_list in queries.items():
        print(f"\n=== Поиск для группы: {group_name} ===\n")
        
        for query in query_list:
            print(f"\nПоиск по запросу: {query}")
            records = search_cc_index(query, index_name)
            
            if records:
                print(f"Найдено {len(records)} записей для {query}")
                
                # Ограничиваем количество записей для ускорения
                for record in records[:5]:  # Например, первые 5 записей
                    warc_filename = record.get('filename')
                    offset = record.get('offset')
                    length = record.get('length')
                    
                    if warc_filename and offset and length:
                        content = fetch_single_record(warc_filename, int(offset), int(length))
                        if content:
                            process_content(content, query)
                        else:
                            print(f"Не удалось извлечь контент для записи: {record}")
                    else:
                        print(f"Недостаточно данных для извлечения записи: {record}")
                    
                    # Задержка между запросами, чтобы избежать перегрузки сервера
                    time.sleep(1)
            else:
                print(f"Результаты не найдены для {query}")
        
        print(f"\n=== Поиск для группы {group_name} завершен ===")

if __name__ == "__main__":
    # Словарь с группами запросов
    search_queries = {
        "г. Пермь, Пермский Политех, кафедра ИТАС": [
            "pstu.ru",  # Домен Пермского Политеха
            "perm.ru"   # Город Пермь
        ],
        "МГУ им. Ломоносова, МФТИ им. Баумана": [
            "msu.ru",   # Домен МГУ
            "mipt.ru"   # Домен МФТИ
        ],
        "Борис Пастернак в контексте г. Перми": [
            "pasternakmuseum.ru"  # Предположительный сайт, связанный с Борисом Пастернаком
        ]
    }

    # Вызов основной функции для поиска и вывода результатов
    search_and_print_results(search_queries, INDEX_NAME)
