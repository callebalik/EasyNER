import json
import sqlite3
import os
from glob import glob
from tqdm import tqdm

def create_database(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    # cursor.execute('DROP TABLE IF EXISTS articles')
    # cursor.execute('DROP TABLE IF EXISTS sentences')
    # cursor.execute('DROP TABLE IF EXISTS entities')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS articles (
            pmid INTEGER PRIMARY KEY,
            title TEXT
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sentences (
            id INTEGER PRIMARY KEY,
            pmid INTEGER,
            sentence_index INTEGER,
            text TEXT,
            FOREIGN KEY(pmid) REFERENCES articles(pmid)
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS entities (
            id INTEGER PRIMARY KEY,
            pmid INTEGER,
            sentence_index INTEGER,
            entity TEXT,
            entity_text TEXT,
            span TEXT,
            FOREIGN KEY(pmid) REFERENCES articles(pmid)
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS processed_files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT UNIQUE
        )
    ''')
    conn.commit()
    return conn

def insert_data(conn, data, filename):
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM processed_files WHERE filename = ?', (filename,))
    if cursor.fetchone()[0] > 0:
        print(f"File {filename} already processed. Skipping.")
        return

    for pmid, article in tqdm(data.items(), desc="Inserting articles"):
        title = article['title']
        cursor.execute('''
            INSERT OR IGNORE INTO articles (pmid, title)
            VALUES (?, ?)
        ''', (pmid, title))
        for sentence_index, sentence in enumerate(tqdm(article['sentences'], desc="Inserting sentences", leave=False)):
            text = sentence['text']
            cursor.execute('''
                INSERT INTO sentences (pmid, sentence_index, text)
                VALUES (?, ?, ?)
            ''', (pmid, sentence_index, text))
            for entity, entity_texts in sentence['entities'].items():
                for entity_text, span in zip(entity_texts, sentence['entity_spans'][entity]):
                    cursor.execute('''
                        INSERT INTO entities (pmid, sentence_index, entity, entity_text, span)
                        VALUES (?, ?, ?, ?, ?)
                    ''', (pmid, sentence_index, entity, entity_text, json.dumps(span)))

    cursor.execute('INSERT INTO processed_files (filename) VALUES (?)', (filename,))
    conn.commit()

def load_json_to_db(json_files, db_path):
    conn = create_database(db_path)
    for json_file in tqdm(json_files, desc="Processing JSON files"):
        with open(json_file, 'r', encoding='utf-8') as file:
            data = json.load(file)
        insert_data(conn, data, json_file)
    conn.close()

def get_total_json_size(json_files):
    total_size = sum(os.path.getsize(json_file) for json_file in json_files)
    return total_size

def compare_sizes(json_files, db_path):
    total_json_size = get_total_json_size(json_files)
    db_size = os.path.getsize(db_path)
    print(f"Total size of JSON files: {total_json_size / (1024 * 1024):.2f} MB")
    print(f"Size of the database file: {db_size / (1024 * 1024):.2f} MB")
    compression_ratio = total_json_size / db_size
    print(f"Compression ratio: {compression_ratio:.2f}")

def run(data_files, db_path):
    load_json_to_db(data_files, db_path)
    compare_sizes(data_files, db_path)

if __name__ == "__main__":
    input_folder = "/proj/berzelius-2021-21/users/x_caoll/EasyNer_ner_output/ner_merged/"

    data_files = sorted(
        glob(f"{input_folder}*.json"),
        key=lambda f: int("".join(filter(str.isdigit, f))),
    )

    db_path = '/proj/berzelius-2021-21/users/x_caoll/EasyNer_ner_output/database# 2.db'
    data_files = data_files[110:120]
    load_json_to_db(data_files, db_path)
    compare_sizes(data_files, db_path)