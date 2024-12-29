
from flask import Flask, render_template, request
import sqlite3

app = Flask(__name__)

def query_co_occurrences(db_path, entity1, entity2):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    query = '''
        SELECT pmid, sentence
        FROM sentences
        JOIN entity_pairs ON sentences.pair_id = entity_pairs.id
        WHERE entity_pairs.entity1 = ? AND entity_pairs.entity2 = ?
    '''
    cursor.execute(query, (entity1, entity2))

    result = {}
    for pmid, sentence in cursor.fetchall():
        if pmid not in result:
            result[pmid] = []
        if sentence not in result[pmid]:  # Ensure no duplicate sentences
            result[pmid].append(sentence)

    conn.close()
    return result

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/search', methods=['POST'])
def search():
    entity1 = request.form['entity1']
    entity2 = request.form['entity2']
    db_path = 'path/to/your/database.db'  # Update this to the correct path
    results = query_co_occurrences(db_path, entity1, entity2)
    return render_template('results.html', entity1=entity1, entity2=entity2, results=results)

if __name__ == '__main__':
    app.run(debug=True)