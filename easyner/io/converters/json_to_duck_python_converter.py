import duckdb
import json
import pandas as pd

# Optimized: Configure DuckDB for performance
con = duckdb.connect(database=":memory:")
con.execute("PRAGMA threads=4")  # Optimized: Enable multi-threading
con.execute("PRAGMA memory_limit='1GB'")  # Optimized: Set memory limit

# Optimized: Use context manager with explicit encoding
with open(
    "/home/carloa/Desktop/EasyNer/tests/data/example_pubmed_articles.json",
    "r",
    encoding="utf-8",
) as f:
    data = json.load(f)

# Optimized: Pre-allocate lists with approximate capacity
article_count = len(data)
articles_data = []
articles_data.reserve(article_count) if hasattr(list, "reserve") else None
sentences_data = []
entities_data = []

# Optimized: Create tables with article_id as INTEGER instead of VARCHAR
con.execute("CREATE TABLE articles (article_id INTEGER, title VARCHAR)")
con.execute(
    "CREATE TABLE sentences (article_id INTEGER, sentence_id INTEGER, text VARCHAR)"
)
con.execute(
    "CREATE TABLE entities (article_id INTEGER, sentence_id INTEGER, entity_id INTEGER, entity VARCHAR, start_pos INTEGER, end_pos INTEGER)"
)

# Optimized: Process data in a single pass with list comprehensions where possible
for article_id_str, article_content in data.items():
    # Convert article_id from string to integer
    article_id = int(article_id_str)

    # Articles table
    articles_data.append(
        {"article_id": article_id, "title": article_content.get("title", "")}
    )

    sentences = article_content.get("sentences", [])

    # Process sentences and entities
    for sentence_idx, sentence in enumerate(sentences):
        sentences_data.append(
            {
                "article_id": article_id,
                "sentence_id": sentence_idx,
                "text": sentence.get("text", ""),
            }
        )

        # Optimized: Get entities once
        entities = sentence.get("entities", [])
        entity_spans = sentence.get("entity_spans", [])

        # Optimized: Extend entities_data with all valid entities at once
        entities_data.extend(
            {
                "article_id": article_id,
                "sentence_id": sentence_idx,
                "entity_id": entity_idx,
                "entity": entity,
                "start_pos": span[0] if span else None,
                "end_pos": span[1] if span else None,
            }
            for entity_idx, (entity, span) in enumerate(
                zip(entities, entity_spans)
            )
            if entity  # Skip empty entities
        )

# Optimized: Batch inserts using DataFrames - direct registration approach
articles_df = pd.DataFrame(articles_data)
sentences_df = pd.DataFrame(sentences_data)
entities_df = pd.DataFrame(entities_data)

# Register DataFrames as views in DuckDB
con.register("articles_df", articles_df)
con.register("sentences_df", sentences_df)
con.register("entities_df", entities_df)

# Insert data from the registered views
con.execute("INSERT INTO articles SELECT * FROM articles_df")
con.execute("INSERT INTO sentences SELECT * FROM sentences_df")
con.execute("INSERT INTO entities SELECT * FROM entities_df")


# Optimized: Create indices after data load for better query performance
con.execute("CREATE INDEX idx_article_id ON articles(article_id)")
con.execute("CREATE INDEX idx_sentence_article_id ON sentences(article_id)")
con.execute(
    "CREATE INDEX idx_entity_article_sentence ON entities(article_id, sentence_id)"
)

# Query to verify - no change needed
print(con.execute("SELECT COUNT(*) FROM articles").fetchall())
print(con.execute("SELECT COUNT(*) FROM sentences").fetchall())
print(con.execute("SELECT COUNT(*) FROM entities").fetchall())

# Join queries for analysis - no change needed
result = con.execute(
    """
    SELECT a.article_id, a.title, COUNT(DISTINCT s.sentence_id) AS sentence_count, COUNT(e.entity) AS entity_count
    FROM articles a
    LEFT JOIN sentences s ON a.article_id = s.article_id
    LEFT JOIN entities e ON s.article_id = e.article_id AND s.sentence_id = e.sentence_id
    GROUP BY a.article_id, a.title
    ORDER BY entity_count DESC
"""
).fetchall()

print(result)

# Optimized: Get all DataFrames in a single query
print("Articles DataFrame:")
articles_df = con.execute("SELECT * FROM articles").fetchdf()
print(articles_df)

print("Sentences DataFrame:")
sentences_df = con.execute("SELECT * FROM sentences").fetchdf()
print(sentences_df)

print("Entities DataFrame:")
entities_df = con.execute("SELECT * FROM entities").fetchdf()
print(entities_df)
