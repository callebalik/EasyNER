# This file is now deprecated, please use the class-based implementation:
# from easyner.io.converters.json_to_duck_converter import JsonToDuckConverter

import json
from pathlib import Path

# Import our new database utility functions
from easyner.io.database.db_utils import (
    initialize_db,
    create_tables,
    insert_data,
    create_indices,
    get_table,
    get_table_count,
    export_to_csv,
    get_article_entity_stats,
)


def main():
    """Legacy function demonstrating how to use the converter"""
    # Optimized: Use context manager with explicit encoding
    BASE_DIR = Path("/home/carloa/Desktop/EasyNer/tests/database/")

    ARTICLES_TEST = BASE_DIR / "example_pubmed_articles.json"
    with open(
        ARTICLES_TEST,
        "r",
        encoding="utf-8",
    ) as f:
        data = json.load(f)

    # Initialize database connection
    con = initialize_db()

    # Create database tables
    create_tables(con)

    # Optimized: Pre-allocate lists with approximate capacity
    article_count = len(data)
    articles_data = []
    articles_data.reserve(article_count) if hasattr(list, "reserve") else None
    sentences_data = []
    entities_data = []

    # Process data in a single pass with list comprehensions where possible
    for article_id_str, article_content in data.items():
        # Convert article_id from string to integer
        article_id = int(article_id_str)

        # Articles table
        articles_data.append(
            {
                "article_id": article_id,
                "title": article_content.get("title", ""),
            }
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
                    "entity": entity,
                    "start_pos": span[0] if span else None,
                    "end_pos": span[1] if span else None,
                    "inference_model": None,
                    "inference_model_metadata": None,
                }
                for entity, span in zip(entities, entity_spans)
                if entity  # Skip empty entities
            )

    # Insert data into database
    insert_data(con, articles_data, sentences_data, entities_data)

    # Create indices for better performance
    create_indices(con)

    # Query to verify - no change needed
    print(f"Articles count: {get_table_count(con, 'articles')}")
    print(f"Sentences count: {get_table_count(con, 'sentences')}")
    print(f"Entities count: {get_table_count(con, 'entities')}")

    # Get article entity statistics
    result = get_article_entity_stats(con)
    print(result)

    # Optimized: Get all DataFrames
    print("Articles DataFrame:")
    articles_df = get_table(con, "articles")
    print(articles_df)

    print("Sentences DataFrame:")
    sentences_df = get_table(con, "sentences")
    print(sentences_df)

    print("Entities DataFrame:")
    entities_df = get_table(con, "entities")
    print(entities_df)

    # Export data to CSV
    ARTICLES_CSV = BASE_DIR / "articles_output.csv"
    SENTENCES_CSV = BASE_DIR / "sentences_output.csv"
    ENTITIES_CSV = BASE_DIR / "entities_output.csv"

    export_to_csv(con, "articles", ARTICLES_CSV)
    export_to_csv(con, "sentences", SENTENCES_CSV)
    export_to_csv(con, "entities", ENTITIES_CSV)


if __name__ == "__main__":
    # Show deprecation warning
    import warnings

    warnings.warn(
        "This script is deprecated. Please use JsonToDuckConverter class instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    main()
