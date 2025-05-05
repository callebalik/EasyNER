from typing import Dict, List, Tuple
import pandas as pd
from easyner.io.handlers.json_handler import JsonHandler
import logging

# Initialize logger
logger = logging.getLogger(__name__)


class PubMedJsonHandler(JsonHandler):
    """
    Specialized JsonHandler for PubMed data with methods to extract articles, sentences, and entities.
    """

    def __init__(self, encoding="utf-8"):
        """Initialize the PubMed JSON handler with encoding"""
        super().__init__(encoding=encoding)

    def _process_article(self, article_id: str, article_data: Dict) -> Dict:
        """Extract article information from article data with optimized memory usage"""
        # Pre-allocate dictionary with common fields
        article = {
            "article_id": article_id,
            "title": article_data.get("title", ""),
            "abstract": article_data.get("abstract", ""),
        }

        # Add metadata selectively to avoid dictionary resizing
        metadata = article_data.get("metadata", {})
        if metadata:
            for key, value in metadata.items():
                if isinstance(value, (str, int, float, bool)):
                    article[key] = value

        return article

    def _process_sentence(
        self, article_id: str, sent_idx: int, sentence_data: Dict
    ) -> Dict:
        """Extract sentence information from sentence data"""
        # Use integer position for proper ordering
        sentence = {
            "sentence_id": int(sent_idx),  # Integer ID within article
            "article_id": article_id,  # PMID
            "position": sent_idx,  # Order within article
            "text": sentence_data.get("text", ""),
        }

        # Add tokens if available
        tokens = sentence_data.get("tokens", [])
        if tokens:
            sentence["tokens"] = tokens

        return sentence

    def _process_entities(
        self, article_id: str, sent_idx: int, sentence_data: Dict
    ) -> List[Dict]:
        """Extract entity information from sentence data"""
        entities = []
        entity_list = sentence_data.get("entities", [])
        if not entity_list:
            return entities

        entity_spans = sentence_data.get("entity_spans", [])

        # Skip entirely if there are entities but no spans
        if not entity_spans:
            logger.warning(
                f"Skipping entities in article {article_id}, sentence {sent_idx}: "
                f"Found {len(entity_list)} entities but no entity spans"
            )
            return entities

        # Check if entity list and spans have mismatched lengths
        if len(entity_list) > len(entity_spans):
            logger.warning(
                f"Mismatched entity data in article {article_id}, sentence {sent_idx}: "
                f"Found {len(entity_list)} entities but only {len(entity_spans)} spans"
            )

        # Get these just once per sentence
        entity_ids = sentence_data.get("ids", [])
        entity_names = sentence_data.get("names", [])

        # Process all entities in one batch
        for ent_idx, (entity_text, span) in enumerate(
            zip(entity_list, entity_spans)
        ):
            if not entity_text:
                logger.warning(
                    f"Empty entity text at position {ent_idx} in article {article_id}, "
                    f"sentence {sent_idx} - skipping"
                )
                continue

            # Build entity dict in one operation to minimize dict creations
            entity = {
                "entity_id": ent_idx,  # Integer ID within sentence
                "sentence_id": int(sent_idx),  # Reference to parent sentence
                "article_id": article_id,  # PMID
                "text": entity_text,
                "start_char": span[0] if len(span) > 0 else None,
                "end_char": span[1] if len(span) > 1 else None,
            }

            # Add entity ID and name only if available
            if ent_idx < len(entity_ids):
                entity["entity_id_value"] = entity_ids[ent_idx]
            if ent_idx < len(entity_names):
                entity["entity_name"] = entity_names[ent_idx]

            entities.append(entity)

        return entities

    def extract_all_dataframes(
        self, data: Dict
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        Extract articles, sentences, and entities from PubMed JSON in a single pass

        Args:
            data: Loaded JSON data

        Returns:
            Tuple of (articles_df, sentences_df, entities_df)
        """
        articles = []
        sentences = []
        entities = []

        # Process all data in a single pass
        for article_id, article_data in data.items():
            # Process article
            article = self._process_article(article_id, article_data)
            articles.append(article)

            # Process sentences and entities
            for sent_idx, sentence_data in enumerate(
                article_data.get("sentences", [])
            ):
                # Process sentence
                sentence = self._process_sentence(
                    article_id, sent_idx, sentence_data
                )
                sentences.append(sentence)

                # Process entities
                sent_entities = self._process_entities(
                    article_id, sent_idx, sentence_data
                )
                entities.extend(sent_entities)

        # Create all DataFrames at once
        return (
            pd.DataFrame(articles),
            pd.DataFrame(sentences),
            pd.DataFrame(entities),
        )

    def extract_articles_dataframe(self, data: Dict) -> pd.DataFrame:
        """
        Extract article information from PubMed JSON into a dataframe

        Args:
            data: Loaded JSON data

        Returns:
            DataFrame with article information
        """
        articles_df, _, _ = self.extract_all_dataframes(data)
        return articles_df

    def extract_sentences_dataframe(self, data: Dict) -> pd.DataFrame:
        """
        Extract sentence information from PubMed JSON into a dataframe

        Args:
            data: Loaded JSON data

        Returns:
            DataFrame with sentence information
        """
        _, sentences_df, _ = self.extract_all_dataframes(data)
        return sentences_df

    def extract_entities_dataframe(self, data: Dict) -> pd.DataFrame:
        """
        Extract entity information from PubMed JSON into a dataframe

        Args:
            data: Loaded JSON data

        Returns:
            DataFrame with entity information
        """
        _, _, entities_df = self.extract_all_dataframes(data)
        return entities_df


# %%
