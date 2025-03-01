# EasyNer NER Processing Guidelines

You are implementing Named Entity Recognition (NER) processing components for the EasyNer system. Your code must handle large-scale text processing efficiently.

ROLE: NLP Engineer
OBJECTIVE: Create efficient and accurate NER processing components

## Processing Requirements

IMPLEMENT:
1. Entity Processing:
   - Batch text processing
   - Memory-efficient tokenization
   - Context-aware entity detection
   - Entity relationship tracking

2. Data Validation:
   - Entity boundary verification
   - Context validation
   - Relationship consistency
   - Text normalization checks

## Implementation Pattern

```python
from typing import Generator, List, Dict, Any
from .db_handler import EasyNerDBHandler

class EntityProcessor:
    """Process named entities with efficient resource usage."""

    def __init__(self, db_handler: EasyNerDBHandler, batch_size: int = 1000):
        self.logger = db_handler.logger
        self.db = db_handler
        self.batch_size = batch_size

    def process_documents(self, doc_ids: List[str]) -> Generator[Dict[str, Any], None, None]:
        """Process documents in batches, extracting named entities.

        Args:
            doc_ids: List of document IDs to process

        Yields:
            Dictionaries containing processed entities and relationships

        Raises:
            ProcessingError: If entity extraction fails
        """
        try:
            with self.db.transaction():
                for batch in self._get_doc_batches(doc_ids):
                    self.logger.info(f"Processing batch of {len(batch)} documents")

                    # Process each document in batch
                    for doc_id in batch:
                        try:
                            entities = self._extract_entities(doc_id)
                            yield self._validate_entities(entities, doc_id)

                        except Exception as e:
                            self.logger.error(f"Failed processing doc {doc_id}: {str(e)}")
                            continue

        except Exception as e:
            self.logger.error(f"Batch processing failed: {str(e)}")
            raise

    def _extract_entities(self, doc_id: str) -> List[Dict[str, Any]]:
        """Extract entities from a single document.

        Args:
            doc_id: Document identifier

        Returns:
            List of extracted entities with metadata
        """
        try:
            # Get document text
            doc_text = self._get_document_text(doc_id)

            # Process in memory-efficient manner
            entities = []
            for chunk in self._chunk_text(doc_text):
                chunk_entities = self._process_chunk(chunk)
                entities.extend(self._deduplicate_entities(chunk_entities))

            return entities

        except Exception as e:
            self.logger.error(f"Entity extraction failed for {doc_id}: {str(e)}")
            raise
```

## Performance Guidelines

OPTIMIZE:
1. Text Processing:
   - Use streaming for large texts
   - Implement proper chunking
   - Maintain entity context
   - Handle overlapping entities

2. Memory Management:
   - Process text in chunks
   - Clear temporary data
   - Use generators
   - Implement cleanup

3. Batch Operations:
   - Group similar operations
   - Maintain transaction boundaries
   - Handle partial failures
   - Track progress

## Validation Requirements

VERIFY:
1. Entity Quality:
   - Boundary accuracy
   - Context relevance
   - Relationship validity
   - Text normalization

2. Processing State:
   - Document completion
   - Entity extraction
   - Relationship mapping
   - Error handling

## Error Recovery

IMPLEMENT:
1. Document Level:
   - Save partial results
   - Track failed documents
   - Enable reprocessing
   - Log specific errors

2. Batch Level:
   - Handle partial failures
   - Maintain consistency
   - Enable resumption
   - Track progress

## Environment Variables

USE:
- EASYNER_BATCH_SIZE: Entity processing batch size
- EASYNER_MODEL_PATH: NER model location
- EASYNER_CONTEXT_SIZE: Token context window
- EASYNER_MAX_LENGTH: Maximum text length