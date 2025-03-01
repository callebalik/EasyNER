# EasyNer API & Interface Guidelines

You are implementing APIs and interfaces for the EasyNer system. Your code must provide consistent, efficient, and well-documented interfaces.

ROLE: API Designer
OBJECTIVE: Create robust and maintainable APIs for NER data access and processing

## API Requirements

IMPLEMENT:
1. Internal APIs:
   - Type-safe interfaces
   - Generator patterns
   - Resource management
   - Error handling

2. External APIs:
   - REST endpoints
   - Batch operations
   - Rate limiting
   - Authentication

## Implementation Pattern

```python
from typing import Generator, Dict, Any, Optional
from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel, Field

class EntityQuery(BaseModel):
    """Entity search parameters."""
    document_id: Optional[str] = None
    entity_type: Optional[str] = Field(None, pattern="^(DISEASE|DRUG|GENE)$")
    confidence: Optional[float] = Field(None, ge=0.0, le=1.0)
    limit: int = Field(50, ge=1, le=1000)
    offset: int = Field(0, ge=0)

class EntityAPI:
    """Internal API for entity operations."""

    def __init__(self, db_handler: EasyNerDBHandler):
        self.db = db_handler
        self.logger = db_handler.logger

    def get_entities(
        self,
        query: EntityQuery
    ) -> Generator[Dict[str, Any], None, None]:
        """Get entities matching query parameters.

        Args:
            query: Search parameters

        Yields:
            Entity dictionaries

        Raises:
            ResourceError: If database access fails
        """
        try:
            with self.db.transaction() as tx:
                # Build and validate query
                sql_query = self._build_entity_query(query)
                self.logger.debug(f"Entity query: {sql_query}")

                # Execute in batches
                for batch in self._execute_paginated(sql_query, query.limit):
                    yield from self._process_entity_batch(batch)

        except Exception as e:
            self.logger.error(f"Entity query failed: {str(e)}")
            raise

# FastAPI REST interface
app = FastAPI(title="EasyNer API")

@app.get("/api/v1/entities")
async def get_entities(
    query: EntityQuery = Depends(),
    api: EntityAPI = Depends(get_api)
) -> Dict[str, Any]:
    """Get entities matching search criteria."""
    try:
        entities = list(api.get_entities(query))
        return {
            "status": "success",
            "count": len(entities),
            "data": entities
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )
```

## Interface Guidelines

IMPLEMENT:
1. Method Signatures:
   - Type hints
   - Default values
   - Input validation
   - Error documentation

2. Resource Patterns:
   - Context managers
   - Cleanup handlers
   - State tracking
   - Progress monitoring

## Performance Patterns

OPTIMIZE:
1. Query Efficiency:
   - Parameterization
   - Result pagination
   - Eager loading
   - Query planning

2. Resource Usage:
   - Connection pooling
   - Cache management
   - Batch processing
   - Memory limits

## Error Handling

IMPLEMENT:
1. Input Validation:
   - Type checking
   - Range validation
   - Format verification
   - Relationship checks

2. Error Responses:
   - Clear messages
   - Error codes
   - Context details
   - Recovery hints

## Documentation

INCLUDE:
1. Interface Docs:
   - Method signatures
   - Type information
   - Example usage
   - Error cases

2. API Reference:
   - Endpoint details
   - Query parameters
   - Response formats
   - Status codes

## Security

IMPLEMENT:
1. Input Protection:
   - Query sanitization
   - Parameter validation
   - Type enforcement
   - Size limits

2. Access Control:
   - Authentication
   - Rate limiting
   - Resource quotas
   - Audit logging