# EasyNer Web Interface Guidelines

You are implementing web interface components for the EasyNer system. Your code must provide efficient data visualization and interaction capabilities.

ROLE: Full-Stack Developer
OBJECTIVE: Create performant and maintainable web interfaces for NER data exploration

## Architecture Requirements

IMPLEMENT:
1. Server Components:
   - Flask-based REST API
   - DBDataExchanger integration
   - Connection pooling
   - Request rate limiting

2. Client Components:
   - Modular JavaScript
   - Reusable components
   - Efficient data handling
   - Progressive loading

## Implementation Patterns

### Server-Side Pattern
```python
from typing import Dict, Any
from flask import jsonify, request
from .db_exchanger import DBDataExchanger

def create_data_endpoint(db: DBDataExchanger):
    """Create a data endpoint with proper error handling and validation.

    Args:
        db: Database exchanger instance
    """
    @app.route('/api/entities', methods=['GET'])
    def get_entities():
        try:
            page = int(request.args.get('page', 1))
            limit = min(int(request.args.get('limit', 50)), 1000)

            with db.connection() as conn:
                data = db.get_paginated_entities(conn, page, limit)
                return jsonify({
                    'status': 'success',
                    'data': data,
                    'page': page,
                    'limit': limit
                })
        except ValueError as e:
            return jsonify({
                'status': 'error',
                'message': 'Invalid parameters'
            }), 400
        except Exception as e:
            app.logger.error(f"API error: {str(e)}")
            return jsonify({
                'status': 'error',
                'message': 'Internal server error'
            }), 500
```

### Client-Side Pattern
```javascript
// Reusable data fetching component
class EntityLoader {
    constructor(options = {}) {
        this.pageSize = options.pageSize || 50;
        this.cache = new Map();
    }

    async fetchPage(page) {
        if (this.cache.has(page)) {
            return this.cache.get(page);
        }

        try {
            const response = await fetch(
                `/api/entities?page=${page}&limit=${this.pageSize}`
            );
            const data = await response.json();

            if (data.status === 'success') {
                this.cache.set(page, data.data);
                return data.data;
            }
            throw new Error(data.message);
        } catch (error) {
            console.error(`Failed to fetch page ${page}:`, error);
            throw error;
        }
    }
}
```

## Styling Requirements

SCSS ORGANIZATION:
```scss
// _variables.scss
$primary-color: #007bff;
$secondary-color: #6c757d;
$spacing-unit: 8px;

// _mixins.scss
@mixin flex-center {
    display: flex;
    align-items: center;
    justify-content: center;
}

// components/_data-table.scss
.data-table {
    @include flex-center;
    margin: $spacing-unit * 2;

    &__header {
        font-weight: bold;
    }

    &__row {
        &:hover {
            background-color: rgba($primary-color, 0.1);
        }
    }
}
```

## Performance Guidelines

OPTIMIZE:
1. Data Loading:
   - Implement pagination
   - Cache results
   - Use progressive loading
   - Compress responses

2. UI Responsiveness:
   - Debounce inputs
   - Throttle API calls
   - Use virtual scrolling
   - Implement loading states

3. Resource Usage:
   - Minimize DOM updates
   - Optimize CSS selectors
   - Use connection pooling
   - Implement proper cleanup

## Security Requirements

IMPLEMENT:
1. Input Validation:
   - Sanitize user input
   - Validate query params
   - Check data types
   - Enforce limits

2. Error Handling:
   - Hide internal errors
   - Log security events
   - Rate limit requests
   - Validate sessions