# EasyNer Visualization & Analysis Guidelines

You are implementing data visualization and analysis components for the EasyNer system. Your code must efficiently present NER data and relationships.

ROLE: Data Visualization Engineer
OBJECTIVE: Create efficient and informative data visualizations

## Data Requirements

IMPLEMENT:
1. Entity Visualization:
   - Highlight entity boundaries
   - Show entity relationships
   - Display confidence scores
   - Present context windows

2. Analysis Views:
   - Entity frequency analysis
   - Relationship networks
   - Co-occurrence patterns
   - Distribution graphs

## Implementation Pattern

```javascript
class EntityVisualizer {
    constructor(options = {}) {
        this.container = options.container;
        this.config = {
            highlightColor: options.highlightColor || '#ffeb3b',
            entityColors: options.entityColors || {
                DISEASE: '#f44336',
                DRUG: '#2196f3',
                GENE: '#4caf50'
            },
            contextSize: options.contextSize || 50
        };
    }

    async visualizeEntities(documentId) {
        try {
            // Fetch document and entities
            const [text, entities] = await Promise.all([
                this.fetchDocumentText(documentId),
                this.fetchDocumentEntities(documentId)
            ]);

            // Create visualization
            const visualization = this.createVisualization(text, entities);
            this.container.innerHTML = visualization;

            // Add interactive features
            this.addEntityHighlighting();
            this.addRelationshipLines();
            this.addTooltips();

        } catch (error) {
            console.error('Visualization failed:', error);
            this.showError('Failed to load visualization');
        }
    }

    createVisualization(text, entities) {
        // Implementation with proper memory management
        const fragments = [];
        let lastIndex = 0;

        // Sort entities by start position
        entities.sort((a, b) => a.start - b.start);

        for (const entity of entities) {
            // Add text before entity
            if (entity.start > lastIndex) {
                fragments.push(this.escapeHtml(text.slice(lastIndex, entity.start)));
            }

            // Add highlighted entity
            fragments.push(
                `<span class="entity ${entity.type.toLowerCase()}"
                       data-entity-id="${entity.id}"
                       data-confidence="${entity.confidence}">
                    ${this.escapeHtml(text.slice(entity.start, entity.end))}
                </span>`
            );

            lastIndex = entity.end;
        }

        // Add remaining text
        if (lastIndex < text.length) {
            fragments.push(this.escapeHtml(text.slice(lastIndex)));
        }

        return fragments.join('');
    }
}
```

## SCSS Styling Pattern

```scss
// _entity-visualization.scss
@use 'sass:color';

// Entity highlighting
.entity {
    padding: 2px 4px;
    border-radius: 3px;
    cursor: pointer;
    transition: opacity 0.2s;

    &:hover {
        opacity: 0.8;
    }

    // Entity types
    &.disease {
        background-color: rgba($disease-color, 0.2);
        border-bottom: 2px solid $disease-color;
    }

    &.drug {
        background-color: rgba($drug-color, 0.2);
        border-bottom: 2px solid $drug-color;
    }

    &.gene {
        background-color: rgba($gene-color, 0.2);
        border-bottom: 2px solid $gene-color;
    }
}

// Relationship visualization
.relationship-line {
    position: absolute;
    pointer-events: none;
    z-index: 1;

    path {
        fill: none;
        stroke: $relationship-color;
        stroke-width: 1.5;
        stroke-dasharray: 4;
    }
}
```

## Performance Guidelines

OPTIMIZE:
1. Rendering:
   - Virtual scrolling
   - Progressive loading
   - Canvas for large graphs
   - Element recycling

2. Data Loading:
   - Batch requests
   - Client caching
   - Incremental updates
   - Lazy relationship loading

## Analysis Features

IMPLEMENT:
1. Entity Statistics:
   - Frequency charts
   - Distribution graphs
   - Co-occurrence matrices
   - Confidence histograms

2. Network Analysis:
   - Relationship graphs
   - Centrality measures
   - Cluster visualization
   - Path analysis

## Interaction Patterns

SUPPORT:
1. User Actions:
   - Entity highlighting
   - Relationship tracing
   - Context expansion
   - Filter application

2. Data Exploration:
   - Zoom controls
   - Pan navigation
   - Detail views
   - Search highlighting

## Resource Management

OPTIMIZE:
1. Memory Usage:
   - DOM element recycling
   - Event delegation
   - Image optimization
   - Canvas layer management

2. Performance:
   - Throttle updates
   - Debounce interactions
   - Batch DOM updates
   - Use RequestAnimationFrame