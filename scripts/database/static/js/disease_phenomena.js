/**
 * Manages the disease-phenomena visualization interface
 * Handles filter validation, form submission, and UI interactions
 */
class DiseasePhenomenaManager {
    /**
     * Initialize the manager with optional configuration
     * @param {Object} options - Configuration options
     * @param {Object} options.defaultValues - Default filter values
     * @param {string} options.formId - ID of the filter form
     * @param {string} options.sankeyContainerId - ID of the Sankey diagram container
     * @param {string} options.loadingIndicatorId - ID of the loading indicator
     * @param {string} options.infoPanelId - ID of the info panel
     * @param {string} options.apiEndpoint - API endpoint for data
     */
    constructor(options = {}) {
        // Use server-provided defaults from options
        this.defaults = options.defaultValues || {
            min_pmi: 5.0,  // Fallback only if server values aren't available
            min_npmi: 0,
            min_fq: 1,
            limit: 30
        };

        // Store element IDs
        this.formId = options.formId || 'filterForm';
        this.sankeyContainerId = options.sankeyContainerId || 'sankeyContainer';
        this.loadingIndicatorId = options.loadingIndicatorId || 'loadingIndicator';
        this.infoPanelId = options.infoPanelId || 'infoPanel';
        this.apiEndpoint = options.apiEndpoint || '/api/disease-phenomena';

        // Initialize elements
        this.form = document.getElementById(this.formId);
        this.sankeyContainer = document.getElementById(this.sankeyContainerId);
        this.loadingIndicator = document.getElementById(this.loadingIndicatorId);
        this.infoPanel = document.getElementById(this.infoPanelId);

        // Current zoom level for the diagram
        this.zoomLevel = 1;

        // Initialize
        this.setupEventListeners();
        this.setupPlotlyEvents();

        // Log initialization
        console.log('DiseasePhenomenaManager initialized with defaults:', this.defaults);
    }

    /**
     * Set up event listeners for form elements
     */
    setupEventListeners() {
        // PMI input validation
        const pmiInputs = ['min_pmi', 'max_pmi'];
        pmiInputs.forEach(id => {
            const input = document.getElementById(id);
            if (input) {
                input.addEventListener('input', () => this.validateNumericInput(input, -Infinity, Infinity));
            }
        });

        // NPMI input validation (-1 to 1)
        const npmiInputs = ['min_npmi', 'max_npmi'];
        npmiInputs.forEach(id => {
            const input = document.getElementById(id);
            if (input) {
                input.addEventListener('input', () => this.validateNumericInput(input, -1, 1));
            }
        });

        // Frequency input validation (non-negative integers)
        const fqInputs = ['min_fq', 'max_fq'];
        fqInputs.forEach(id => {
            const input = document.getElementById(id);
            if (input) {
                input.addEventListener('input', () => this.validateNumericInput(input, 0, Infinity, true));
            }
        });

        // Limit input validation (1-200)
        const limitInput = document.getElementById('limit');
        if (limitInput) {
            limitInput.addEventListener('input', () => this.validateNumericInput(limitInput, 1, 200, true));
        }

        // Form submission handler
        if (this.form) {
            this.form.addEventListener('submit', (e) => {
                // Only submit if all fields are valid
                const invalidInputs = this.form.querySelectorAll('.is-invalid');
                if (invalidInputs.length > 0) {
                    e.preventDefault();
                    this.showAlert('Please correct the invalid filter values.', 'warning');
                } else {
                    this.showLoadingIndicator();
                }
            });
        }
    }

    /**
     * Set up Plotly-specific event handlers for the Sankey diagram
     */
    setupPlotlyEvents() {
        // Check if we have a Plotly figure in the document
        if (this.sankeyContainer && window.Plotly) {
            try {
                // Find the Plotly graph div by class - it's dynamically generated
                const plotlyDiv = this.sankeyContainer.querySelector('.plotly-graph-div');

                if (plotlyDiv) {
                    // Add click handler for nodes and links
                    plotlyDiv.on('plotly_click', (data) => {
                        this.handleDiagramClick(data);
                    });

                    // Add hover handler for tooltips
                    plotlyDiv.on('plotly_hover', (data) => {
                        // Enhanced tooltips logic could be added here if needed
                        console.log('Hover data:', data);
                    });
                }
            } catch (error) {
                console.error('Error setting up Plotly events:', error);
            }
        }
    }

    /**
     * Handle click events on the Sankey diagram
     * @param {Object} eventData - Plotly click event data
     */
    handleDiagramClick(eventData) {
        if (!eventData || !eventData.points || !eventData.points[0]) {
            return;
        }

        const point = eventData.points[0];
        let content = '';

        if (point.pointNumber !== undefined) {
            // Node click
            const nodeName = point.label;
            const nodeType = point.pointNumber < point.node.label.length / 2 ? 'Disease' : 'Phenomenon';

            // Get node metadata from customdata if available
            const metadata = point.customdata || [];
            const docFrequency = metadata[0] || 'Unknown';
            const uniqueDocs = metadata[1] || 'Unknown';

            content = `
                <div class="entity-details">
                    <h5>${nodeName}</h5>
                    <div class="entity-type ${nodeType.toLowerCase()}">${nodeType}</div>
                    <table class="table table-sm">
                        <tr>
                            <th>Document Frequency:</th>
                            <td>${docFrequency}</td>
                        </tr>
                        <tr>
                            <th>Unique Documents:</th>
                            <td>${uniqueDocs}</td>
                        </tr>
                    </table>
                    <button class="btn btn-sm btn-outline-primary entity-filter"
                            data-type="${nodeType.toLowerCase()}"
                            data-name="${nodeName}">
                        Filter by this ${nodeType}
                    </button>
                </div>
            `;
        } else if (point.link !== undefined) {
            // Link click
            const source = point.source.label;
            const target = point.target.label;
            const value = point.value;

            content = `
                <div class="relationship-details">
                    <h5>Relationship</h5>
                    <div class="relationship-entities">
                        <span class="entity disease">${source}</span>
                        <span class="connector">→</span>
                        <span class="entity phenomenon">${target}</span>
                    </div>
                    <table class="table table-sm">
                        <tr>
                            <th>NPMI:</th>
                            <td>${value.toFixed(3)}</td>
                        </tr>
                    </table>
                    <div class="btn-group btn-group-sm">
                        <button class="btn btn-outline-primary entity-filter"
                                data-type="disease"
                                data-name="${source}">
                            Filter by Disease
                        </button>
                        <button class="btn btn-outline-primary entity-filter"
                                data-type="phenomenon"
                                data-name="${target}">
                            Filter by Phenomenon
                        </button>
                    </div>
                </div>
            `;
        }

        if (content) {
            this.showInfoPanel(content);

            // Add event listeners for filter buttons in the info panel
            setTimeout(() => {
                const filterButtons = document.querySelectorAll('.entity-filter');
                filterButtons.forEach(button => {
                    button.addEventListener('click', (e) => {
                        const type = e.target.dataset.type;
                        const name = e.target.dataset.name;
                        this.applyEntityFilter(type, name);
                    });
                });
            }, 10);
        }
    }

    /**
     * Apply entity filter by setting the appropriate form field and submitting
     * @param {string} type - Entity type (disease or phenomenon)
     * @param {string} name - Entity name
     */
    applyEntityFilter(type, name) {
        if (!this.form) return;

        if (type === 'disease') {
            const input = document.getElementById('disease_search');
            if (input) {
                input.value = name;
                // Clear the other search to focus on just this entity
                const phenoInput = document.getElementById('phenomenon_search');
                if (phenoInput) phenoInput.value = '';
                this.form.submit();
            }
        } else if (type === 'phenomenon') {
            const input = document.getElementById('phenomenon_search');
            if (input) {
                input.value = name;
                // Clear the other search to focus on just this entity
                const diseaseInput = document.getElementById('disease_search');
                if (diseaseInput) diseaseInput.value = '';
                this.form.submit();
            }
        }
    }

    /**
     * Validate a numeric input field
     * @param {HTMLInputElement} input - Input element to validate
     * @param {number} min - Minimum allowed value
     * @param {number} max - Maximum allowed value
     * @param {boolean} requireInteger - Whether to require integer values
     */
    validateNumericInput(input, min, max, requireInteger = false) {
        if (!input.value.trim()) return;

        let value = parseFloat(input.value);
        let isValid = !isNaN(value) && value >= min && value <= max;
        if (requireInteger) {
            isValid = isValid && Number.isInteger(value);
        }

        input.classList.toggle('is-invalid', !isValid);

        // Enhanced validation for pairs of inputs (min/max)
        if (isValid && (input.id.startsWith('min_') || input.id.startsWith('max_'))) {
            const prefix = input.id.split('_')[0];
            const fieldName = input.id.split('_')[1];
            const oppositeId = (prefix === 'min' ? 'max_' : 'min_') + fieldName;
            const oppositeInput = document.getElementById(oppositeId);

            if (oppositeInput && oppositeInput.value.trim()) {
                const oppositeValue = parseFloat(oppositeInput.value);
                if (!isNaN(oppositeValue)) {
                    const minInput = prefix === 'min' ? input : oppositeInput;
                    const maxInput = prefix === 'max' ? input : oppositeInput;
                    const minValue = parseFloat(minInput.value);
                    const maxValue = parseFloat(maxInput.value);

                    if (minValue > maxValue) {
                        minInput.classList.add('is-invalid');
                        maxInput.classList.add('is-invalid');
                    }
                }
            }
        }
    }

    /**
     * Reset all filters to default values and redirect
     */
    resetFilters() {
        this.showLoadingIndicator();
        // Redirect to base URL without parameters
        window.location.href = '/disease-phenomena';
    }

    /**
     * Show the loading indicator
     */
    showLoadingIndicator() {
        if (this.loadingIndicator) {
            this.loadingIndicator.classList.add('visible');
        }
    }

    /**
     * Hide the loading indicator
     */
    hideLoadingIndicator() {
        if (this.loadingIndicator) {
            this.loadingIndicator.classList.remove('visible');
        }
    }

    /**
     * Show the info panel with the provided content
     * @param {string} content - HTML content for the panel
     */
    showInfoPanel(content) {
        if (!this.infoPanel) return;

        // Find content container in panel
        const contentContainer = this.infoPanel.querySelector('.info-panel-content');
        if (contentContainer) {
            contentContainer.innerHTML = content;
        }

        // Make panel visible
        this.infoPanel.classList.add('visible');
    }

    /**
     * Hide the info panel
     */
    hideInfoPanel() {
        if (this.infoPanel) {
            this.infoPanel.classList.remove('visible');
        }
    }

    /**
     * Show an alert message to the user
     * @param {string} message - Message to display
     * @param {string} type - Alert type (success, info, warning, danger)
     */
    showAlert(message, type = 'info') {
        // Create alert container if it doesn't exist
        let alertContainer = document.querySelector('.alert-container');
        if (!alertContainer) {
            alertContainer = document.createElement('div');
            alertContainer.className = 'alert-container';
            if (this.sankeyContainer) {
                this.sankeyContainer.parentNode.insertBefore(alertContainer, this.sankeyContainer);
            } else {
                document.body.prepend(alertContainer);
            }
        }

        // Create the alert
        const alert = document.createElement('div');
        alert.className = `alert alert-${type} alert-dismissible fade show`;
        alert.innerHTML = `
            ${message}
            <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
        `;

        // Add to container
        alertContainer.appendChild(alert);

        // Auto-remove after 5 seconds
        setTimeout(() => {
            alert.classList.remove('show');
            setTimeout(() => alert.remove(), 300);
        }, 5000);
    }

    /**
     * Download the current data as a CSV file
     */
    downloadData() {
        // Build query string from current form values
        const formData = new FormData(this.form);
        const params = new URLSearchParams();
        for (const [key, value] of formData.entries()) {
            if (value.trim()) params.append(key, value);
        }

        // Show loading
        this.showLoadingIndicator();

        // Fetch data from the API
        fetch(`${this.apiEndpoint}?${params.toString()}`)
            .then(response => {
                if (!response.ok) throw new Error('Network response was not ok');
                return response.json();
            })
            .then(data => {
                if (!data.data || !data.data.length) {
                    this.showAlert('No data available to download', 'warning');
                    return;
                }

                // Convert to CSV
                const headers = ['disease', 'phenomenon', 'npmi', 'pmi', 'fq_doc_level', 'uniq_docs'];
                let csv = headers.join(',') + '\n';

                data.data.forEach(row => {
                    const values = headers.map(header => {
                        const value = row[header];
                        // Handle fields that might contain commas
                        if (typeof value === 'string' && value.includes(',')) {
                            return `"${value}"`;
                        }
                        return value !== undefined ? value : '';
                    });
                    csv += values.join(',') + '\n';
                });

                // Create download link
                const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
                const url = URL.createObjectURL(blob);
                const link = document.createElement('a');
                link.setAttribute('href', url);
                link.setAttribute('download', 'disease_phenomena_data.csv');
                link.style.visibility = 'hidden';
                document.body.appendChild(link);
                link.click();
                document.body.removeChild(link);

                this.showAlert('Data downloaded successfully', 'success');
            })
            .catch(error => {
                console.error('Error downloading data:', error);
                this.showAlert('Error downloading data: ' + error.message, 'danger');
            })
            .finally(() => {
                this.hideLoadingIndicator();
            });
    }

    /**
     * Zoom in the Sankey diagram
     */
    zoomIn() {
        if (!this.sankeyContainer) return;

        const plotlyContainer = this.sankeyContainer.querySelector('.plotly-container');
        if (!plotlyContainer) return;

        this.zoomLevel = Math.min(this.zoomLevel + 0.1, 2);
        this.applyZoom(plotlyContainer);
    }

    /**
     * Zoom out the Sankey diagram
     */
    zoomOut() {
        if (!this.sankeyContainer) return;

        const plotlyContainer = this.sankeyContainer.querySelector('.plotly-container');
        if (!plotlyContainer) return;

        this.zoomLevel = Math.max(this.zoomLevel - 0.1, 0.5);
        this.applyZoom(plotlyContainer);
    }

    /**
     * Reset zoom to default level
     */
    resetZoom() {
        if (!this.sankeyContainer) return;

        const plotlyContainer = this.sankeyContainer.querySelector('.plotly-container');
        if (!plotlyContainer) return;

        this.zoomLevel = 1;
        this.applyZoom(plotlyContainer);
    }

    /**
     * Apply zoom transform to the Plotly container
     * @param {HTMLElement} container - The container to apply zoom to
     */
    applyZoom(container) {
        const plotlyDiv = container.querySelector('.plotly-graph-div');
        if (plotlyDiv) {
            plotlyDiv.style.transform = `scale(${this.zoomLevel})`;
            plotlyDiv.style.transformOrigin = 'center top';

            // Adjust height to accommodate zoom
            const originalHeight = 500; // Base height in pixels
            plotlyDiv.style.height = `${originalHeight * this.zoomLevel}px`;
        }
    }
}