/**
 * SankeyFilterHandler - Manages filters specifically for Sankey diagram visualization
 * Integrates with NumericFilterManager for numeric filter controls
 */
class SankeyFilterHandler {
    constructor() {
        this.form = document.getElementById('filterForm');
        this.sanitizeInputsBeforeSubmit();
        this.setupPercentileFilters();
        this.setupRangeFilters();
    }

    /**
     * Add input sanitization and validation before form submission
     */
    sanitizeInputsBeforeSubmit() {
        if (!this.form) return;

        this.form.addEventListener('submit', (event) => {
            // Get all numeric inputs
            const numericInputs = this.form.querySelectorAll('input[type="number"]');
            let hasError = false;

            numericInputs.forEach(input => {
                // Skip empty fields
                if (!input.value.trim()) return;

                // For min/max pairs, validate min <= max
                if (input.id.startsWith('min_')) {
                    const maxInput = document.getElementById(input.id.replace('min_', 'max_'));
                    if (maxInput && maxInput.value.trim()) {
                        const minVal = parseFloat(input.value);
                        const maxVal = parseFloat(maxInput.value);

                        if (minVal > maxVal) {
                            this.showValidationError(input, 'Minimum value cannot exceed maximum');
                            this.showValidationError(maxInput, 'Maximum value cannot be less than minimum');
                            hasError = true;
                        }
                    }
                }

                // Special validation for NPMI (-1 to 1)
                if (input.id.includes('npmi')) {
                    const val = parseFloat(input.value);
                    if (val < -1 || val > 1) {
                        this.showValidationError(input, 'NPMI must be between -1 and 1');
                        hasError = true;
                    }
                }

                // Validate PMI (can be any number, but typically positive)
                if (input.id.includes('pmi')) {
                    // Just ensure it's a valid number
                    if (isNaN(parseFloat(input.value))) {
                        this.showValidationError(input, 'PMI must be a valid number');
                        hasError = true;
                    }
                }

                // Validate frequencies (must be positive integers)
                if (input.id.includes('_fq')) {
                    const val = parseFloat(input.value);
                    if (isNaN(val) || val < 0 || !Number.isInteger(val)) {
                        this.showValidationError(input, 'Frequency must be a positive integer');
                        hasError = true;
                    }
                }
            });

            if (hasError) {
                event.preventDefault();
                return false;
            }
        });
    }

    /**
     * Display validation error for an input
     */
    showValidationError(input, message) {
        input.classList.add('is-invalid');

        // Look for existing feedback or create new
        let feedback = input.nextElementSibling;
        if (!feedback || !feedback.classList.contains('invalid-feedback')) {
            feedback = document.createElement('div');
            feedback.className = 'invalid-feedback';
            input.parentNode.insertBefore(feedback, input.nextSibling);
        }

        feedback.textContent = message;
    }

    /**
     * Setup percentile shortcuts for common filter patterns
     */
    setupPercentileFilters() {
        const percentileButtons = document.querySelectorAll('.percentile-preset');
        if (!percentileButtons.length) return;

        percentileButtons.forEach(button => {
            button.addEventListener('click', (e) => {
                e.preventDefault();

                const percentile = parseInt(button.dataset.percentile || '0');
                const filterType = button.dataset.filter || '';

                switch (filterType) {
                    case 'npmi':
                        this.setPercentileFilter('npmi', percentile);
                        break;
                    case 'pmi':
                        this.setPercentileFilter('pmi', percentile);
                        break;
                    case 'frequency':
                        this.setPercentileFilter('fq', percentile);
                        break;
                }
            });
        });
    }

    /**
     * Apply a percentile-based filter preset
     */
    setPercentileFilter(filterType, percentile) {
        // Clear existing filters
        document.getElementById(`min_${filterType}`).value = '';
        document.getElementById(`max_${filterType}`).value = '';

        // Set appropriate filter based on percentile
        if (percentile <= 25) {
            // Bottom 25%
            document.getElementById(`max_${filterType}`).value = this.getPercentileValue(filterType, percentile);
        } else if (percentile >= 75) {
            // Top 25%
            document.getElementById(`min_${filterType}`).value = this.getPercentileValue(filterType, percentile);
        } else {
            // Middle range
            document.getElementById(`min_${filterType}`).value = this.getPercentileValue(filterType, 25);
            document.getElementById(`max_${filterType}`).value = this.getPercentileValue(filterType, 75);
        }
    }

    /**
     * Get a percentile value from cached stats if available
     */
    getPercentileValue(filterType, percentile) {
        // If we have NumericFilterManager with cached stats
        if (window.numericFilterManager && window.numericFilterManager.statsCache) {
            const stats = window.numericFilterManager.statsCache.get(filterType);
            if (stats && stats.percentiles) {
                return stats.percentiles[`p${percentile}`] || '';
            }
        }
        return '';
    }

    /**
     * Setup range filter UI interactions
     */
    setupRangeFilters() {
        // Add range slider support if needed
        const rangeSliders = document.querySelectorAll('.range-slider');
        if (!rangeSliders.length) return;

        rangeSliders.forEach(slider => {
            // Initialize noUiSlider or similar here if needed
        });
    }
}

// Initialize when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    window.sankeyFilterHandler = new SankeyFilterHandler();
});