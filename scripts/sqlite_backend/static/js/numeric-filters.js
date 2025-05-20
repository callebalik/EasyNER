class NumericFilterManager {
    constructor() {
        this.charts = new Map(); // Store chart instances by column name
        this.statsCache = new Map(); // Cache statistics data by column
        this.initializeFilters();
    }

    initializeFilters() {
        document.querySelectorAll('.numeric-filter').forEach(filterElement => {
            const columnName = filterElement.querySelector('select').name.replace('_op', '');
            this.setupFilter(columnName);
        });
    }

    setupFilter(columnName) {
        const filterElement = document.querySelector(`[name="${columnName}_op"]`).closest('.numeric-filter');
        const dropdown = filterElement.querySelector('.numeric-filter-dropdown');

        // Setup filter toggling
        filterElement.querySelector('.filter-toggle').addEventListener('click', async (event) => {
            event.stopPropagation();
            if (dropdown.style.display !== 'block') {
                dropdown.style.display = 'block';
                await this.loadColumnStats(columnName);
            } else {
                dropdown.style.display = 'none';
            }
        });

        // Setup operation type change handler
        const opSelect = filterElement.querySelector(`#${columnName}_op`);
        opSelect.addEventListener('change', () => this.handleOperationChange(columnName));

        // Close dropdown when clicking outside
        document.addEventListener('click', (event) => {
            if (!filterElement.contains(event.target)) {
                dropdown.style.display = 'none';
            }
        });

        // Add keyboard support
        const val1Input = document.getElementById(`${columnName}_val1`);
        const val2Input = document.getElementById(`${columnName}_val2`);

        [val1Input, val2Input].forEach(input => {
            if (input) {
                input.addEventListener('keydown', (event) => {
                    if (event.key === 'Enter') {
                        event.preventDefault();
                        this.applyFilter(columnName);
                    }
                });
            }
        });

        // Setup percentile card clicks
        dropdown.addEventListener('click', (event) => {
            const percentileCard = event.target.closest('.percentile-card');
            if (percentileCard) {
                this.handlePercentileCardClick(columnName, percentileCard);
            }
        });
    }

    async loadColumnStats(columnName) {
        try {
            // Check if we have cached data
            if (this.statsCache.has(columnName)) {
                this.updateUIWithStats(columnName, this.statsCache.get(columnName));
                return;
            }

            const statsContainer = document.querySelector(`#${columnName}_op`)
                .closest('.numeric-filter-dropdown')
                .querySelector('.stats-section');

            // Show loading states
            this.showLoadingState(statsContainer);

            const tableName = document.querySelector('h1').textContent.split(' ')[0];
            const response = await fetch(`/api/column/stats/${tableName}/${columnName}`);
            const stats = await response.json();

            if (!stats || stats.error || stats.status === 'error') {
                console.error('Error loading statistics:', stats?.error || 'Unknown error');
                this.showError(statsContainer, 'Error loading statistics');
                return;
            }

            if (stats.status === 'empty' || stats.count === 0) {
                this.showEmpty(statsContainer);
                return;
            }

            // Cache the results
            this.statsCache.set(columnName, stats);
            this.updateUIWithStats(columnName, stats);

        } catch (error) {
            console.error('Error loading column statistics:', error);
            const statsContainer = document.querySelector(`#${columnName}_op`)
                .closest('.numeric-filter-dropdown')
                .querySelector('.stats-section');
            this.showError(statsContainer, 'Error loading statistics');
        }
    }

    showLoadingState(container) {
        const statsGrid = container.querySelector('.stats-grid');
        const chartArea = container.querySelector('.distribution-chart');
        const percentileGrid = container.querySelector('.percentile-grid');

        // Add loading indicators to stat cards
        if (statsGrid) {
            statsGrid.querySelectorAll('.stat-card').forEach(card => {
                const value = card.querySelector('.stat-value');
                value.innerHTML = '<div class="loading-indicator text"></div>';
            });
        }

        // Add loading indicator to chart area
        if (chartArea) {
            chartArea.innerHTML = '<div class="loading-indicator stats"></div>';
        }

        // Add loading indicators to percentile grid
        if (percentileGrid) {
            percentileGrid.innerHTML = '';
            for (let i = 0; i < 8; i++) {
                const card = document.createElement('div');
                card.className = 'percentile-card';
                card.innerHTML = '<div class="loading-indicator text"></div>';
                percentileGrid.appendChild(card);
            }
        }
    }

    showError(container, message) {
        container.innerHTML = `
            <div class="error-message">
                <i class="fas fa-exclamation-circle"></i>
                <span>${message}</span>
            </div>
        `;
    }

    showEmpty(container) {
        container.innerHTML = `
            <div class="empty-message">
                <i class="fas fa-info-circle"></i>
                <span>No numeric data available for this column</span>
            </div>
        `;
    }

    updateUIWithStats(columnName, stats) {
        // Update basic statistics
        document.getElementById(`${columnName}_min`).textContent = this.formatNumber(stats.min);
        document.getElementById(`${columnName}_max`).textContent = this.formatNumber(stats.max);
        document.getElementById(`${columnName}_avg`).textContent = this.formatNumber(stats.avg);
        document.getElementById(`${columnName}_count`).textContent = this.formatNumber(stats.count);

        // Add value bars to the stat cards
        this.addValueBarsToStats(columnName, stats);

        // Update or create chart
        this.updateChart(columnName, stats);

        // Update percentile grid
        this.updatePercentileGrid(columnName, stats.percentiles);
    }

    addValueBarsToStats(columnName, stats) {
        // Add value bars to min/max/avg cards
        const min = stats.min;
        const max = stats.max;
        const range = max - min;

        // Only add bars if we have a valid range
        if (range <= 0) return;

        const minCard = document.getElementById(`${columnName}_min`).closest('.stat-card');
        const maxCard = document.getElementById(`${columnName}_max`).closest('.stat-card');
        const avgCard = document.getElementById(`${columnName}_avg`).closest('.stat-card');

        // Min card (0% fill)
        this.createValueBar(minCard, 0);

        // Max card (100% fill)
        this.createValueBar(maxCard, 100);

        // Average card (calculate percentage based on position in range)
        const avgPercent = ((stats.avg - min) / range) * 100;
        this.createValueBar(avgCard, avgPercent);
    }

    createValueBar(container, fillPercent) {
        // Check if value bar already exists
        let valueBar = container.querySelector('.value-bar');
        if (!valueBar) {
            valueBar = document.createElement('div');
            valueBar.className = 'value-bar';

            const fill = document.createElement('div');
            fill.className = 'fill';
            valueBar.appendChild(fill);

            container.appendChild(valueBar);
        }

        // Update the fill percentage
        const fill = valueBar.querySelector('.fill');
        fill.style.width = `${Math.max(0, Math.min(100, fillPercent))}%`;
    }

    updatePercentileGrid(columnName, percentiles) {
        const gridElement = document.querySelector(`#${columnName}_op`).closest('.numeric-filter-dropdown').querySelector('.percentile-grid');

        // Clear existing percentiles
        gridElement.innerHTML = '';

        // Create clickable percentile cards
        const percentileOrder = [25, 50, 75, 90, 95, 99];
        percentileOrder.forEach(p => {
            const percentileKey = `p${p}`;
            if (percentiles[percentileKey] === undefined || percentiles[percentileKey] === null) return;

            const card = document.createElement('div');
            card.className = 'percentile-card';
            card.dataset.percentile = p;
            card.dataset.value = percentiles[percentileKey];

            const label = document.createElement('div');
            label.className = 'percentile-label';
            label.textContent = `P${p}`;

            const value = document.createElement('div');
            value.className = 'percentile-value';
            value.textContent = this.formatNumber(percentiles[percentileKey]);

            card.appendChild(label);
            card.appendChild(value);

            // Add value bar to visualize the percentile position
            this.createValueBar(card, p);

            gridElement.appendChild(card);
        });
    }

    handlePercentileCardClick(columnName, card) {
        const percentile = parseInt(card.dataset.percentile);
        const value = parseFloat(card.dataset.value);

        const opSelect = document.getElementById(`${columnName}_op`);
        const val1Input = document.getElementById(`${columnName}_val1`);

        if (percentile <= 50) {
            // For lower percentiles, use "less than" (get bottom X%)
            opSelect.value = 'percentile_lt';
            val1Input.value = percentile;
        } else {
            // For higher percentiles, use "greater than" (get top X%)
            opSelect.value = 'percentile_gt';
            val1Input.value = 100 - percentile;
        }

        // Trigger operation change to update UI
        this.handleOperationChange(columnName);

        // Highlight the selected card
        document.querySelectorAll('.percentile-card').forEach(c => {
            c.classList.remove('selected');
        });
        card.classList.add('selected');
    }

    updateChart(columnName, stats) {
        const chartElement = document.getElementById(`${columnName}_chart`);

        // Check if chart element exists
        if (!chartElement) {
            console.error(`Chart element not found for column: ${columnName}`);
            return;
        }

        const chartContainer = chartElement.closest('.distribution-chart');

        // Ensure we have valid frequency data
        if (!stats.frequency || !Array.isArray(stats.frequency) || stats.frequency.length === 0) {
            // Show a message instead of an empty chart
            chartContainer.innerHTML = `
                <div class="empty-message">
                    <i class="fas fa-chart-bar"></i>
                    <span>No distribution data available for this column</span>
                </div>
            `;
            return;
        }

        const ctx = chartElement.getContext('2d');

        // Destroy existing chart if it exists
        if (this.charts.has(columnName)) {
            this.charts.get(columnName).destroy();
        }

        try {
            // Re-create canvas element if it was replaced
            if (chartContainer.querySelector('canvas') !== chartElement) {
                chartContainer.innerHTML = '';
                const canvas = document.createElement('canvas');
                canvas.id = `${columnName}_chart`;
                chartContainer.appendChild(canvas);
                ctx = canvas.getContext('2d');
            }

            // Prepare data for the chart
            const chartData = this.prepareChartData(stats.frequency);

            // Check if we have valid chart data
            if (!chartData.labels.length || !chartData.values.length) {
                chartContainer.innerHTML = `
                    <div class="empty-message">
                        <i class="fas fa-chart-bar"></i>
                        <span>Insufficient data for distribution chart</span>
                    </div>
                `;
                return;
            }

            // Calculate a nice step size for the y-axis
            const maxCount = Math.max(...chartData.values);
            const stepSize = this.calculateStepSize(maxCount);

            // Create color gradient based on percentiles
            const gradient = ctx.createLinearGradient(0, 0, 0, 400);
            gradient.addColorStop(0, 'rgba(54, 162, 235, 0.8)');
            gradient.addColorStop(1, 'rgba(54, 162, 235, 0.2)');

            // Create new chart
            const chart = new Chart(ctx, {
                type: 'bar',
                data: {
                    labels: chartData.labels,
                    datasets: [{
                        label: 'Distribution',
                        data: chartData.values,
                        backgroundColor: gradient,
                        borderColor: 'rgba(54, 162, 235, 1)',
                        borderWidth: 1,
                        barPercentage: 1,
                        categoryPercentage: 0.95
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: {
                            display: false
                        },
                        tooltip: {
                            callbacks: {
                                label: (context) => `Count: ${this.formatNumber(context.raw)}`,
                                title: (tooltipItems) => {
                                    const item = tooltipItems[0];
                                    const frequency = stats.frequency[item.dataIndex];
                                    if (frequency && frequency.range_start !== undefined) {
                                        return `Range: ${this.formatNumber(frequency.range_start)} - ${this.formatNumber(frequency.range_end)}`;
                                    }
                                    return frequency ? `Value: ${this.formatNumber(frequency.value)}` : 'Value: unknown';
                                }
                            }
                        }
                    },
                    scales: {
                        y: {
                            beginAtZero: true,
                            ticks: {
                                stepSize: stepSize,
                                callback: (value) => this.formatNumber(value)
                            },
                            title: {
                                display: true,
                                text: 'Count'
                            }
                        },
                        x: {
                            grid: {
                                display: false
                            },
                            ticks: {
                                autoSkip: true,
                                maxRotation: 0,
                                minRotation: 0
                            }
                        }
                    },
                    onClick: (event, elements) => {
                        if (elements && elements.length > 0) {
                            const index = elements[0].index;
                            const frequency = stats.frequency[index];
                            if (frequency) {
                                this.handleChartClick(columnName, frequency);
                            }
                        }
                    }
                }
            });

            this.charts.set(columnName, chart);

            // Add percentile markers to the chart
            this.addPercentileMarkersToChart(columnName, stats);
        } catch (error) {
            console.error(`Error creating chart for ${columnName}:`, error);
            chartContainer.innerHTML = `
                <div class="error-message">
                    <i class="fas fa-exclamation-circle"></i>
                    <span>Error creating chart: ${error.message}</span>
                </div>
            `;
        }
    }

    prepareChartData(frequency) {
        const labels = [];
        const values = [];

        if (!frequency || !Array.isArray(frequency)) {
            return { labels, values };
        }

        frequency.forEach(item => {
            if (!item) return;

            if (item.range_start !== undefined) {
                labels.push(this.formatNumber(item.range_start));
            } else if (item.value !== undefined) {
                labels.push(this.formatNumber(item.value));
            } else {
                // Skip items without proper data
                return;
            }

            values.push(item.count);
        });

        return { labels, values };
    }

    addPercentileMarkersToChart(columnName, stats) {
        // Get the chart to find its dimensions
        const chart = this.charts.get(columnName);
        if (!chart) return;

        const chartContainer = document.getElementById(`${columnName}_chart`).closest('.distribution-chart');

        // Remove any existing markers
        const existingMarkers = chartContainer.querySelectorAll('.percentile-marker-container');
        existingMarkers.forEach(marker => marker.remove());

        // Create container for percentile markers
        const markerContainer = document.createElement('div');
        markerContainer.className = 'percentile-marker-container';
        chartContainer.appendChild(markerContainer);

        // Add markers for key percentiles
        const percentiles = [25, 50, 75];
        percentiles.forEach(p => {
            const percentileKey = `p${p}`;
            if (!stats.percentiles || !stats.percentiles[percentileKey]) return;

            // Calculate position as a percentage of the chart width
            // We need to map the percentile value to the x-axis position
            const percentileValue = stats.percentiles[percentileKey];
            const percentilePosition = this.calculatePercentilePosition(
                percentileValue, stats.min, stats.max, chart
            );

            // Create marker
            const marker = document.createElement('div');
            marker.className = `percentile-marker p${p}`;
            marker.style.left = `${percentilePosition}%`;
            marker.dataset.percentile = p;
            marker.dataset.value = percentileValue;

            // Create label
            const label = document.createElement('div');
            label.className = 'percentile-marker-label';
            label.textContent = `P${p}`;
            marker.appendChild(label);

            // Add tooltip with value
            const tooltip = document.createElement('div');
            tooltip.className = 'percentile-tooltip';
            tooltip.textContent = this.formatNumber(percentileValue);
            marker.appendChild(tooltip);

            // Add click handler to apply filter
            marker.addEventListener('click', () => {
                this.handlePercentileMarkerClick(columnName, p, percentileValue);
            });

            markerContainer.appendChild(marker);
        });
    }

    calculatePercentilePosition(value, min, max, chart) {
        // For logarithmic scales or complex charts, we'd need more computation
        // For a simple linear scale:
        if (min === max) return 50; // Center if min=max

        // Calculate percentage position along the x-axis
        let position = ((value - min) / (max - min)) * 100;

        // Ensure the position is within bounds
        position = Math.max(0, Math.min(100, position));

        return position;
    }

    handlePercentileMarkerClick(columnName, percentile, value) {
        const opSelect = document.getElementById(`${columnName}_op`);
        const val1Input = document.getElementById(`${columnName}_val1`);

        // Set the appropriate operation based on which percentile was clicked
        if (percentile <= 25) {
            // Bottom 25% - use "less than" percentile operation
            opSelect.value = 'percentile_lt';
            val1Input.value = percentile;
        } else if (percentile >= 75) {
            // Top 25% - use "greater than" percentile operation
            opSelect.value = 'percentile_gt';
            val1Input.value = 100 - percentile;
        } else {
            // Middle 50% - use "between" operation with actual values
            opSelect.value = 'between';

            // Try to get the nearest percentiles
            const statsData = this.statsCache.get(columnName);
            if (statsData && statsData.percentiles) {
                if (percentile === 50) {
                    // For median, get the 25-75 range
                    val1Input.value = statsData.percentiles.p25 || value;
                    document.getElementById(`${columnName}_val2`).value = statsData.percentiles.p75 || value;
                } else {
                    // Default to the selected percentile value
                    val1Input.value = value;
                }
            } else {
                val1Input.value = value;
            }
        }

        this.handleOperationChange(columnName);

        // Highlight the selected marker
        const chartContainer = document.getElementById(`${columnName}_chart`).closest('.distribution-chart');
        chartContainer.querySelectorAll('.percentile-marker').forEach(marker => {
            marker.classList.remove('selected');
        });
        chartContainer.querySelector(`.p${percentile}`).classList.add('selected');
    }

    handleChartClick(columnName, frequency) {
        const opSelect = document.getElementById(`${columnName}_op`);
        const val1Input = document.getElementById(`${columnName}_val1`);
        const val2Input = document.getElementById(`${columnName}_val2`);

        // If we have a range, set up a between filter
        if (frequency.range_start !== undefined) {
            opSelect.value = 'between';
            val1Input.value = frequency.range_start;
            val2Input.value = frequency.range_end;
        } else {
            // Otherwise set up an equals filter
            opSelect.value = 'eq';
            val1Input.value = frequency.value;
        }

        this.handleOperationChange(columnName);
    }

    calculateStepSize(maxValue) {
        const targetSteps = 5;
        const magnitude = Math.pow(10, Math.floor(Math.log10(maxValue)));
        let stepSize = magnitude;

        if (maxValue / stepSize > targetSteps) {
            stepSize *= 2;
        }

        return stepSize;
    }

    handleOperationChange(columnName) {
        const opSelect = document.getElementById(`${columnName}_op`);
        const betweenRow = document.getElementById(`${columnName}_between_row`);
        const percentileInfo = document.getElementById(`${columnName}_percentile_info`);

        // Hide both specialized rows first
        betweenRow.style.display = 'none';
        percentileInfo.style.display = 'none';

        // Show the appropriate row based on selected operation
        switch (opSelect.value) {
            case 'between':
                betweenRow.style.display = 'flex';
                break;
            case 'percentile_gt':
                percentileInfo.style.display = 'block';
                percentileInfo.querySelector('.percentile-info').textContent =
                    'Show values in the top N% (e.g., 10 for top 10%)';
                break;
            case 'percentile_lt':
                percentileInfo.style.display = 'block';
                percentileInfo.querySelector('.percentile-info').textContent =
                    'Show values in the bottom N% (e.g., 25 for bottom 25%)';
                break;
        }

        // Highlight appropriate percentile cards based on the current operation and value
        this.updatePercentileCardHighlighting(columnName);
    }

    updatePercentileCardHighlighting(columnName) {
        const opSelect = document.getElementById(`${columnName}_op`);
        const val1Input = document.getElementById(`${columnName}_val1`);
        const percentileCards = document.querySelectorAll(`#${columnName}_op`)
            .closest('.numeric-filter-dropdown')
            .querySelectorAll('.percentile-card');

        // Reset all cards
        percentileCards.forEach(card => card.classList.remove('selected'));

        // If it's a percentile operation, find and highlight the matching card
        if (opSelect.value === 'percentile_lt') {
            const value = parseInt(val1Input.value);
            percentileCards.forEach(card => {
                if (parseInt(card.dataset.percentile) === value) {
                    card.classList.add('selected');
                }
            });
        } else if (opSelect.value === 'percentile_gt') {
            const value = 100 - parseInt(val1Input.value);
            percentileCards.forEach(card => {
                if (parseInt(card.dataset.percentile) === value) {
                    card.classList.add('selected');
                }
            });
        }
    }

    applyFilter(columnName) {
        const opSelect = document.getElementById(`${columnName}_op`);
        const val1Input = document.getElementById(`${columnName}_val1`);

        if (!opSelect.value || !val1Input.value) {
            alert('Please select an operation and enter a value.');
            return;
        }

        let url = new URL(window.location.href);

        // Remove existing filters for this column
        url.searchParams.delete(`${columnName}_op`);
        url.searchParams.delete(`${columnName}_val1`);
        url.searchParams.delete(`${columnName}_val2`);

        // Add new filter parameters
        url.searchParams.set(`${columnName}_op`, opSelect.value);
        url.searchParams.set(`${columnName}_val1`, val1Input.value);

        // If it's a between operation, add the second value
        if (opSelect.value === 'between') {
            const val2Input = document.getElementById(`${columnName}_val2`);
            if (val2Input && val2Input.value) {
                url.searchParams.set(`${columnName}_val2`, val2Input.value);
            }
        }

        // For percentile operations, validate the range
        if (opSelect.value === 'percentile_gt' || opSelect.value === 'percentile_lt') {
            const percentile = parseFloat(val1Input.value);
            if (percentile < 0 || percentile > 100 || isNaN(percentile)) {
                alert('Percentile value must be between 0 and 100');
                return;
            }
        }

        window.location.href = url.href;
    }

    clearFilter(columnName) {
        let url = new URL(window.location.href);
        url.searchParams.delete(`${columnName}_op`);
        url.searchParams.delete(`${columnName}_val1`);
        url.searchParams.delete(`${columnName}_val2`);
        window.location.href = url.href;
    }

    formatNumber(value) {
        if (value === null || value === undefined) return '-';
        if (typeof value === 'number') {
            if (Number.isInteger(value)) {
                return value.toLocaleString();
            }
            return value.toLocaleString(undefined, {
                minimumFractionDigits: 2,
                maximumFractionDigits: 2
            });
        }
        return value;
    }
}

// Initialize when the DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    window.numericFilterManager = new NumericFilterManager();

    // Override the global filter functions to use our manager
    window.applyNumericFilter = function(columnName) {
        window.numericFilterManager.applyFilter(columnName);
    };

    window.clearNumericFilter = function(columnName) {
        window.numericFilterManager.clearFilter(columnName);
    };

    window.toggleNumericFilter = function(element) {
        const dropdown = element.nextElementSibling;
        dropdown.style.display = dropdown.style.display === 'block' ? 'none' : 'block';

        if (dropdown.style.display === 'block') {
            const columnName = element.closest('.numeric-filter').querySelector('select').name.replace('_op', '');
            window.numericFilterManager.loadColumnStats(columnName);
        }
    };
});