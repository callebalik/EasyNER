/**
 * RangeSliderManager - Enhances numeric filter form with visual range sliders
 */
class RangeSliderManager {
    constructor() {
        this.sliders = new Map();
        this.initializeSliders();
    }

    initializeSliders() {
        document.querySelectorAll('.range-slider-container').forEach(container => {
            this.setupSlider(container);
        });
    }

    setupSlider(container) {
        const filterType = container.dataset.filterType;
        const sliderElement = container.querySelector('.range-slider');
        const minInput = document.getElementById(`min_${filterType}`);
        const maxInput = document.getElementById(`max_${filterType}`);

        // Skip if any required elements are missing
        if (!sliderElement || !minInput || !maxInput) return;

        // Get min/max from data attributes or use defaults
        const minValue = parseFloat(container.dataset.min || '-1');
        const maxValue = parseFloat(container.dataset.max || '1');

        // Get current values
        const currentMin = minInput.value ? parseFloat(minInput.value) : minValue;
        const currentMax = maxInput.value ? parseFloat(maxInput.value) : maxValue;

        // Initialize noUiSlider
        if (window.noUiSlider) {
            noUiSlider.create(sliderElement, {
                start: [currentMin, currentMax],
                connect: true,
                step: parseFloat(container.dataset.step || '0.01'),
                range: {
                    'min': minValue,
                    'max': maxValue
                },
                format: {
                    to: value => parseFloat(value).toFixed(2),
                    from: value => parseFloat(value)
                },
                tooltips: [true, true],
                pips: {
                    mode: 'positions',
                    values: [0, 25, 50, 75, 100],
                    density: 5,
                    format: {
                        to: value => parseFloat(value).toFixed(2)
                    }
                }
            });

            // Connect slider to inputs
            sliderElement.noUiSlider.on('update', (values, handle) => {
                const input = handle ? maxInput : minInput;
                input.value = values[handle];
            });

            // Connect inputs to slider
            minInput.addEventListener('change', () => {
                if (!minInput.value) return;

                const value = parseFloat(minInput.value);
                if (!isNaN(value)) {
                    sliderElement.noUiSlider.set([value, null]);
                }
            });

            maxInput.addEventListener('change', () => {
                if (!maxInput.value) return;

                const value = parseFloat(maxInput.value);
                if (!isNaN(value)) {
                    sliderElement.noUiSlider.set([null, value]);
                }
            });

            // Store reference to slider
            this.sliders.set(filterType, sliderElement);
        } else {
            // Fallback when noUiSlider not available - use HTML5 range inputs
            const rangeSlider = document.createElement('div');
            rangeSlider.className = 'native-range-slider';

            const slider = document.createElement('input');
            slider.type = 'range';
            slider.min = minValue;
            slider.max = maxValue;
            slider.step = container.dataset.step || '0.01';
            slider.value = currentMin;

            slider.addEventListener('input', () => {
                minInput.value = slider.value;
            });

            rangeSlider.appendChild(slider);
            sliderElement.parentNode.replaceChild(rangeSlider, sliderElement);
        }
    }

    updateSliderRange(filterType, min, max) {
        const slider = this.sliders.get(filterType);
        if (slider && slider.noUiSlider) {
            slider.noUiSlider.updateOptions({
                range: {
                    'min': min,
                    'max': max
                }
            });
        }
    }

    resetSlider(filterType) {
        const slider = this.sliders.get(filterType);
        if (slider && slider.noUiSlider) {
            const container = slider.closest('.range-slider-container');
            const minValue = parseFloat(container.dataset.min || '-1');
            const maxValue = parseFloat(container.dataset.max || '1');

            slider.noUiSlider.set([minValue, maxValue]);

            // Also reset the inputs
            document.getElementById(`min_${filterType}`).value = '';
            document.getElementById(`max_${filterType}`).value = '';
        }
    }
}

// Initialize when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    window.rangeSliderManager = new RangeSliderManager();
});