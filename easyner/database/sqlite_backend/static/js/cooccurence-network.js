$(document).ready(function() {
    // Initialize Select2
    $('.select2').select2({
        placeholder: "Select entities to exclude",
        allowClear: true
    });

    // Load entities for exclusion dropdowns
    loadEntities();

    // When entity types change, reload the entities list
    $('#entity1-type, #entity2-type, #min-frequency').change(function() {
        loadEntities();
    });

    // Generate network button click
    $('#generate-network').click(function() {
        generateNetwork();
    });

    // Initial network generation with default settings
    generateNetwork();
});

/**
 * Load entity options for exclusion dropdowns
 */
function loadEntities() {
    const minFrequency = $('#min-frequency').val();
    const entity1Type = $('#entity1-type').val();
    const entity2Type = $('#entity2-type').val();

    $.ajax({
        url: '/cooccurrence-network/entities',
        data: {
            min_frequency: minFrequency,
            entity1_type: entity1Type,
            entity2_type: entity2Type
        },
        success: function(data) {
            // Clear existing options
            $('#exclude-entity1').empty();
            $('#exclude-entity2').empty();

            // Populate entity1 dropdown
            if (data[entity1Type]) {
                data[entity1Type].forEach(function(entity) {
                    $('#exclude-entity1').append(
                        $('<option></option>')
                            .attr('value', entity.text)
                            .text(`${entity.text} (${entity.frequency})`)
                    );
                });
            }

            // Populate entity2 dropdown
            if (data[entity2Type]) {
                data[entity2Type].forEach(function(entity) {
                    $('#exclude-entity2').append(
                        $('<option></option>')
                            .attr('value', entity.text)
                            .text(`${entity.text} (${entity.frequency})`)
                    );
                });
            }

            // Refresh Select2
            $('.select2').trigger('change');
        },
        error: function(xhr, status, error) {
            console.error("Error loading entities:", error);
            alert("Failed to load entities: " + error);
        }
    });
}

/**
 * Generate and display the co-occurrence network visualization
 */
function generateNetwork() {
    // Show loading overlay
    $('#loading-overlay').show();

    // Get all parameters
    const params = {
        min_frequency: $('#min-frequency').val(),
        entity1_type: $('#entity1-type').val(),
        entity2_type: $('#entity2-type').val(),
        entity1_filter: $('#entity1-filter').val(),
        entity2_filter: $('#entity2-filter').val(),
        'excluded_entity1[]': $('#exclude-entity1').val() || [],
        'excluded_entity2[]': $('#exclude-entity2').val() || []
    };

    // Generate unique timestamp to prevent caching
    const timestamp = new Date().getTime();

    // Create URL with parameters
    let url = '/cooccurrence-network/generate?' + $.param(params) + '&_t=' + timestamp;

    // Load network in iframe
    $('#network-iframe').attr('src', url);

    // Hide loading overlay when iframe loads
    $('#network-iframe').on('load', function() {
        $('#loading-overlay').hide();
    });
}