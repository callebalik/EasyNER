from flask import render_template, jsonify, request

class SankeyVisualizationError(Exception):
    """Custom exception for Sankey visualization errors."""
    def __init__(self, message, details=None, status_code=500):
        super().__init__(message)
        self.message = message
        self.details = details
        self.status_code = status_code

class CooccurrenceNetworkError(Exception):
    """Custom exception for co-occurrence network visualization errors."""
    def __init__(self, message, details=None, status_code=500):
        super().__init__(message)
        self.message = message
        self.details = details
        self.status_code = status_code

def init_error_handlers(app):
    """Initialize error handlers for the Flask application."""

    @app.errorhandler(400)
    def bad_request_error(error):
        if request.path.startswith('/api/'):
            return jsonify({
                'error': 'Bad Request',
                'message': str(error)
            }), 400
        return render_template('error.html',
            error_code=400,
            error_name='Bad Request',
            error_description=str(error)
        ), 400

    @app.errorhandler(404)
    def not_found_error(error):
        if request.path.startswith('/api/'):
            return jsonify({
                'error': 'Not Found',
                'message': str(error)
            }), 404
        return render_template('error.html',
            error_code=404,
            error_name='Not Found',
            error_description='The requested resource was not found on this server.'
        ), 404

    @app.errorhandler(500)
    def internal_error(error):
        app.logger.error(f'Server Error: {str(error)}', exc_info=True)
        if request.path.startswith('/api/'):
            return jsonify({
                'error': 'Internal Server Error',
                'message': 'An unexpected error occurred'
            }), 500
        return render_template('error.html',
            error_code=500,
            error_name='Internal Server Error',
            error_description='An unexpected error occurred. Please try again later.'
        ), 500

    @app.errorhandler(Exception)
    def handle_exception(error):
        """Handle any uncaught exception."""
        app.logger.error(f'Uncaught Exception: {str(error)}', exc_info=True)
        if request.path.startswith('/api/'):
            return jsonify({
                'error': 'Internal Server Error',
                'message': 'An unexpected error occurred',
                'details': str(error) if app.debug else None
            }), 500
        return render_template('error.html',
            error_code=500,
            error_name='Internal Server Error',
            error_description='An unexpected error occurred. Please try again later.',
            error_details=str(error) if app.debug else None
        ), 500

    @app.errorhandler(SankeyVisualizationError)
    def handle_sankey_error(error):
        """Handle specific Sankey visualization errors."""
        app.logger.error(f'Sankey Visualization Error: {error.message}', exc_info=True)
        if request.path.startswith('/api/'):
            return jsonify({
                'error': 'Visualization Error',
                'message': error.message,
                'details': error.details if app.debug else None
            }), error.status_code
        return render_template('error.html',
            error_code=error.status_code,
            error_name='Visualization Error',
            error_description=error.message,
            error_details=error.details if app.debug else None
        ), error.status_code

    @app.errorhandler(CooccurrenceNetworkError)
    def handle_cooccurrence_network_error(error):
        """Handle specific co-occurrence network visualization errors."""
        app.logger.error(f'Co-occurrence Network Error: {error.message}', exc_info=True)
        if request.path.startswith('/api/'):
            return jsonify({
                'error': 'Visualization Error',
                'message': error.message,
                'details': error.details if app.debug else None
            }), error.status_code
        return render_template('error.html',
            error_code=error.status_code,
            error_name='Visualization Error',
            error_description=error.message,
            error_details=error.details if app.debug else None
        ), error.status_code