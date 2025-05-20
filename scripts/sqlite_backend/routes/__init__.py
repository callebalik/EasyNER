from .disease_phenomena import register_disease_phenomena_routes
from .errors import init_error_handlers, SankeyVisualizationError


def init_routes(app, get_db, visualization_manager):
    """Initialize all routes and error handlers with dependency injection."""
    # Register error handlers first
    init_error_handlers(app)

    # Get the simple connection function from the app context
    get_db_simple_connection = (
        app.get_db_simple_connection
        if hasattr(app, "get_db_simple_connection")
        else None
    )

    # If the simple connection function isn't available, use our fallback
    if get_db_simple_connection is None:
        # Try to extract it from Flask's global context
        def get_db_simple_connection():
            """Get a simple database connection from the Flask context."""
            from flask import g, current_app
            from sqlite3 import connect

            if not hasattr(g, "_database_simple"):
                db_path = current_app.config.get("DB_PATH") or app.config.get("DB_PATH")
                if not db_path:
                    # Try to get it from environment
                    import os

                    db_path = os.environ.get("DB_PATH")
                g._database_simple = connect(db_path)
            return g._database_simple

    # Register routes using dependency injection - pass both connection functions
    register_disease_phenomena_routes(
        app, get_db_simple_connection, visualization_manager
    )

    return app
