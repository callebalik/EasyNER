from flask import Flask, jsonify, g, render_template, request, has_request_context
from .db_main import EasyNerDBHandler
from .data_model import entities
from .data_model.schema import *
import os
import sass
import logging
from logging.handlers import RotatingFileHandler
import re
import sys
import importlib
import time
import traceback
import psutil
import atexit
import argparse
from socket import socket, AF_INET, SOCK_STREAM
import threading
from contextlib import contextmanager
import tempfile


DBPATH = os.environ.get("DB_PATH")
if not DBPATH:
    raise ValueError("DB_PATH environment variable is not set")
if not os.path.exists(DBPATH):
    raise ValueError(f"Database path {DBPATH} does not exist")
if not os.path.isabs(DBPATH):
    raise ValueError(f"Database path {DBPATH} is not an absolute path")


# Import our new monitoring module
from .monitoring import OperationMonitor, DBConnectionMonitor, ThreadMonitor

# Import after app is defined
from .statistics.visualization_manager import VisualizationManager

def setup_logging(app):
    """Configure logging for the application"""
    # Create logs directory if it doesn't exist
    log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
    os.makedirs(log_dir, exist_ok=True)

    # Set up file handler
    log_file = os.path.join(log_dir, 'server.log')
    print(f"Logging to: {log_file}")

    # Only remove Flask's default handlers, not handlers from other loggers
    flask_logger = app.logger
    werkzeug_logger = logging.getLogger('werkzeug')

    # Remove Flask's default handlers
    for handler in flask_logger.handlers[:]:
        flask_logger.removeHandler(handler)

    # Remove Werkzeug's default handlers
    for handler in werkzeug_logger.handlers[:]:
        werkzeug_logger.removeHandler(handler)

    class SmartFormatter(logging.Formatter):
        def format(self, record):
            # First format the basic message
            formatted = super().format(record)

            # Only append traceback info if there is an actual exception
            if record.exc_info:
                # Get the traceback text
                tb = '\n'.join(traceback.format_exception(*record.exc_info))
                if tb:
                    formatted += f"\nStack trace:\n{tb}"

            return formatted

    class RequestFormatter(logging.Formatter):
        def format(self, record):
            # For Werkzeug request logs, simplify the message
            if hasattr(record, 'msg') and isinstance(record.msg, str) and ' - - [' in record.msg:
                # Extract just the method, path and status code
                try:
                    method = record.msg.split('"')[1].split()[0]
                    path = record.msg.split('"')[1].split()[1]
                    status = record.msg.split('"')[2].strip().split()[0]
                    record.msg = f"{method} {path} - {status}"
                except:
                    pass # If parsing fails, leave message as is
            return super().format(record)

    # Create file handler with the smart formatter
    # file_handler = RotatingFileHandler(log_file, maxBytes=1024 * 1024, backupCount=10)
    file_handler = logging.FileHandler(log_file, mode='w')
    file_handler.setFormatter(SmartFormatter(
        '[%(asctime)s] %(levelname)s in %(module)s: %(message)s'
    ))
    file_handler.setLevel(logging.DEBUG)

    # Create console handler with the request formatter for cleaner output
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(RequestFormatter(
        '[%(levelname)s] %(message)s'
    ))
    console_handler.setLevel(logging.WARNING)

    # Add handler to app logger
    flask_logger.addHandler(file_handler)
    flask_logger.addHandler(console_handler)
    flask_logger.setLevel(logging.INFO)

    # Configure werkzeug logger to be less verbose
    werkzeug_logger.addHandler(file_handler)
    werkzeug_logger.addHandler(console_handler)
    werkzeug_logger.setLevel(logging.WARNING)  # Only show warnings and errors

    app.logger.info('Flask logging setup completed')

# Set template directory to current directory/templates
template_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates")
app = Flask(__name__, template_folder=template_dir)
setup_logging(app)

# Initialize thread-local storage for database connections
db_connections = threading.local()

# Initialize monitors
operation_monitor = OperationMonitor(app.logger)
connection_monitor = DBConnectionMonitor(app.logger)
thread_monitor = ThreadMonitor(app.logger)

# Register periodic monitoring
operation_monitor.register_periodic_monitor(
    app,
    interval=int(os.environ.get('EASYNER_METRIC_INTERVAL', '60'))
)

# Initialize visualization manager
visualization_manager = VisualizationManager(app)

@contextmanager
def get_db_connection():
    """Get a thread-local database connection with monitoring"""
    # Check if we already have a connection for this thread
    if not hasattr(db_connections, 'connection'):
        try:
            # Start monitoring the database connection operation
            with operation_monitor.monitor_operation('create_db_connection'):
                # Create a new connection
                connection = EasyNerDBHandler()
                db_connections.connection = connection

                # Track whether this connection was created within a request context
                # to determine if it should be closed at request end
                is_request_context = has_request_context()
                db_connections.is_request_connection = is_request_context

                # Register the connection with the monitor
                connection_monitor.register_connection(
                    connection.conn,
                    context={
                        'thread_id': threading.get_ident(),
                        'request_connection': is_request_context
                    }
                )

                app.logger.debug(f"Created new database connection (request context: {is_request_context})", extra={
                    'thread_id': threading.get_ident(),
                    'conn_id': id(connection.conn)
                })
        except Exception as e:
            app.logger.error(f"Database connection error: {e}", exc_info=True)
            raise

    # Yield the connection
    try:
        yield db_connections.connection
    except Exception as e:
        # Log the exception with the monitor
        operation_monitor.monitor_exception(e, context={
            'operation': 'database_operation',
            'thread_id': threading.get_ident()
        })
        raise


def get_db_simple_connection() -> sqlite3.Connection:
    """Get a simple database connection"""
    db = getattr(g, '_database_simple', None)
    if db is None:
        db = g._database = sqlite3.connect(DBPATH)
    return db

def get_db_easyner():
    """Get the database handler using the connection manager"""
    with get_db_connection() as db:
        return db

def close_db_connections():
    """Close all database connections during cleanup"""
    app.logger.info("Closing all database connections")

    # Use connection monitor to check for leaked connections
    leaks = connection_monitor.check_for_leaks()
    if leaks:
        app.logger.warning(f"Found {len(leaks)} potentially leaked connections")

    # Close all connections
    connection_monitor.close_all()

# Register cleanup function
atexit.register(close_db_connections)

@app.teardown_appcontext
def cleanup(e=None):
    """Close database connections created during this request"""
    if e:
        app.logger.error(f"Error during request: {e}", exc_info=True)

    # Close main connection if it was created during a request
    if hasattr(db_connections, 'connection') and getattr(db_connections, 'is_request_connection', False):
        try:
            # Close the connection
            db_connections.connection.close()

            # Remove the connection from thread-local storage
            del db_connections.connection
            del db_connections.is_request_connection

            app.logger.debug("Closed request-specific database connection")
        except Exception as close_error:
            app.logger.error(f"Error closing request database connection: {close_error}")

def close_simple_connection(exception):
    """Close the simple database connection."""
    db = getattr(g, '_database_simple', None)
    if db is not None:
        db.close()

app.teardown_appcontext(close_simple_connection)

# Compile SCSS to CSS on server load
def compile_scss():
    """Compile SCSS files to CSS with support for partials and watching changes"""
    try:
        scss_file = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "static/styles.scss"
        )
        css_file = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "static/styles.css"
        )
        static_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
        partials_dir = os.path.join(static_dir, "partials")

        # Ensure the partials directory exists
        os.makedirs(partials_dir, exist_ok=True)

        # Read and compile the main SCSS file
        with open(scss_file, "r") as f:
            scss_content = f.read()

        # Compile SCSS with include paths for partials
        css_content = sass.compile(
            string=scss_content,
            include_paths=[static_dir],
            output_style="compressed" if not app.debug else "nested",
        )

        # Write the compiled CSS
        with open(css_file, "w") as f:
            f.write(css_content)

        app.logger.info("SCSS compilation successful")
    except Exception as e:
        app.logger.error(f"Error compiling SCSS: {e}")
        raise


compile_scss()


# Serve static CSS file
@app.route("/static/styles.css")
def styles():
    return app.send_static_file("styles.css")


def align_with_schema():
    """Initialize database schema if needed"""
    try:
        with get_db_connection() as db:
            # Use existing schema alignment method
            schema_path = os.path.join(os.path.dirname(__file__), "schema.sql")
            db.align_with_schema(schema_path)

            # Apply indexes (they are idempotent with IF NOT EXISTS)
            with open(os.path.join(os.path.dirname(__file__), "indexes.sql"), "r") as f:
                indexes_sql = f.read()
                for statement in indexes_sql.split(";"):
                    if statement.strip():
                        try:
                            db.execute(statement)
                        except Exception as e:
                            app.logger.warning(f"Error applying index: {e}")
                            continue

            app.logger.info("Database indexes applied successfully")

            return db
    except Exception as e:
        app.logger.error(f"Error initializing database: {e}")
        raise


def get_available_entities(db):
    """Get available entity types from database"""
    try:
        return db.execute(
            f"SELECT {CLASS_ID}, {NE_CLASS} FROM {TABLE_NE_CLASS} ORDER BY {NE_CLASS}"
        )
    except Exception as e:
        app.logger.error(f"Error fetching entities: {e}")
        return []


@app.route("/")
def home():
    """Home page displaying database statistics"""
    try:
        with operation_monitor.monitor_operation('home_page_load'):
            with get_db_connection() as db:
                tables_info = {}

                # Get counts for each table
                for table in db.tables["tables"]:
                    count = db.execute(f"SELECT COUNT(*) FROM {table}")[0][0]
                    columns = db.execute(f"PRAGMA table_info({table})")
                    tables_info[table] = {
                        "row_count": count,
                        "columns": [col[1] for col in columns],
                    }

                # Get database statistics
                stats = {
                    "db_name": db.name,
                    "db_size": _format_size(db.statistics.size),
                    "source_size": _format_size(db.statistics.total_source_size),
                    "compression_ratio": f"{db.statistics.compression_ratio:.2f}",
                    "document_count": db.statistics.document_count,
                    "sentence_count": db.statistics.sentence_count,
                    "named_entity_count": db.statistics.named_entity_classes_count,
                }

                return render_template("home.html", tables=tables_info, stats=stats)
    except Exception as e:
        app.logger.error(f"Error loading home page: {e}")
        operation_monitor.monitor_exception(e, context={'route': '/'})
        return (
            render_template("error.html", message="Error loading database information"),
            500,
        )


def _format_size(size_bytes):
    """Convert size in bytes to human readable format."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size_bytes < 1024 or unit == "TB":
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024


@app.route("/health")
def health_check():
    """Health check endpoint with database status"""
    try:
        with operation_monitor.monitor_operation('health_check'):
            with get_db_connection() as db:
                tables_info = {}

                # Get counts for each table
                for table in db.tables["tables"]:
                    count = db.execute(f"SELECT COUNT(*) FROM {table}")[0][0]
                    columns = db.execute(f"PRAGMA table_info({table})")
                    tables_info[table] = {
                        "row_count": count,
                        "columns": [col[1] for col in columns],
                    }

                # Get connection status
                connections = connection_monitor.get_open_connections()

                # Get thread status
                threads = thread_monitor.get_active_threads()

                return (
                    jsonify(
                        {
                            "status": "healthy",
                            "database": {"connected": True, "tables": tables_info},
                            "server_status": "running",
                            "connections": {
                                "count": len(connections),
                                "details": connections[:5]  # Limit for readability
                            },
                            "threads": {
                                "count": len(threads),
                                "details": threads[:5]  # Limit for readability
                            },
                            "endpoints": {
                                "/": "Health check and basic info",
                                "/tables": "Detailed table information",
                                "/document/<id>": "Get document by ID",
                            },
                        }
                    ),
                    200,
                )
    except Exception as e:
        app.logger.error(f"Health check failed: {e}")
        operation_monitor.monitor_exception(e, context={'route': '/health'})
        return jsonify({"status": "unhealthy", "error": str(e)}), 500

@app.route("/documents")
def list_documents():
    """Display documents with filtering"""
    try:
        with operation_monitor.monitor_operation('list_documents'):
            with get_db_connection() as db:
                page = int(request.args.get("page", 1))
                query = request.args.get("query", "")
                doc_id = request.args.get("doc_id", "")
                selected_entities = request.args.getlist("entities")
                per_page = 30
                offset = (page - 1) * per_page

                params = []
                conditions = []

                if query:
                    conditions.append(f"d.{TITLE} LIKE ?")
                    params.append(f"%{query}%")

                if doc_id:
                    conditions.append(f"d.{DOC_ID} = ?")
                    params.append(doc_id)

                # Base query
                sql = f"""--sql
                    SELECT DISTINCT d.{DOC_ID}, d.{TITLE}, d.{WORD_COUNT}
                    FROM documents d
                """

                # Add entity filtering - using EXISTS for each entity to ensure ALL are present
                if selected_entities:
                    for entity_id in selected_entities:
                        sql_condition = f"""
                        EXISTS (
                            SELECT 1 FROM {TABLE_NE} eo{entity_id}
                            JOIN {TABLE_NE_CLASS} ne{entity_id} ON eo{entity_id}.{CLASS_ID} = ne{entity_id}.{CLASS_ID}
                            WHERE eo{entity_id}.{DOC_ID} = d.{DOC_ID} AND ne{entity_id}.{CLASS_ID} = ?
                        )"""
                        conditions.append(sql_condition)
                        params.append(entity_id)

                if conditions:
                    sql += " WHERE " + " AND ".join(conditions)

                sql += f" ORDER BY d.{DOC_ID} LIMIT ? OFFSET ?"
                params.extend([per_page + 1, offset])

                # Using monitor_query to track query performance
                with operation_monitor.monitor_query(sql, params,
                                                  context={'page': page, 'per_page': per_page},
                                                  conn=db.conn):
                    documents = db.execute(sql, params)

                has_more = len(documents) > per_page
                documents = documents[:per_page]  # Trim to per_page items

                # Get available entities for the filter
                entities = get_available_entities(db)

                return render_template(
                    "documents.html",
                    documents=[
                        dict(zip(["id", "title", "word_count"], doc)) for doc in documents
                    ],
                    page=page,
                    query=query,
                    doc_id=doc_id,
                    has_more=has_more,
                    entities=entities,
                    selected_entities=selected_entities,
                )
    except Exception as e:
        app.logger.error(f"Error loading documents page: {e}")
        operation_monitor.monitor_exception(e, context={'route': '/documents'})
        return render_template("error.html", message="Error loading documents"), 500

@app.route("/named-entities")
def list_named_entity_classes():
    result = display_table(TABLE_NE)
    if "error" in result:
        return render_template("error.html", message=result["error"]), 500
    return render_template("named_entities.html", **result)


@app.route("/named-entities/types")
def get_named_entity_types():
    try:
        db = get_db_simple_connection()
        sql = f"SELECT {CLASS_ID}, {NE_CLASS} FROM {TABLE_NE_CLASS} ORDER BY {NE_CLASS}"
        types = db.execute(sql)
        return jsonify(
            {
                "types": [
                    dict(zip(["id", "class_name"], type_row)) for type_row in types
                ]
            }
        )
    except Exception as e:
        app.logger.error(f"Error fetching named entity types: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/entity-occurrences")
def list_entity_occurrences():
    result = display_table(VIEW_NE_PRESENTATION)
    if "error" in result:
        return render_template("error.html", message=result["error"]), 500
    return render_template(
        "table_view.html", table_name=f"{entities.VIEW_NE_COMP}", **result
    )


@app.route("/document/<int:doc_id>")
def show_document(doc_id):
    db = get_db_simple_connection()

    document = db.data_exchanger.get_document(doc_id)
    if not document:
        return "Document not found", 404

    return render_template(
        "document.html", document=document, content=document.to_html()
    )


@app.route("/entity-cooccurrences/")
def entity_cooccurrences():
    return render_template("entity_cooccurrences.html")


@app.route("/entity-cooccurrences/summary")
def entity_cooccurrences_summary():
    return render_template("entity_cooccurrences_summary.html")


@app.route("/entity-cooccurrences/table")
def entity_cooccurrences_table():
    try:
        db = get_db_simple_connection()
        page = int(request.args.get("page", 1))
        per_page = int(request.args.get("per_page", 30))
        offset = (page - 1) * per_page

        query = f"""--sql
            SELECT
                ec.id,
                ec.e1_id,
                ec.e2_id,
                eo1.entity_text as entity1_text,
                eo2.entity_text as entity2_text,
                ec.sentence_distance,
                eo1.document_id,
                eo1.sentence_index
            FROM entity_cooccurrences ec
            JOIN entity_occurrences eo1 ON eo1.id = ec.e1_id
            JOIN entity_occurrences eo2 ON eo2.id = ec.e2_id
            ORDER BY ec.id DESC
            LIMIT ? OFFSET ?
        """
        params = [per_page + 1, offset]

        cooccurrences = db.execute(query, params)
        has_more = len(cooccurrences) > per_page
        cooccurrences = cooccurrences[:per_page]

        return jsonify(
            {
                "cooccurrences": [
                    dict(
                        zip(
                            [
                                "id",
                                "e1_id",
                                "e2_id",
                                "entity1_text",
                                "entity2_text",
                                "sentence_distance",
                                "document_id",
                                "sentence_index",
                            ],
                            row,
                        )
                    )
                    for row in cooccurrences
                ],
                "has_more": has_more,
            }
        )
    except Exception as e:
        app.logger.error(f"Error loading entity co-occurrences table: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/entity-cooccurrences/summary/table")
def entity_cooccurrences_summary_table():
    try:
        db = get_db_simple_connection()
        page = int(request.args.get("page", 1))
        per_page = int(request.args.get("per_page", 30))
        include_self = request.args.get("include_self", "false").lower() == "true"
        entity1_type = request.args.get("entity1_type")
        entity2_type = request.args.get("entity2_type")
        sort = request.args.get("sort", "fq_document_level")
        order = request.args.get("order", "desc")
        entity1_search = request.args.get("entity1_search")
        entity2_search = request.args.get("entity2_search")

        result = db.data_exchanger.get_cooccurrences_summary(
            page=page,
            per_page=per_page,
            include_self=include_self,
            entity1_type=entity1_type,
            entity2_type=entity2_type,
            sort=sort,
            order=order,
            entity1_search=entity1_search,
            entity2_search=entity2_search,
        )

        return jsonify(
            {
                "summaries": result["summaries"],
                "has_more": result["has_more"],
                "total": result["total"],
            }
        )
    except Exception as e:
        app.logger.error(f"Error loading entity co-occurrences summary table: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/raw-cooccurrences")
def raw_cooccurrences():
    return render_template("raw_cooccurrences.html")


@app.route("/summary-cooccurrences")
def summary_cooccurrences():
    return render_template("summary_cooccurrences.html")


@app.route("/raw-cooccurrences/table")
def raw_cooccurrences_table():
    try:
        db = get_db_simple_connection()
        page = int(request.args.get("page", 1))
        per_page = int(request.args.get("per_page", 30))
        offset = (page - 1) * per_page

        query = """
            SELECT
                ec.id,
                ec.e1_id,
                ec.e2_id,
                eo1.entity_text as entity1_text,
                eo2.entity_text as entity2_text,
                ec.sentence_distance,
                eo1.document_id,
                eo1.sentence_index
            FROM entity_cooccurrences ec
            JOIN entity_occurrences eo1 ON eo1.id = ec.e1_id
            JOIN entity_occurrences eo2 ON eo2.id = ec.e2_id
            ORDER BY ec.id DESC
            LIMIT ? OFFSET ?
        """
        params = [per_page + 1, offset]

        cooccurrences = db.execute(query, params)
        has_more = len(cooccurrences) > per_page
        cooccurrences = cooccurrences[:per_page]

        return jsonify(
            {
                "cooccurrences": [
                    dict(
                        zip(
                            [
                                "id",
                                "e1_id",
                                "e2_id",
                                "entity1_text",
                                "entity2_text",
                                "sentence_distance",
                                "document_id",
                                "sentence_index",
                            ],
                            row,
                        )
                    )
                    for row in cooccurrences
                ],
                "has_more": has_more,
            }
        )
    except Exception as e:
        app.logger.error(f"Error loading entity co-occurrences table: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/summary-cooccurrences/table")
def summary_cooccurrences_table():
    try:
        db = get_db_easyner()
        page = int(request.args.get("page", 1))
        per_page = int(request.args.get("per_page", 30))
        include_self = request.args.get("include_self", "false").lower() == "true"
        entity1_type = request.args.get("entity1_type")
        entity2_type = request.args.get("entity2_type")
        sort = request.args.get("sort", "fq_document_level")
        order = request.args.get("order", "desc")
        entity1_search = request.args.get("entity1_search")
        entity2_search = request.args.get("entity2_search")

        result = db.data_exchanger.get_cooccurrences_summary(
            page=page,
            per_page=per_page,
            include_self=include_self,
            entity1_type=entity1_type,
            entity2_type=entity2_type,
            sort=sort,
            order=order,
            entity1_search=entity1_search,
            entity2_search=entity2_search,
        )

        return jsonify(
            {
                "summaries": result["summaries"],
                "has_more": result["has_more"],
                "total": result["total"],
            }
        )
    except Exception as e:
        app.logger.error(f"Error loading entity co-occurrences summary table: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/entity-cooccurrences/plot-data")
def entity_cooccurrences_plot_data():
    try:
        db = get_db_simple_connection()
        freq_column = request.args.get("freq_column", "fq_document_level")
        min_freq = int(request.args.get("min_freq", 0))
        max_freq = int(request.args.get("max_freq", 100))

        sql = f"""
            SELECT {freq_column}, COUNT(*) as count
            FROM entity_cooccurrences_summary
            WHERE {freq_column} BETWEEN ? AND ?
            GROUP BY {freq_column}
            ORDER BY {freq_column}
        """
        params = [min_freq, max_freq]

        plot_data = db.execute(sql, params)

        return jsonify(
            {
                "plot_data": [
                    dict(zip([col[0] for col in db.cursor.description], row))
                    for row in plot_data
                ]
            }
        )
    except Exception as e:
        app.logger.error(f"Error loading entity co-occurrences plot data: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/entity-cooccurrences/summary/plot-data")
def entity_cooccurrences_summary_plot_data():
    try:
        db = get_db_simple_connection()
        cursor = db.cursor
        freq_column = request.args.get("freq_column", "fq_document_level")
        min_freq = int(request.args.get("min_freq", 0))
        max_freq = int(request.args.get("max_freq", 100))

        sql = f"""
            SELECT {freq_column}, COUNT(*) as count
            FROM entity_cooccurrences_summary
            WHERE {freq_column} BETWEEN ? AND ?
            GROUP BY {freq_column}
            ORDER BY {freq_column}
        """
        params = [min_freq, max_freq]

        plot_data = cursor.execute(sql, params)

        return jsonify(
            {
                "plot_data": [
                    dict(zip([col[0] for col in cursor.description], row))
                    for row in plot_data
                ]
            }
        )
    except Exception as e:
        app.logger.error(f"Error loading entity co-occurrences summary plot data: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/summary-cooccurrences/plot-data")
def summary_cooccurrences_plot_data():
    try:
        db = get_db_easyner()
        app.logger.debug("Starting plot data generation")

        # Log request parameters
        params = {
            "freq_column": request.args.get("freq_column", "fq_document_level"),
            "bucket_size": request.args.get("bucket_size", 20),
            "include_self": request.args.get("include_self", "false").lower() == "true",
            "entity1_type": request.args.get("entity1_type"),
            "entity2_type": request.args.get("entity2_type"),
            "entity1_search": request.args.get("entity1_search"),
            "entity2_search": request.args.get("entity2_search"),
        }
        app.logger.debug(f"Plot parameters: {params}")

        # Validate frequency column to prevent SQL injection
        allowed_freq_columns = [
            "fq_document_level",
            "fq_document_level_normalized",
            "fq_sentence_level",
            "fq_sentence_level_normalized",
        ]
        if params["freq_column"] not in allowed_freq_columns:
            app.logger.error(f"Invalid frequency column: {params['freq_column']}")
            return jsonify({"error": "Invalid frequency column"}), 400

        try:
            bucket_size = int(params["bucket_size"])
            if not (5 <= bucket_size <= 100):
                app.logger.error(f"Invalid bucket size: {bucket_size}")
                return jsonify({"error": "Bucket size must be between 5 and 100"}), 400
        except ValueError:
            app.logger.error(f"Invalid bucket size parameter: {params['bucket_size']}")
            return jsonify({"error": "Invalid bucket size"}), 400

        # Build WHERE clause
        where_clauses = []
        query_params = []

        where_clauses.append(f"{params['freq_column']} IS NOT NULL")

        if not params["include_self"]:
            where_clauses.append("ecs.e1_id_normalized != ecs.e2_id_normalized")

        if params["entity1_type"]:
            where_clauses.append("e1.entity_id = ?")
            query_params.append(params["entity1_type"])
        if params["entity2_type"]:
            where_clauses.append("e2.entity_id = ?")
            query_params.append(params["entity2_type"])

        if params["entity1_search"]:
            where_clauses.append("e1.entity_text LIKE ?")
            query_params.append(f"%{params['entity1_search']}%")
        if params["entity2_search"]:
            where_clauses.append("e2.entity_text LIKE ?")
            query_params.append(f"%{params['entity2_search']}%")

        app.logger.debug(f"WHERE clauses: {where_clauses}")
        app.logger.debug(f"Query parameters: {query_params}")

        # Get min/max values with error handling
        min_max_sql = f"""
            SELECT
                MIN({params['freq_column']}) as min_freq,
                MAX({params['freq_column']}) as max_freq,
                COUNT(*) as total_count
            FROM entity_cooccurrences_summary ecs
            JOIN entity_occurrences e1 ON ecs.e1_id_normalized = e1.id
            JOIN entity_occurrences e2 ON ecs.e2_id_normalized = e2.id
            WHERE {' AND '.join(where_clauses)}
        """

        app.logger.debug(f"Executing min/max query: {min_max_sql}")
        app.logger.debug(f"With parameters: {query_params}")

        result = db.execute(min_max_sql, query_params)
        min_freq, max_freq, total_count = result[0]

        app.logger.debug(
            f"Min freq: {min_freq}, Max freq: {max_freq}, Total count: {total_count}"
        )

        if min_freq is None or max_freq is None or total_count == 0:
            app.logger.info("No data found matching the criteria")
            return jsonify(
                {
                    "pairs": [],
                    "total": 0,
                    "message": "No data found matching the criteria",
                }
            )

        # Get top pairs for the plot
        pairs_sql = f"""
            SELECT
                e1.entity_text as entity1_text,
                e2.entity_text as entity2_text,
                ecs.{params['freq_column']} as frequency
            FROM entity_cooccurrences_summary ecs
            JOIN entity_occurrences e1 ON ecs.e1_id_normalized = e1.id
            JOIN entity_occurrences e2 ON ecs.e2_id_normalized = e2.id
            WHERE {' AND '.join(where_clauses)}
            ORDER BY ecs.{params['freq_column']} DESC
            LIMIT 20
        """

        app.logger.debug(f"Executing pairs query: {pairs_sql}")
        pairs_result = db.execute(pairs_sql, query_params)

        pairs = []
        for row in pairs_result:
            pair = {
                "entity1_text": row[0],
                "entity2_text": row[1],
                params["freq_column"]: row[2],
            }
            pairs.append(pair)

        app.logger.debug(f"Found {len(pairs)} pairs for plotting")

        return jsonify({"pairs": pairs, "total": total_count})

    except Exception as e:
        app.logger.error(f"Error generating plot data: {str(e)}", exc_info=True)
        return jsonify({"error": "Internal server error", "message": str(e)}), 500


@app.route("/debug/entity-cooccurrences-summary")
def debug_entity_cooccurrences_summary():
    try:
        db = get_db_easyner()

        # Check total count
        count_sql = "SELECT COUNT(*) FROM entity_cooccurrences_summary"
        count = db.execute(count_sql)[0][0]

        # Get a sample row with entity texts
        sample_sql = """
            SELECT
                ecs.*,
                eo1.entity_text as entity1_text,
                eo2.entity_text as entity2_text
            FROM entity_cooccurrences_summary ecs
            JOIN entity_occurrences eo1 ON ecs.e1_id = eo1.id
            JOIN entity_occurrences eo2 ON ecs.e2_id = eo2.id
            LIMIT 1
        """
        sample = db.execute(sample_sql)

        # Get the actual table schema
        schema_sql = "PRAGMA table_info(entity_cooccurrences_summary)"
        schema = db.execute(schema_sql)

        return jsonify(
            {
                "total_records": count,
                "schema": [
                    dict(
                        zip(["cid", "name", "type", "notnull", "dflt_value", "pk"], col)
                    )
                    for col in schema
                ],
                "sample_row": [
                    dict(zip([col[0] for col in db.cursor.description], row))
                    for row in sample
                ],
            }
        )
    except Exception as e:
        app.logger.error(f"Debug endpoint error: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/view/<view_name>/sample")
def get_view_sample(view_name):
    try:
        db = get_db_easyner()
        # Check if it's actually a view first
        view_check = db.execute(
            "SELECT type FROM sqlite_master WHERE type='view' AND name=?", [view_name]
        )
        if not view_check:
            return jsonify({"error": "View not found"}), 404

        # Get schema info for column names
        schema = db.execute(f"PRAGMA table_info({view_name})")
        columns = [col[1] for col in schema]

        # Get sample row
        sample_sql = f"SELECT * FROM {view_name} LIMIT 1"
        sample = db.execute(sample_sql)

        if not sample:
            return jsonify({"message": "No data available"}), 404

        # Convert sample to dictionary
        sample_data = [dict(zip([col for col in columns], row)) for row in sample]

        return jsonify({"sample": sample_data})
    except Exception as e:
        app.logger.error(f"Error fetching sample for view {view_name}: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/views")
def show_views():
    try:
        db = get_db_simple_connection()
        views_info = {}

        # Get list of views with their full definitions
        views = db.execute("""
            SELECT name, sql
            FROM sqlite_master
            WHERE type='view'
        """)

        total_views = 0
        valid_views = 0

        for view in views:
            total_views += 1
            view_name = view[0]
            create_sql = view[1]

            if create_sql:
                create_sql = ' '.join(line.strip() for line in create_sql.splitlines())
                create_sql = re.sub(r'\s+', ' ', create_sql)

            # Validate the view and get its status
            is_valid, validation_result = validate_view(db, view_name)
            if is_valid:
                valid_views += 1
                schema_info = []
                schema_error = None
                try:
                    # Only try to get schema if view is valid
                    schema = db.execute(f"PRAGMA table_info({view_name})")
                    schema_info = [
                        dict(zip(["cid", "name", "type", "notnull", "dflt_value", "pk"], col))
                        for col in schema
                    ]
                except Exception as schema_error:
                    app.logger.warning(f"Error getting schema for valid view {view_name}: {schema_error}")
            else:
                schema_info = []
                schema_error = validation_result.get('error')

            views_info[view_name] = {
                "schema": schema_info,
                "definition": create_sql,
                "is_valid": is_valid,
                "schema_error": schema_error,
                "validation_result": validation_result
            }

        if not views_info:
            app.logger.info("No views found in the database")
            return render_template(
                "views.html",
                views=views_info,
                message="No views found in the database",
                total_views=0,
                valid_views=0
            )

        return render_template(
            "views.html",
            views=views_info,
            total_views=total_views,
            valid_views=valid_views,
            error="Some views have errors" if valid_views < total_views else None
        )

    except Exception as e:
        app.logger.error(f"Error loading views page: {e}")
        return render_template("error.html", message=f"Error loading views page: {str(e)}"), 500


@app.route("/tables-json")
def get_tables_json():
    try:
        db = get_db_easyner()
        tables_info = {}

        for table in db.tables["tables"]:
            # Get column information
            columns = db.execute(f"PRAGMA table_info({table})")
            columns_info = [
                {
                    "name": col[1],
                    "type": col[2],
                    "nullable": not col[3],
                    "primary_key": bool(col[5]),
                }
                for col in columns
            ]

            # Get foreign keys
            foreign_keys = db.execute(f"PRAGMA foreign_key_list({table})")
            fk_info = [
                {"from": fk[3], "to_table": fk[2], "to_column": fk[4]}
                for fk in foreign_keys
            ]

            tables_info[table] = {"columns": columns_info, "foreign_keys": fk_info}

        return jsonify(tables_info), 200
    except Exception as e:
        app.logger.error(f"Error getting table information: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/tables")
@app.route("/tables/")
def show_tables():
    try:
        db = get_db_simple_connection()
        tables_info = {}

        # Get list of tables ordered alphabetically
        tables = db.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type='table' AND name NOT LIKE 'sqlite_%'
            ORDER BY name ASC
            """
        )

        for table in tables:
            table_name = table[0]

            # Get table schema information
            schema = db.execute(f"PRAGMA table_info({table_name})")
            columns = [
                {
                    "name": col[1],
                    "type": col[2],
                    "notnull": col[3] == 1,
                    "dflt_value": col[4],
                    "pk": col[5] == 1
                }
                for col in schema
            ]

            # Get foreign keys information
            foreign_keys = db.execute(f"PRAGMA foreign_key_list({table_name})")
            fk_info = [
                {
                    "from_column": fk[3],
                    "to_table": fk[2],
                    "to_column": fk[4]
                }
                for fk in foreign_keys
            ]

            # Store comprehensive table information
            tables_info[table_name] = {
                "name": table_name,
                "columns": columns,
                "foreign_keys": fk_info
            }

        return render_template("tables.html", tables=tables_info)
    except Exception as e:
        app.logger.error(f"Error loading tables: {e}")
        return render_template("error.html", message=f"Error loading tables: {str(e)}"), 500


@app.route("/table/<table_name>/rowcount")

def get_table_rowcount(table_name):
    try:
        db = get_db_easyner()
        count = db.execute(f"SELECT COUNT(*) FROM {table_name}")[0][0]
        return jsonify({"row_count": count})
    except Exception as e:
        app.logger.error(f"Error getting row count for {table_name}: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/tables/<table_name>")
def view_table(table_name):
    try:
        db = get_db_simple_connection()
        page = int(request.args.get("page", 1))
        per_page = 30
        offset = (page - 1) * per_page

        # Get column information dynamically
        db.cursor.execute(f"PRAGMA table_info({table_name})")
        columns = [col[1] for col in db.cursor.fetchall()]

        sql = f"""
            SELECT *
            FROM {table_name}
            LIMIT ? OFFSET ?
        """
        params = [per_page + 1, offset]

        rows = db.execute(sql, params)
        has_more = len(rows) > per_page
        rows = rows[:per_page]

        return render_template(
            "table_view.html",
            table_name=table_name,
            columns=columns,
            rows=rows,
            page=page,
            has_more=has_more,
        )
    except Exception as e:
        app.logger.error(f"Error loading table {table_name}: {e}")
        return (
            render_template("error.html", message=f"Error loading table {table_name}"),
            500,
        )


def display_table(table_name):
    try:
        db = get_db_simple_connection()
        cursor = db.cursor()
        page = int(request.args.get("page", 1))
        per_page = int(request.args.get("per_page", 30))
        offset = (page - 1) * per_page
        sort_by = request.args.get("sort_by", "id")
        sort_order = request.args.get("sort_order", "asc")
        search_query = request.args.get("search_query", "")
        # Default to False (show only OVERLAP = 0)
        show_overlap = request.args.get("show_overlap", "false").lower() == "true"
        # Get filter_no_errors parameter (default to True for showing only valid entities)
        filter_no_errors = request.args.get("filter_no_errors", "true").lower() == "true"

        # Get column information dynamically
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns_info = cursor.fetchall()
        columns = [col[1] for col in columns_info]
        column_types = {
            col[1]: col[2].upper() for col in columns_info
        }  # Store column types

        # Get text columns for text search
        text_columns = [col[1] for col in columns_info if col[2].upper() == "TEXT"]

        # Get numeric columns for numeric filtering
        numeric_columns = [col[1] for col in columns_info if col[2].upper() in ["INTEGER", "INT", "REAL", "FLOAT", "NUMERIC"]]

        # Include ID columns that should be searchable
        searchable_id_columns = [col[1] for col in columns_info if col[1] in ['id', 'NE_NORM_ID', 'NE_PRIMARY_ID'] and col[2].upper() in ['INTEGER', 'INT']]

        # Extract search queries for each searchable column
        column_search_queries = {
            col: request.args.get(f"{col}_search", "") for col in text_columns + searchable_id_columns
        }

        # Get NE_CLASS filters (can be multiple values)
        selected_classes = request.args.getlist("NE_CLASS_filter")

        # Get ERROR_ID filters (can be multiple values)
        selected_errors = request.args.getlist("ERROR_ID_filter")

        # Process numeric filters
        numeric_filters = {}
        for column in numeric_columns:
            op = request.args.get(f"{column}_op")
            if op:
                val1 = request.args.get(f"{column}_val1")
                if val1:
                    # Create a filter object with operator and value(s)
                    numeric_filters[column] = {
                        "op": op,
                        "val1": val1,
                        "val2": request.args.get(f"{column}_val2", "") if op == "between" else None
                    }

        # Fetch available NE_CLASS values if the table has that column
        ne_classes = None
        if 'NE_CLASS' in columns:
            try:
                query_result = cursor.execute(f"SELECT {NE_CLASS} FROM {TABLE_NE_CLASS} ORDER BY {NE_CLASS}")
                ne_classes = [row[0] for row in query_result if row[0]]
            except Exception as ne_class_error:
                app.logger.warning(f"Error fetching NE_CLASS values: {ne_class_error}")

        # Fetch available ERROR_ID values if the table has that column
        error_codes = None
        if 'ERROR_ID' in columns:
            try:
                query_result = cursor.execute(f"SELECT {ERROR_ID}, {ERROR_DESC} FROM {TABLE_NE_ERROR} ORDER BY {ERROR_ID}")
                error_codes = [(row[0], row[1]) for row in query_result if row[0]]
            except Exception as error_code_error:
                app.logger.warning(f"Error fetching ERROR_ID values: {error_code_error}")

        # Build the base SQL query
        sql = f"SELECT * FROM {table_name}"
        params = []

        # Add search functionality for columns
        search_conditions = []

        # Process text columns (LIKE search)
        for col, query in column_search_queries.items():
            if query and col in text_columns:
                search_conditions.append(f"{col} LIKE ?")
                params.append(f"%{query}%")
            # Process ID columns (exact match)
            elif query and col in searchable_id_columns:
                try:
                    id_value = int(query)
                    search_conditions.append(f"{col} = ?")
                    params.append(id_value)
                except ValueError:
                    search_conditions.append(f"{col} LIKE ?")
                    params.append(f"%{query}%")
                    app.logger.debug(f"Non-integer search value '{query}' for ID column {col}")

        # Process numeric filters
        percentile_filters = []  # Track any percentile filters for later processing

        for column, filter_data in numeric_filters.items():
            op = filter_data["op"]
            try:
                # For percentile filters, we'll handle them differently
                if op.startswith('percentile_'):
                    # Validate percentile value (0-100)
                    percentile_value = float(filter_data["val1"])
                    if not (0 <= percentile_value <= 100):
                        app.logger.warning(f"Invalid percentile value: {percentile_value}. Must be between 0 and 100.")
                        continue

                    # Store for later processing after we have the query structure
                    percentile_filters.append({
                        "column": column,
                        "op": op,
                        "value": percentile_value
                    })
                    continue

                # Convert the first value based on column type
                if column_types[column] in ["REAL", "FLOAT", "NUMERIC"]:
                    val1 = float(filter_data["val1"])
                else:
                    val1 = int(filter_data["val1"])

                # Apply the appropriate operator
                if op == "gt":
                    search_conditions.append(f"{column} > ?")
                    params.append(val1)
                elif op == "lt":
                    search_conditions.append(f"{column} < ?")
                    params.append(val1)
                elif op == "eq":
                    search_conditions.append(f"{column} = ?")
                    params.append(val1)
                elif op == "gte":
                    search_conditions.append(f"{column} >= ?")
                    params.append(val1)
                elif op == "lte":
                    search_conditions.append(f"{column} <= ?")
                    params.append(val1)
                elif op == "between" and filter_data["val2"]:
                    # For BETWEEN, we need two values
                    if column_types[column] in ["REAL", "FLOAT", "NUMERIC"]:
                        val2 = float(filter_data["val2"])
                    else:
                        val2 = int(filter_data["val2"])
                    search_conditions.append(f"{column} BETWEEN ? AND ?")
                    params.extend([val1, val2])

                app.logger.debug(f"Applied numeric filter on {column}: {op} {val1} {filter_data['val2'] if op == 'between' else ''}")
            except (ValueError, TypeError) as e:
                app.logger.warning(f"Invalid numeric filter value for {column}: {e}")

        # Add NE_CLASS filter if present
        if selected_classes and 'NE_CLASS' in columns:
            placeholders = ','.join('?' for _ in selected_classes)
            search_conditions.append(f"NE_CLASS IN ({placeholders})")
            params.extend(selected_classes)
            app.logger.debug(f"Applied NE_CLASS filter: {selected_classes}")

        # Handle ERROR_ID filtering
        if 'ERROR_ID' in columns:
            if filter_no_errors:
                # Filter out entities with error codes
                search_conditions.append(f"{ERROR_ID} IS NULL")
                app.logger.debug("Applied filter: Show only entries without errors")
            elif selected_errors:
                # Only apply specific error codes filter if No Errors filter is not active
                placeholders = ','.join('?' for _ in selected_errors)
                search_conditions.append(f"{ERROR_ID} IN ({placeholders})")
                params.extend(selected_errors)
                app.logger.debug(f"Applied ERROR_ID filter: {selected_errors}")

        # Add OVERLAP filter if column exists and show_overlap is false
        if 'OVERLAP' in columns and not show_overlap:
            search_conditions.append("OVERLAP = 0")

        # Apply any WHERE conditions from the base filters
        where_clause = ""
        if search_conditions:
            where_clause = " WHERE " + " AND ".join(search_conditions)
            sql += where_clause

        # Handle percentile filters
        if percentile_filters:
            app.logger.debug(f"Processing {len(percentile_filters)} percentile filters")

            for p_filter in percentile_filters:
                column = p_filter["column"]
                is_top_percentile = p_filter["op"] == "percentile_gt"
                percentile_value = p_filter["value"]

                try:
                    # First check if the column has any data at all
                    check_query = f"""--sql
                        SELECT COUNT(*) FROM {table_name}
                        WHERE {column} IS NOT NULL
                        {' AND ' + ' AND '.join(search_conditions) if search_conditions else ''}
                    """

                    count_result = db.execute(check_query, params)
                    if not count_result or count_result[0][0] == 0:
                        app.logger.warning(f"No data found for percentile calculation on {column}")
                        continue

                    # Calculate the threshold value for this percentile
                    # Use a window function to calculate percentiles with error handling
                    percentile_query = f"""--sql
                        WITH data AS (
                            SELECT {column}
                            FROM {table_name}
                            WHERE {column} IS NOT NULL
                            {' AND ' + ' AND '.join(search_conditions) if search_conditions else ''}
                        ),
                        ranked AS (
                            SELECT
                                {column},
                                PERCENT_RANK() OVER (ORDER BY {column}) * 100 AS pct_rank
                            FROM data
                        )
                        SELECT {column} FROM ranked
                        WHERE pct_rank {'>' if is_top_percentile else '<'} ?
                        ORDER BY {column} {'DESC' if is_top_percentile else 'ASC'}
                        LIMIT 1
                    """

                    percentile_params = params + [100 - percentile_value if is_top_percentile else percentile_value]
                    app.logger.debug(f"Executing percentile query: {percentile_query} with params: {percentile_params}")

                    threshold_result = cursor.execute(percentile_query, percentile_params)

                    if threshold_result and threshold_result[0][0] is not None:
                        threshold_value = threshold_result[0][0]
                        app.logger.debug(f"Calculated {'top' if is_top_percentile else 'bottom'} {percentile_value}% threshold for {column}: {threshold_value}")

                        # Now that we have the threshold, add it as a condition to our main query
                        if search_conditions:
                            sql = sql + " AND "
                        else:
                            sql = sql + " WHERE "

                        if is_top_percentile:
                            sql = sql + f"{column} >= ?"
                            params.append(threshold_value)
                        else:
                            sql = sql + f"{column} <= ?"
                            params.append(threshold_value)

                        app.logger.debug(f"Applied percentile filter on {column}: {'top' if is_top_percentile else 'bottom'} {percentile_value}%")
                    else:
                        app.logger.warning(f"No valid threshold found for percentile filter on {column}")
                except Exception as e:
                    app.logger.error(f"Error applying percentile filter on {column}: {str(e)}", exc_info=True)

        # Add sorting
        if sort_by in columns:
            sql += f" ORDER BY {sort_by} {sort_order.upper()}"
        else:
            app.logger.warning(f"Invalid sort column: {sort_by}")
            if columns:
                fallback_column = columns[0]
                sql += f" ORDER BY {fallback_column} {sort_order.upper()}"
            else:
                app.logger.warning("No columns found for sorting.")

        # Add pagination
        sql += " LIMIT ? OFFSET ?"
        params.extend([per_page + 1, offset])

        # Store the generated SQL query
        generated_sql = sql
        app.logger.debug(f"Generated SQL: {generated_sql} with params: {params}")

        # Execute the query
        cursor.execute(sql, params)
        rows = cursor.fetchall()
        has_more = len(rows) > per_page
        rows = rows[:per_page]

        return {
            "columns": columns,
            "rows": rows,
            "page": page,
            "has_more": has_more,
            "sort_by": sort_by,
            "sort_order": sort_order,
            "column_types": column_types,
            "column_search_queries": column_search_queries,
            "ne_classes": ne_classes,
            "selected_classes": selected_classes,
            "error_codes": error_codes,
            "selected_errors": selected_errors,
            "numeric_filters": numeric_filters,  # Pass numeric filter state to template
            "generated_sql": generated_sql,
            "searchable_id_columns": searchable_id_columns,
            "show_overlap": show_overlap,  # Pass the filter state to template
            "has_overlap_column": 'OVERLAP' in columns,  # Tell template if OVERLAP exists
            "filter_no_errors": filter_no_errors  # Pass the "No Errors Only" filter state
        }
    except Exception as e:
        app.logger.error(f"Error displaying table {table_name}: {e}", exc_info=True)
        return {"error": str(e)}


@app.route("/table/<table_name>")
def table_view(table_name):
    result = display_table(table_name)
    if "error" in result:
        return render_template("error.html", message=result["error"]), 500
    return render_template("table_view.html", table_name=table_name, **result)


@app.route("/indexes")
def show_indexes():
    try:
        db = get_db_simple_connection()
        indexes_info = {}
        indexed_columns_by_table = {}
        table_schemas = {}

        # Get list of tables
        tables = db.execute("SELECT name FROM sqlite_master WHERE type='table'")

        for table in tables:
            table_name = table[0]
            indexed_columns = set()
            # Get indexes for each table
            indexes = db.execute(
                f"SELECT * FROM sqlite_master WHERE type='index' AND tbl_name=?",
                [table_name],
            )

            indexes_info[table_name] = []
            for idx in indexes:
                index_info = {
                    "name": idx[1],
                    "sql": idx[4],
                    "columns": _parse_index_columns(idx[4]),
                }
                indexes_info[table_name].append(index_info)
                indexed_columns.update(index_info["columns"])  # Collect indexed columns

            indexed_columns_by_table[table_name] = (
                indexed_columns  # Store indexed columns
            )

        return render_template(
            "indexes.html",
            indexes=indexes_info,
            table_schemas=table_schemas,
            indexed_columns_by_table=indexed_columns_by_table,
        )  # Pass indexed columns to the template
    except Exception as e:
        app.logger.error(f"Error loading indexes: {e}")
        return render_template("error.html", message="Error loading indexes"), 500


def _parse_index_columns(create_sql):
    """Extract column names from CREATE INDEX statement"""
    if not create_sql:
        return []
    try:
        # Find text between parentheses
        import re

        match = re.search(r"\((.*?)\)", create_sql)
        if match:
            # Split columns and clean up
            return [col.strip() for col in match.group(1).split(",")]
    except Exception:
        pass
    return []


@app.route("/aggregated-entity-occurrences")
def list_aggregated_entity_occurrences():
    result = display_table("view_entity_occurrences_summary")
    if "error" in result:
        return render_template("error.html", message=result["error"]), 500
    return render_template(
        "table_view.html", table_name="view_entity_occurrences_summary", **result
    )


import re


@app.route("/explain-query")
def explain_query():
    try:
        query = request.args.get("query", "").strip()
        app.logger.debug(f"Original query received: {query}")

        if not query:
            return jsonify({"error": "No query provided"}), 400

        # Basic validation and extraction of query
        query_lower = query.lower()
        if query_lower.startswith('create view'):
            try:
                app.logger.debug("Processing CREATE VIEW statement")
                # Extract the query part after "AS" and clean it up
                match = re.search(r'create\s+view\s+.*?\s+as\s+(.*)', query, re.IGNORECASE | re.DOTALL)
                if not match:
                    app.logger.error("Could not extract query from CREATE VIEW statement")
                    return jsonify({"error": "Invalid CREATE VIEW syntax. Could not find query after AS."}), 400
                query = match.group(1).strip()
                app.logger.debug(f"Extracted query from CREATE VIEW: {query}")

                # Clean up the query by removing newlines and extra spaces
                query = ' '.join(query.split())
                app.logger.debug(f"Cleaned query: {query}")
            except Exception as e:
                app.logger.error(f"Error parsing CREATE VIEW statement: {e}")
                return jsonify({"error": f"Error parsing CREATE VIEW statement: {str(e)}"}), 400
        elif not query_lower.startswith('select'):
            app.logger.error(f"Invalid query type: {query_lower[:20]}...")
            return jsonify({"error": "Invalid query. Only CREATE VIEW and SELECT statements are allowed."}), 400

        db = get_db_simple_connection()

        # Wrap the query in a transaction that we'll roll back
        db.cursor.execute("BEGIN")
        try:
            app.logger.debug("Executing EXPLAIN QUERY PLAN")
            db.cursor.execute(f"EXPLAIN QUERY PLAN {query}")
            rows = db.cursor.fetchall()
            app.logger.debug(f"Got {len(rows)} rows from EXPLAIN QUERY PLAN")

            column_names = [col[0] for col in db.cursor.description]
            app.logger.debug(f"Column names: {column_names}")

            result = [dict(zip(column_names, row)) for row in rows]
            app.logger.debug(f"Final result: {result}")

            db.cursor.execute("ROLLBACK")

            if not result:
                return jsonify([{"id": 0, "parent": 0, "notused": 0, "detail": "Simple query - no complex plan needed"}])

            return jsonify(result)

        except Exception as e:
            db.cursor.execute("ROLLBACK")  # Ensure we rollback on error
            error_msg = str(e)
            app.logger.error(f"Query execution error: {error_msg}")
            app.logger.error(f"Failed query: {query}")

            if "syntax error" in error_msg.lower():
                return jsonify({"error": f"Invalid SQL syntax: {error_msg}"}), 400
            elif "no such table" in error_msg.lower():
                return jsonify({"error": f"Table not found: {error_msg}"}), 400
            elif "no such column" in error_msg.lower():
                return jsonify({"error": f"Column not found: {error_msg}"}), 400
            else:
                return jsonify({"error": f"Error analyzing query: {error_msg}"}), 400

    except Exception as e:
        app.logger.error(f"Unexpected error in explain_query: {e}", exc_info=True)
        return jsonify({"error": f"An unexpected error occurred: {str(e)}"}), 500


@app.route("/table/<table_name>/delete", methods=["POST"])
def delete_table(table_name):
    db = None
    try:
        db = get_db_simple_connection()
        app.logger.info(f"Attempting to delete table: {table_name}")
        cursor = db.cursor

        # Start transaction
        cursor.execute("BEGIN")

        # Check if it's actually a table first
        table_check = cursor.execute(
            "SELECT type, sql FROM sqlite_master WHERE type='table' AND name=?", [table_name]
        ).fetchall()

        if not table_check:
            app.logger.warning(f"Attempted to delete non-existent table: {table_name}")
            cursor.execute("ROLLBACK")
            return jsonify({"error": "Table not found"}), 404

        table_info = table_check[0]

        # Additional safety checks
        if table_name.startswith('sqlite_'):
            app.logger.warning(f"Attempted to delete system table: {table_name}")
            cursor.execute("ROLLBACK")
            return jsonify({"error": "Cannot delete system tables"}), 403

        # Check for dependent views
        dependent_views = cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='view' AND sql LIKE ?",
            [f'%{table_name}%']
        ).fetchall()

        if dependent_views:
            view_names = [view[0] for view in dependent_views]
            app.logger.warning(f"Cannot delete table {table_name} due to dependent views: {view_names}")
            cursor.execute("ROLLBACK")
            return jsonify({
                "error": f"Cannot delete table due to dependent views: {', '.join(view_names)}"
            }), 409

        # Save table schema for logging
        table_schema = table_info[1]

        # Get row count before deletion
        count_result = cursor.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        row_count = count_result[0] if count_result else 0

        # Execute deletion
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")

        # Commit transaction
        cursor.execute("COMMIT")

        # Log successful deletion
        app.logger.info(
            f"Successfully deleted table {table_name}. "
            f"Rows deleted: {row_count}. Schema was: {table_schema}"
        )

        return jsonify({
            "message": "Table deleted successfully",
            "rows_affected": row_count
        }), 200

    except Exception as e:
        app.logger.error(f"Error deleting table {table_name}: {str(e)}", exc_info=True)
        # Only try to rollback if we have a cursor and transaction is active
        if db is not None and db.cursor is not None:
            try:
                db.cursor.execute("ROLLBACK")
            except Exception as rollback_error:
                app.logger.error(f"Error during rollback: {rollback_error}")
        return jsonify({"error": str(e)}), 500

@app.route("/view/<view_name>/delete", methods=["POST"])
def delete_view(view_name):
    db = None
    try:
        db = get_db_easyner()
        app.logger.info(f"Attempting to delete view: {view_name}")
        cursor = db.cursor

        # Start transaction
        cursor.execute("BEGIN")

        # Check if it's actually a view first
        view_check = cursor.execute(
            "SELECT type, sql FROM sqlite_master WHERE type='view' AND name=?", [view_name]
        ).fetchall()

        if not view_check:
            app.logger.warning(f"Attempted to delete non-existent view: {view_name}")
            cursor.execute("ROLLBACK")
            return jsonify({"error": "View not found"}), 404

        view_info = view_check[0]
        # Save view definition for logging
        view_definition = view_info[1]

        # Check for dependent views
        dependent_views = cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='view' AND sql LIKE ? AND name != ?",
            [f'%{view_name}%', view_name]
        ).fetchall()

        if dependent_views:
            view_names = [view[0] for view in dependent_views]
            app.logger.warning(f"Cannot delete view {view_name} due to dependent views: {view_names}")
            cursor.execute("ROLLBACK")
            return jsonify({
                "error": f"Cannot delete view due to dependent views: {', '.join(view_names)}"
            }), 409

        # Execute deletion
        cursor.execute(f"DROP VIEW IF EXISTS {view_name}")

        # Commit transaction
        cursor.execute("COMMIT")

        # Log successful deletion
        app.logger.info(
            f"Successfully deleted view {view_name}. "
            f"View definition was: {view_definition}"
        )

        return jsonify({
            "message": "View deleted successfully"
        }), 200

    except Exception as e:
        app.logger.error(f"Error deleting view {view_name}: {str(e)}", exc_info=True)
        # Only try to rollback if we have a cursor and transaction is active
        if db is not None and db.cursor is not None:
            try:
                db.cursor.execute("ROLLBACK")
            except Exception as rollback_error:
                app.logger.error(f"Error during rollback: {rollback_error}")
        return jsonify({"error": str(e)}), 500

@app.route("/execute-query", methods=["POST"])
def execute_query():
    try:
        db = get_db_simple_connection()
        query = request.json.get("query", "").strip()

        if not query:
            return jsonify({"error": "No query provided"}), 400

        # Only allow SELECT queries for safety
        # if not query.lower().startswith('select'):
        #     return jsonify({"error": "Only SELECT queries are allowed"}), 400

        # Execute the query with a row limit for safety
        modified_query = f"{query} LIMIT 1000"
        results = db.execute(modified_query)

        # Get column names
        columns = [description[0] for description in db.cursor.description]

        # Convert rows to list of dictionaries
        rows = [dict(zip(columns, row)) for row in results]

        return jsonify({
            "columns": columns,
            "rows": rows,
            "rowCount": len(rows),
            "truncated": len(rows) == 1000
        })
    except Exception as e:
        app.logger.error(f"Query execution error: {e}")
        return jsonify({"error": str(e)}), 500


def validate_view(db, view_name):
    """
    Validate a view by attempting to query it and checking its structure.
    Returns a tuple of (is_valid, error_message).
    """
    try:
        # Try to get one row from the view to validate it
        cursor = db.cursor
        cursor.execute(f"SELECT * FROM {view_name} LIMIT 1")

        # Even if no rows, getting here means view is structurally valid
        columns = [description[0] for description in cursor.description]
        return True, {"columns": columns}
    except Exception as e:
        error_msg = str(e)

        # Check for specific error types
        if "no such table" in error_msg.lower():
            return False, {"error": "View definition references non-existent tables or views"}
        elif "no such column" in error_msg.lower():
            return False, {"error": "View definition references non-existent columns"}
        else:
            return False, {"error": f"View error: {error_msg}"}


@app.route("/api/view/<view_name>/validate")
def validate_view_api(view_name):
    try:
        db = get_db_simple_connection()

        # First check if view exists
        view_check = db.execute(
            "SELECT sql FROM sqlite_master WHERE type='view' AND name=?",
            [view_name]
        ).fetchone()

        if not view_check:
            return jsonify({
                "exists": False,
                "error": "View does not exist"
            }), 404

        # Validate the view
        is_valid, validation_result = validate_view(db, view_name)

        return jsonify({
            "exists": True,
            "is_valid": is_valid,
            "validation_result": validation_result,
            "definition": view_check[0]
        })

    except Exception as e:
        app.logger.error(f"Error validating view {view_name}: {e}")
        return jsonify({
            "exists": True,
            "is_valid": False,
            "error": str(e)
        }), 500


@app.route("/table/<table_name>/schema")
def get_table_schema(table_name):
    try:
        db = get_db_easyner()
        # Get table creation SQL
        result = db.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
            [table_name]
        )

        # Check if we have results and handle them properly
        if not result or len(result) == 0:
            return jsonify({"error": f"Table {table_name} not found or has no schema"}), 404

        create_sql = result[0][0]  # Access the first column of the first row

        if not create_sql:
            return jsonify({"error": f"Table {table_name} exists but has no schema definition"}), 404

        return jsonify({"schema": create_sql})
    except Exception as e:
        app.logger.error(f"Error getting schema for {table_name}: {e}")
        return jsonify({"error": f"Error retrieving schema: {str(e)}"}), 500


@app.route("/dis-pnm-presentation")
def dis_pnm_presentation():
    """Display the DIS-PNM Presentation view."""
    try:
        result = display_table(VIEW_DIS_PNM_CO_AGGR_ROW_FACTORY)
        if "error" in result:
            return render_template("error.html", message=result["error"]), 500
        return render_template("table_view.html", table_name=VIEW_DIS_PNM_CO_AGGR_ROW_FACTORY, **result)
    except Exception as e:
        app.logger.error(f"Error loading DIS-PNM Presentation: {e}")
        return render_template("error.html", message="Error loading DIS-PNM Presentation"), 500

@app.route("/ne-presentation")
def ne_presentation():
    """Display the NE Presentation view."""
    try:
        result = display_table("v_NE_PRESENTATION")
        if "error" in result:
            return render_template("error.html", message=result["error"]), 500
        return render_template("table_view.html", table_name="v_NE_PRESENTATION", **result)
    except Exception as e:
        app.logger.error(f"Error loading NE Presentation: {e}")
        return render_template("error.html", message="Error loading NE Presentation"), 500

@app.route("/clear-visualization-cache", methods=["POST"])
def clear_visualization_cache():
    """Clear all visualization cache files and in-memory caches."""
    try:
        app.logger.info("Clear visualization cache request received")

        # Get cache stats before clearing for logging purposes
        stats_before = visualization_manager.get_cache_stats()

        # Clear the cache
        cleared_count = visualization_manager.clear_cache()

        # Get updated cache stats to verify clearing worked
        stats_after = visualization_manager.get_cache_stats()

        app.logger.info(
            f"Visualization cache cleared successfully. "
            f"Removed {cleared_count} cache files. "
            f"Before: {stats_before['file_cache_count']} files ({_format_size(stats_before['cache_size_bytes'])}), "
            f"After: {stats_after['file_cache_count']} files ({_format_size(stats_after['cache_size_bytes'])})"
        )

        return jsonify({
            "status": "success",
            "message": f"Cleared {cleared_count} visualization cache files",
            "details": {
                "cleared_count": cleared_count,
                "before": stats_before,
                "after": stats_after
            }
        }), 200

    except Exception as e:
        app.logger.error(f"Error clearing visualization cache: {e}", exc_info=True)
        return jsonify({
            "status": "error",
            "message": f"Failed to clear visualization cache: {str(e)}"
        }), 500

@app.route("/visualization-cache-stats")
def visualization_cache_stats():
    """Get statistics about the visualization cache."""
    try:
        stats = visualization_manager.get_cache_stats()

        # Format the stats for display
        formatted_stats = {
            "in_memory_cache_count": stats["in_memory_cache_count"],
            "file_cache_count": stats["file_cache_count"],
            "cache_size": _format_size(stats["cache_size_bytes"]),
            "cache_items": []
        }

        # Format each cache item
        for item in stats["cache_items"]:
            formatted_stats["cache_items"].append({
                "name": item["name"],
                "size": _format_size(item["size_bytes"]),
                "last_modified": time.strftime(
                    "%Y-%m-%d %H:%M:%S",
                    time.localtime(item["last_modified"])
                )
            })

        return jsonify(formatted_stats)
    except Exception as e:
        app.logger.error(f"Error getting visualization cache stats: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/reload-server", methods=["POST"])
def reload_server():
    """Reload modules and refresh the server without restarting the process."""
    try:
        app.logger.info("Reload server request received")

        # Get modules to reload - we're particularly interested in our own modules
        import sys
        import importlib

        # Track detailed reload information for the response
        reload_details = {
            "attempted": 0,
            "reloaded": 0,
            "failed": 0,
            "skipped": 0,
            "successful_modules": [],
            "failed_modules": {},
            "skipped_modules": []
        }

        # Collect all modules that are part of our application
        modules_to_reload = []
        for module_name, module in list(sys.modules.items()):
            # Filter for our application modules
            if module_name.startswith('scripts.') or module_name == 'scripts':
                modules_to_reload.append(module_name)
                reload_details["attempted"] += 1

        # Log which modules we'll reload
        app.logger.info(f"Preparing to reload {len(modules_to_reload)} modules")

        # Sort modules by dependency order (parent modules after their children)
        # This helps with proper reloading order
        modules_to_reload.sort(key=lambda m: -m.count('.'))

        # Reload collected modules
        for module_name in modules_to_reload:
            try:
                if module_name in sys.modules:
                    # Check if module has reload capability (some built-ins don't)
                    if hasattr(sys.modules[module_name], '__file__') and sys.modules[module_name].__file__:
                        # Perform the actual reload
                        importlib.reload(sys.modules[module_name])
                        reload_details["reloaded"] += 1
                        reload_details["successful_modules"].append(module_name)
                        app.logger.debug(f"Reloaded module: {module_name}")
                    else:
                        # Skip modules without a file (built-ins or C extensions)
                        reload_details["skipped"] += 1
                        reload_details["skipped_modules"].append(module_name)
                        app.logger.debug(f"Skipped module (no file): {module_name}")
            except Exception as e:
                reload_details["failed"] += 1
                reload_details["failed_modules"][module_name] = str(e)
                app.logger.error(f"Error reloading module {module_name}: {e}")

        # Recompile SCSS files
        scss_status = "failed"
        try:
            compile_scss()
            scss_status = "successful"
            app.logger.info("Recompiled SCSS files")
        except Exception as e:
            app.logger.error(f"Error recompiling SCSS: {e}")

        # We skip re-initializing routes to avoid the endpoint overwriting issue
        # Routes have already been registered and will use the reloaded module code
        app.logger.info(f"Server reload completed. "
                        f"Reloaded {reload_details['reloaded']} modules, "
                        f"skipped {reload_details['skipped']}, "
                        f"failed {reload_details['failed']}.")

        # Return detailed information about the reload process
        return jsonify({
            "status": "success",
            "message": f"Server reloaded. Refreshed {reload_details['reloaded']} modules.",
            "details": {
                "modules": {
                    "attempted": reload_details["attempted"],
                    "reloaded": reload_details["reloaded"],
                    "failed": reload_details["failed"],
                    "skipped": reload_details["skipped"]
                },
                "successful_modules": reload_details["successful_modules"],
                "failed_modules": reload_details["failed_modules"],
                "skipped_modules": reload_details["skipped_modules"],  # Add this line
                "scss_compilation": scss_status
            }
        }), 200

    except Exception as e:
        app.logger.error(f"Error during server reload: {e}", exc_info=True)
        return jsonify({
            "status": "error",
            "message": f"Failed to reload server: {str(e)}"
        }), 500

@app.route("/dev/reload")
def dev_reload_page():
    """Developer page with reload server button."""
    return render_template("dev_reload.html",
                          server_status="Running",
                          modules_count=len([m for m in sys.modules if m.startswith('scripts.')]))

@app.route("/monitor")
def show_monitoring_status():
    """Display monitoring stats for administrators"""
    try:
        # Check for active connections
        connections = connection_monitor.get_open_connections()

        # Check for leaked connections
        leaks = connection_monitor.check_for_leaks(
            age_threshold=float(os.environ.get('EASYNER_CONNECTION_LEAK_THRESHOLD', '300.0'))
        )

        # Check for stuck threads
        stuck_threads = thread_monitor.check_for_stuck_threads(
            heartbeat_threshold=float(os.environ.get('EASYNER_THREAD_STUCK_THRESHOLD', '60.0'))
        )

        # Get active threads
        active_threads = thread_monitor.get_active_threads()

        # Get resource stats
        resource_stats = {
            'memory': operation_monitor._get_memory_usage(),
            'cpu': operation_monitor._get_cpu_usage(),
            'disk': operation_monitor._get_disk_usage()
        }

        # Get operation stats
        operation_stats = operation_monitor._operation_counts

        # Get open transactions
        open_transactions = operation_monitor.get_open_transactions()

        return render_template(
            "monitoring.html",
            connections=connections,
            leaks=leaks,
            stuck_threads=stuck_threads,
            active_threads=active_threads,
            resource_stats=resource_stats,
            operation_stats=operation_stats,
            open_transactions=open_transactions
        )
    except Exception as e:
        app.logger.error(f"Error displaying monitoring status: {e}")
        operation_monitor.monitor_exception(e, context={'route': '/monitor'})
        return render_template("error.html", message="Error accessing monitoring data"), 500

# Import route implementations
from .routes import init_routes

# Initialize all routes from the routes module
init_routes(app, get_db_easyner, visualization_manager)

if __name__ == "__main__":
    import os
    import signal
    import tempfile

    # Set up command-line argument parsing
    parser = argparse.ArgumentParser(description='EasyNer DB Server')
    parser.add_argument('--port', type=int, help='Port to run the server on')
    args = parser.parse_args()

    # Create PID file for process tracking
    pid = os.getpid()
    pid_dir = os.path.join(tempfile.gettempdir(), 'easyner')
    os.makedirs(pid_dir, exist_ok=True)
    pid_file = os.path.join(pid_dir, 'db_server.pid')

    # Check for existing PID file (orphaned process)
    if os.path.exists(pid_file):
        with open(pid_file, 'r') as f:
            old_pid = int(f.read().strip())
            try:
                # Check if process exists and terminate it
                if psutil.pid_exists(old_pid):
                    old_process = psutil.Process(old_pid)
                    if "python" in old_process.name().lower():
                        app.logger.info(f"Terminating orphaned server process: {old_pid}")
                        old_process.terminate()
                        try:
                            old_process.wait(timeout=3)
                        except psutil.TimeoutExpired:
                            old_process.kill()
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                app.logger.info(f"Old PID {old_pid} no longer exists or not accessible")

    # Write current PID to file
    with open(pid_file, 'w') as f:
        f.write(str(pid))

    # Cleanup function for proper server shutdown
    def cleanup_server():
        app.logger.info("Cleaning up server resources...")
        try:
            if os.path.exists(pid_file):
                os.unlink(pid_file)
        except Exception as e:
            app.logger.error(f"Error cleaning up PID file: {e}")

        # Close all database connections
        close_db_connections()

    # Register the cleanup function
    atexit.register(cleanup_server)

    # Handle termination signals gracefully
    def signal_handler(sig, frame):
        app.logger.info(f"Received signal {sig}, shutting down server...")
        cleanup_server()
        os._exit(0)

    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    if hasattr(signal, 'SIGUSR1'):
        signal.signal(signal.SIGUSR1, signal_handler)
    if hasattr(signal, 'SIGUSR2'):
        signal.signal(signal.SIGUSR2, signal_handler)

    # Determine the port (command line > environment variable > default)
    port = args.port if args.port else int(os.environ.get('EASYNER_SERVER_PORT', 5001))

    # Check if the port is available
    s = socket(AF_INET, SOCK_STREAM)
    port_in_use = False
    try:
        s.bind(('127.0.0.1', port))
    except OSError:
        port_in_use = True
    finally:
        s.close()

    # If port is in use and not explicitly specified, find an available one
    if port_in_use and not args.port:
        app.logger.warning(f"Port {port} is in use. Trying alternative ports...")
        for test_port in range(5002, 5020):
            s = socket(AF_INET, SOCK_STREAM)
            try:
                s.bind(('127.0.0.1', test_port))
                port = test_port
                port_in_use = False
                app.logger.info(f"Found available port: {port}")
                break
            except OSError:
                pass
            finally:
                s.close()

    # Start the server if we found an available port
    if not port_in_use:
        with app.app_context():
            try:
                # Initialize database schema before running the server
                align_with_schema()

                app.logger.info(f"Starting Flask server on port {port}...")

                app.run(
                    host="127.0.0.1",
                    port=port,
                    debug=True,
                    use_reloader=False,  # Disable reloader to prevent duplicate processes
                    threaded=True,
                )
            except Exception as e:
                app.logger.error(f"Server error: {e}")
                cleanup_server()  # Ensure cleanup happens on error
                raise
    else:
        print(f"ERROR: Port {port} is already in use. To fix:")
        print(f"1. Find and stop the process using port {port}:")
        print(f"   $ lsof -i :{port}    # Find the process ID")
        print(f"   $ kill <PID>         # Stop the process")
        print("2. Or specify a different port:")
        print(f"   $ python db_server.py --port 5002")
        print("3. Or set environment variable:")
        print(f"   $ export EASYNER_SERVER_PORT=5002 && python db_server.py")