from flask import Flask, jsonify, g, render_template, request
from .db_main import EasyNerDBHandler
from .data_model import entities
import os
import sass
from .statistics.sankey_diagram import create_disease_phenomena_sankey
from .statistics.visualization_manager import VisualizationManager
import logging
from logging.handlers import RotatingFileHandler
from .data_model.schema import *
from .core.core_methods import *


def setup_logging(app):
    """Configure logging for the application"""
    # Create logs directory if it doesn't exist
    log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
    os.makedirs(log_dir, exist_ok=True)

    # Set up file handler
    log_file = os.path.join(log_dir, 'server.log')
    print(f"Logging to: {log_file}")
    file_handler = RotatingFileHandler(log_file, maxBytes=1024 * 1024, backupCount=10)
    file_handler.setFormatter(logging.Formatter(
        '[%(asctime)s] %(levelname)s in %(module)s: %(message)s'
    ))
    file_handler.setLevel(logging.INFO)

    # Add handler to app logger
    app.logger.addHandler(file_handler)
    app.logger.setLevel(logging.INFO)

    app.logger.info('Logging setup completed')

# Set template directory to current directory/templates
template_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates")
app = Flask(__name__, template_folder=template_dir)
setup_logging(app)

# Initialize single database connection
visualization_manager = VisualizationManager(app)
db_instance = None


def init_db():
    """Initialize the database connection"""
    global db_instance
    if db_instance is None:
        try:
            db_instance = EasyNerDBHandler()
            db_instance.logger.info("Database connection initialized")
        except Exception as e:
            app.logger.error(f"Database initialization error: {e}")
            raise
    return db_instance


# Initialize database on startup
with app.app_context():
    init_db()


def get_db():
    """Get the database connection"""
    global db_instance
    if db_instance is None:
        db_instance = init_db()
    return db_instance


@app.teardown_appcontext
def cleanup(e=None):
    """Only close the database connection when the app is shutting down"""
    global db_instance
    if db_instance is not None:
        try:
            db_instance.close()
            db_instance.logger.info("Database connection closed on shutdown")
            db_instance = None
        except Exception as e:
            app.logger.error(f"Error closing database connection: {e}")


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
    try:
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

    except Exception as e:
        app.logger.error(f"Error initializing database: {e}")
        raise


    app.logger.info("Flask application initialized")
    return db


def get_available_entities(db):
    try:
        return db.execute(
            f"SELECT {CLASS_ID}, {NE_CLASS} FROM {TABLE_NE_CLASS} ORDER BY {NE_CLASS}"
        )
    except Exception as e:
        app.logger.error(f"Error fetching entities: {e}")
        return []


@app.route("/")
def home():
    try:
        db = get_db()
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
    try:
        db = get_db()
        tables_info = {}

        # Get counts for each table
        for table in db.tables["tables"]:
            count = db.execute(f"SELECT COUNT(*) FROM {table}")[0][0]
            columns = db.execute(f"PRAGMA table_info({table})")
            tables_info[table] = {
                "row_count": count,
                "columns": [col[1] for col in columns],
            }

        return (
            jsonify(
                {
                    "status": "healthy",
                    "database": {"connected": True, "tables": tables_info},
                    "server_status": "running",
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
        return jsonify({"status": "unhealthy", "error": str(e)}), 500

@app.route("/documents")
def list_documents():
    try:
        db = get_db()
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
                    WHERE eo{entity_id}.{DOC_ID} = d.{DOC_ID} AND ne{entity_id}.{DOC_ID} = ?
                )"""
                conditions.append(sql_condition)
                params.append(entity_id)

        if conditions:
            sql += " WHERE " + " AND ".join(conditions)

        sql += f" ORDER BY d.{DOC_ID} LIMIT ? OFFSET ?"
        params.extend([per_page + 1, offset])

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
        db = get_db()
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
    db = get_db()

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
        db = get_db()
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
        db = get_db()
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
        db = get_db()
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
        db = get_db()
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
        db = get_db()
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
        db = get_db()
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
        app.logger.error(f"Error loading entity co-occurrences summary plot data: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/summary-cooccurrences/plot-data")
def summary_cooccurrences_plot_data():
    try:
        db = get_db()
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
        db = get_db()

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
        db = get_db()
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
        db = get_db()
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
        db = get_db()
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
        db = get_db()
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
        db = get_db()
        count = db.execute(f"SELECT COUNT(*) FROM {table_name}")[0][0]
        return jsonify({"row_count": count})
    except Exception as e:
        app.logger.error(f"Error getting row count for {table_name}: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/tables/<table_name>")
def view_table(table_name):
    try:
        db = get_db()
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
        db = get_db()
        page = int(request.args.get("page", 1))
        per_page = int(request.args.get("per_page", 30))
        offset = (page - 1) * per_page
        sort_by = request.args.get("sort_by", "id")
        sort_order = request.args.get("sort_order", "asc")
        search_query = request.args.get("search_query", "")

        # Get column information dynamically
        db.cursor.execute(f"PRAGMA table_info({table_name})")
        columns_info = db.cursor.fetchall()
        columns = [col[1] for col in columns_info]
        column_types = {
            col[1]: col[2].upper() for col in columns_info
        }  # Store column types
        text_columns = [col[1] for col in columns_info if col[2].upper() == "TEXT"]

        # Extract search queries for each column
        column_search_queries = {
            col: request.args.get(f"{col}_search", "") for col in text_columns
        }

        # Build the base SQL query
        sql = f"SELECT * FROM {table_name}"
        params = []

        # Add search functionality for TEXT columns
        search_conditions = []
        for col, query in column_search_queries.items():
            if query:
                search_conditions.append(f"{col} LIKE ?")
                params.append(f"%{query}%")

        if search_conditions:
            sql += " WHERE " + " AND ".join(search_conditions)

        # Add sorting
        if sort_by in columns:
            sql += f" ORDER BY {sort_by} {sort_order.upper()}"
        else:
            app.logger.warning(f"Invalid sort column: {sort_by}")
            if columns:
                fallback_column = columns[0]
                sql += f" ORDER BY {fallback_column} {sort_order.upper()}"
            else:
                # No columns available, so just skip sorting
                app.logger.warning("No columns found for sorting.")

        # Add pagination
        sql += " LIMIT ? OFFSET ?"
        params.extend([per_page + 1, offset])

        # Store the generated SQL query
        generated_sql = sql

        # Execute the query
        db.cursor.execute(sql, params)
        rows = db.cursor.fetchall()
        has_more = len(rows) > per_page
        rows = rows[:per_page]

        return {
            "columns": columns,
            "rows": rows,
            "page": page,
            "has_more": has_more,
            "sort_by": sort_by,
            "sort_order": sort_order,
            "column_types": column_types,  # Pass column types to the template
            "column_search_queries": column_search_queries,  # Pass search queries to the template
        }
    except Exception as e:
        app.logger.error(f"Error displaying table {table_name}: {e}")
        return {"error": str(e)}



@app.route("/table/<table_name>")
def table_view(table_name):
    result = display_table(table_name)
    if "error" in result:
        return render_template("error.html", message=result["error"]), 500
    return render_template("table_view.html", table_name=table_name, **result)


@app.route("/disease-phenomena-sankey")
def disease_phenomena_sankey():
    try:
        db = get_db()
        # Get filter parameters from request, only pass non-empty values
        disease_search = request.args.get('disease_search', '').strip() or None
        phenomenon_search = request.args.get('phenomenon_search', '').strip() or None
        min_pmi = request.args.get('min_pmi', '5.0').strip()  # Default to 5.0
        max_pmi = request.args.get('max_pmi', '').strip()
        min_fq = request.args.get('min_fq', '').strip()
        max_fq = request.args.get('max_fq', '').strip()
        limit = request.args.get('limit', '30').strip()

        # Get numeric filters with validation, convert empty strings to None
        try:
            min_pmi = float(min_pmi) if min_pmi else 5.0  # Default to 5.0 if empty
            max_pmi = float(max_pmi) if max_pmi else None
            min_fq = float(min_fq) if min_fq else None
            max_fq = float(max_fq) if max_fq else None
            limit_val = int(limit) if limit else 30
            # Ensure limit is between 1 and 100
            limit_val = max(1, min(100, limit_val))
        except ValueError as e:
            app.logger.error(f"Invalid numeric filter value: {e}")
            return render_template("error.html", message="Invalid numeric filter value"), 400

        # Create a unique cache key based on actual filter values
        filters = []
        if disease_search: filters.append(f"d_{disease_search}")
        if phenomenon_search: filters.append(f"p_{phenomenon_search}")
        if min_pmi != 5.0: filters.append(f"minp_{min_pmi}")  # Only include if different from default
        if max_pmi is not None: filters.append(f"maxp_{max_pmi}")
        if min_fq is not None: filters.append(f"minf_{min_fq}")
        if max_fq is not None: filters.append(f"maxf_{max_fq}")
        if limit_val != 30: filters.append(f"lim_{limit_val}")  # Only include if different from default

        # If no filters are applied or only default values are used, use 'base' as the cache key
        cache_key = 'sankey_' + ('base' if not filters else '_'.join(filters))

        return visualization_manager.get_cached_visualization(
            cache_key,
            db,
            lambda db: create_disease_phenomena_sankey(
                db,
                disease_search=disease_search,
                phenomenon_search=phenomenon_search,
                min_pmi=min_pmi,
                max_pmi=max_pmi,
                min_fq=min_fq,
                max_fq=max_fq,
                limit=limit_val
            )
        )
    except Exception as e:
        app.logger.error(f"Error in disease phenomena sankey route: {e}")
        return render_template("error.html", message="Error generating Sankey diagram"), 500


@app.route("/disease-phenomena")
def disease_phenomena_page():
    try:
        db = get_db()
        # Get filter parameters
        disease_search = request.args.get('disease_search', '').strip()
        phenomenon_search = request.args.get('phenomenon_search', '').strip()
        min_pmi = request.args.get('min_pmi', '5.0').strip()  # Default to 5.0
        max_pmi = request.args.get('max_pmi', '').strip()
        min_fq = request.args.get('min_fq', '').strip()
        max_fq = request.args.get('max_fq', '').strip()
        limit = request.args.get('limit', '30').strip()

        # Get the Sankey diagram HTML
        try:
            limit_val = int(limit) if limit else 30
            # Ensure limit is between 1 and 100
            limit_val = max(1, min(100, limit_val))

            sankey_html = create_disease_phenomena_sankey(
                db,
                disease_search=disease_search or None,
                phenomenon_search=phenomenon_search or None,
                min_pmi=float(min_pmi) if min_pmi else 5.0,  # Default to 5.0 if empty
                max_pmi=float(max_pmi) if max_pmi else None,
                min_fq=float(min_fq) if min_fq else None,
                max_fq=float(max_fq) if max_fq else None,
                limit=limit_val
            )
        except ValueError as e:
            app.logger.error(f"Invalid numeric filter value: {e}")
            sankey_html = "<div class='alert alert-danger'>Invalid numeric filter value</div>"
        except Exception as e:
            app.logger.error(f"Error generating Sankey diagram: {e}")
            sankey_html = f"<div class='alert alert-danger'>Error generating visualization: {str(e)}</div>"

        # Render template with both filters and diagram
        return render_template(
            "disease_phenomena.html",
            disease_search=disease_search,
            phenomenon_search=phenomenon_search,
            min_pmi=min_pmi,  # Pass the original or default value
            max_pmi=max_pmi,
            min_fq=min_fq,
            max_fq=max_fq,
            limit=limit_val if 'limit_val' in locals() else 30,
            sankey_html=sankey_html
        )
    except Exception as e:
        app.logger.error(f"Error rendering disease phenomena page: {e}")
        return render_template("error.html", message="Error loading page"), 500


@app.route("/indexes")
def show_indexes():
    try:
        db = get_db()
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

        db = get_db()

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
    try:
        db = get_db()
        # Check if it's actually a table first
        table_check = db.execute(
            "SELECT type FROM sqlite_master WHERE type='table' AND name=?", [table_name]
        )
        if not table_check:
            return jsonify({"error": "Table not found"}), 404

        # Additional safety check - prevent deletion of system tables
        if table_name.startswith('sqlite_'):
            return jsonify({"error": "Cannot delete system tables"}), 403


    except Exception as e:
        app.logger.error(f"Error deleting table {table_name}: {str(e)}", exc_info=True)
        return jsonify({"error": str(e)}), 500

@app.route("/view/<view_name>/delete", methods=["POST"])
def delete_view(view_name):
    try:
        db = get_db()
        # Check if it's actually a view first
        view_check = db.execute(
            "SELECT type FROM sqlite_master WHERE type='view' AND name=?", [view_name]
        )
        if not view_check:
            return jsonify({"error": "View not found"}), 404

        }), 200

    except Exception as e:
        app.logger.error(f"Error deleting view {view_name}: {str(e)}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route("/execute-query", methods=["POST"])
def execute_query():
    try:
        db = get_db()
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
        db = get_db()

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
        db = get_db()
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


if __name__ == "__main__":
    with app.app_context():
        try:
            # Initialize database before running the server
            db = get_db()
            app.logger.info("Starting Flask server...")

            app.run(
                host="127.0.0.1",
                port=5001,
                debug=True,
                use_reloader=True,
                threaded=True,
            )
        except Exception as e:
            if "db" in locals():
                app.logger.error(f"Server error: {e}")
            raise
