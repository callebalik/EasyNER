from flask import Flask, jsonify, g, render_template, request
from db_main import EasyNerDBHandler
import os
from data_model import Document, Sentence, NamedEntity
import sass
import subprocess

# Set template directory to current directory/templates
template_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates')
app = Flask(__name__, template_folder=template_dir)

# Compile SCSS to CSS on server load
def compile_scss():
    """Compile SCSS files to CSS with support for partials and watching changes"""
    try:
        scss_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'static/styles.scss')
        css_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'static/styles.css')
        static_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'static')
        partials_dir = os.path.join(static_dir, 'partials')

        # Ensure the partials directory exists
        os.makedirs(partials_dir, exist_ok=True)

        # Read and compile the main SCSS file
        with open(scss_file, 'r') as f:
            scss_content = f.read()
            
        # Compile SCSS with include paths for partials
        css_content = sass.compile(
            string=scss_content,
            include_paths=[static_dir],
            output_style='compressed' if not app.debug else 'nested'
        )
        
        # Write the compiled CSS
        with open(css_file, 'w') as f:
            f.write(css_content)
            
        app.logger.info("SCSS compilation successful")
    except Exception as e:
        app.logger.error(f"Error compiling SCSS: {e}")
        raise

compile_scss()

# Serve static CSS file
@app.route('/static/styles.css')
def styles():
    return app.send_static_file('styles.css')

def get_db():
    if 'db' not in g:
        try:
            g.db = EasyNerDBHandler()
            g.db.logger.info("New database connection created")
        except Exception as e:
            g.db.logger.error(f"Database connection error: {e}")
            raise
    return g.db

@app.teardown_appcontext
def close_db(e=None):
    db = g.pop('db', None)
    if db is not None:
        try:
            db.close()
            db.logger.info("Database connection closed")
        except Exception as e:
            db.logger.error(f"Error closing database: {e}")

# Remove @app.before_first_request and initialize db in a context
def init_db():
    with app.app_context():
        db = get_db()
        try:
            # Use existing schema alignment method
            schema_path = os.path.join(os.path.dirname(__file__), 'schema.sql')
            db.align_with_schema(schema_path)
            
            # Apply indexes (they are idempotent with IF NOT EXISTS)
            with open(os.path.join(os.path.dirname(__file__), 'indexes.sql'), 'r') as f:
                indexes_sql = f.read()
                for statement in indexes_sql.split(';'):
                    if statement.strip():
                        try:
                            db.execute(statement)
                        except Exception as e:
                            db.logger.warning(f"Error applying index: {e}")
                            continue
            db.logger.info("Database indexes applied successfully")
            
        except Exception as e:
            db.logger.error(f"Error initializing database: {e}")
            raise
            
        db.logger.info("Flask application initialized")
        return db

def get_available_entities(db):
    try:
        return db.execute("SELECT id, named_entity FROM named_entities ORDER BY named_entity")
    except Exception as e:
        db.logger.error(f"Error fetching entities: {e}")
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
                "columns": [col[1] for col in columns]
            }
        
        # Get database statistics
        stats = {
            'db_size': _format_size(db.statistics.size),
            'source_size': _format_size(db.statistics.total_source_size),
            'compression_ratio': f"{db.statistics.compression_ratio:.2f}",
            'document_count': db.statistics.get_document_count(),
            'sentence_count': db.statistics.get_sentence_count(),
            'named_entity_count': db.statistics.get_named_entity_count()
        }
        
        return render_template('home.html', tables=tables_info, stats=stats)
    except Exception as e:
        db.logger.error(f"Error loading home page: {e}")
        return render_template('error.html', message="Error loading database information"), 500

def _format_size(size_bytes):
    """Convert size in bytes to human readable format."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024 or unit == 'TB':
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
                "columns": [col[1] for col in columns]
            }
        
        return jsonify({
            "status": "healthy",
            "database": {
                "connected": True,
                "tables": tables_info
            },
            "server_status": "running",
            "endpoints": {
                "/": "Health check and basic info",
                "/tables": "Detailed table information",
                "/document/<id>": "Get document by ID"
            }
        }), 200
    except Exception as e:
        db.logger.error(f"Health check failed: {e}")
        return jsonify({
            "status": "unhealthy",
            "error": str(e)
        }), 500

@app.route("/tables")
def get_tables():
    try:
        db = get_db()
        tables_info = {}
        
        for table in db.tables["tables"]:
            # Get column information
            columns = db.execute(f"PRAGMA table_info({table})")
            columns_info = [{
                "name": col[1],
                "type": col[2],
                "nullable": not col[3],
                "primary_key": bool(col[5])
            } for col in columns]
            
            # Get foreign keys
            foreign_keys = db.execute(f"PRAGMA foreign_key_list({table})")
            fk_info = [{
                "from": fk[3],
                "to_table": fk[2],
                "to_column": fk[4]
            } for fk in foreign_keys]
            
            # Get row count
            count = db.execute(f"SELECT COUNT(*) FROM {table}")[0][0]
            
            tables_info[table] = {
                "columns": columns_info,
                "foreign_keys": fk_info,
                "row_count": count
            }
        
        return jsonify(tables_info), 200
    except Exception as e:
        db.logger.error(f"Error getting table information: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/documents")
def list_documents():
    try:
        db = get_db()
        page = int(request.args.get('page', 1))
        query = request.args.get('query', '')
        doc_id = request.args.get('doc_id', '')
        selected_entities = request.args.getlist('entities')
        per_page = 30
        offset = (page - 1) * per_page

        params = []
        conditions = []

        if query:
            conditions.append("d.title LIKE ?")
            params.append(f'%{query}%')
        
        if doc_id:
            conditions.append("d.id = ?")
            params.append(doc_id)

        # Base query
        sql = """
            SELECT DISTINCT d.id, d.title, d.word_count 
            FROM documents d
        """

        # Add entity filtering - using EXISTS for each entity to ensure ALL are present
        if selected_entities:
            for entity_id in selected_entities:
                sql_condition = f"""
                EXISTS (
                    SELECT 1 FROM entity_occurrences eo{entity_id} 
                    JOIN named_entities ne{entity_id} ON eo{entity_id}.entity_id = ne{entity_id}.id
                    WHERE eo{entity_id}.document_id = d.id AND ne{entity_id}.id = ?
                )"""
                conditions.append(sql_condition)
                params.append(entity_id)

        if conditions:
            sql += " WHERE " + " AND ".join(conditions)

        sql += " ORDER BY d.id LIMIT ? OFFSET ?"
        params.extend([per_page + 1, offset])

        documents = db.execute(sql, params)
        has_more = len(documents) > per_page
        documents = documents[:per_page]  # Trim to per_page items

        # Get available entities for the filter
        entities = get_available_entities(db)

        return render_template('documents.html',
                             documents=[dict(zip(['id', 'title', 'word_count'], doc)) for doc in documents],
                             page=page,
                             query=query,
                             doc_id=doc_id,
                             has_more=has_more,
                             entities=entities,
                             selected_entities=selected_entities)
    except Exception as e:
        db.logger.error(f"Error loading documents page: {e}")
        return render_template('error.html', message="Error loading documents"), 500

@app.route("/named-entities")
def list_named_entities():
    try:
        db = get_db()
        page = int(request.args.get('page', 1))
        query = request.args.get('query', '')
        min_freq = request.args.get('min_freq', 0)
        per_page = 30
        offset = (page - 1) * per_page

        sql = """
            SELECT ne.id, ne.named_entity, 
                   eos.fq as fq_document_level, 
                   eos.fq as fq_sentence_level
            FROM named_entities ne
            LEFT JOIN entity_occurrences_summary eos ON eos.entity_id = ne.id
            WHERE 1=1
        """
        params = []

        if query:
            sql += " AND ne.named_entity LIKE ?"
            params.append(f'%{query}%')

        if min_freq:
            sql += " AND eos.fq_document_level >= ?"
            params.append(int(min_freq))

        sql += " ORDER BY eos.fq_document_level DESC LIMIT ? OFFSET ?"
        params.extend([per_page + 1, offset])

        entities = db.execute(sql, params)
        has_more = len(entities) > per_page
        entities = entities[:per_page]

        return render_template('named_entities.html',
                             entities=[dict(zip(['id', 'named_entity', 'fq_document_level', 'fq_sentence_level'], entity)) 
                                     for entity in entities],
                             page=page,
                             query=query,
                             min_freq=min_freq,
                             has_more=has_more)
    except Exception as e:
        db.logger.error(f"Error loading named entities page: {e}")
        return render_template('error.html', message="Error loading named entities"), 500

@app.route('/named-entities/types')
def get_named_entity_types():
    try:
        db = get_db()
        sql = "SELECT id, named_entity FROM named_entities ORDER BY named_entity"
        types = db.execute(sql)
        return jsonify({
            'types': [dict(zip(['id', 'named_entity'], type_row)) for type_row in types]
        })
    except Exception as e:
        db.logger.error(f"Error fetching named entity types: {e}")
        return jsonify({'error': str(e)}), 500

@app.route("/entities")
def list_entities():
    try:
        db = get_db()
        page = int(request.args.get('page', 1))
        query = request.args.get('query', '')
        doc_id = request.args.get('doc_id')
        entity_type = request.args.get('type')
        sort = request.args.get('sort', 'tf_idf')
        order = request.args.get('order', 'desc')
        per_page = 30
        offset = (page - 1) * per_page
        
        # Get all entity types for the filter dropdown
        entity_types = db.execute("SELECT id, named_entity FROM named_entities ORDER BY named_entity")
        
        # Use enhanced search_entities with sorting
        entities_data = db.data_exchanger.search_entities(
            type=entity_type if entity_type else None,
            doc_id=int(doc_id) if doc_id else None,
            like=query if query else None,
            sort_by=sort,
            sort_order=order
        )
        
        # Handle pagination
        total_results = len(entities_data)
        has_more = total_results > (offset + per_page)
        paginated_data = entities_data[offset:offset + per_page]
        
        # Convert to NamedEntity objects
        entities = []
        for entity_data in paginated_data:
            entity_type_name = entity_data.pop('named_entity')
            entity_data.pop('overlap', None)  # Remove 'overlap' parameter
            entity = NamedEntity(**entity_data)
            entity.named_entity = entity_type_name
            entities.append(entity)

        # Calculate next sort order
        next_order = 'asc' if order == 'desc' else 'desc'

        return render_template('entities.html',
                             entities=entities,
                             entity_types=[dict(zip(['id', 'named_entity'], et)) for et in entity_types],
                             page=page,
                             query=query,
                             doc_id=doc_id,
                             type=entity_type,
                             sort=sort,
                             order=order,
                             next_order=next_order,
                             has_more=has_more)
    except Exception as e:
        db.logger.error(f"Error loading entities page: {e}")
        return render_template('error.html', message="Error loading entities"), 500

@app.route('/document/<int:doc_id>')
def show_document(doc_id):
    db = get_db()
    
    document = db.data_exchanger.get_document(doc_id)
    if not document:
        return "Document not found", 404

    return render_template('document.html', document=document, content=document.to_html())

@app.route('/entity-cooccurrences/')
def entity_cooccurrences():
    return render_template('entity_cooccurrences.html')

@app.route('/entity-cooccurrences/summary')
def entity_cooccurrences_summary():
    return render_template('entity_cooccurrences_summary.html')

@app.route('/entity-cooccurrences/table')
def entity_cooccurrences_table():
    try:
        db = get_db()
        page = int(request.args.get('page', 1))
        per_page = int(request.args.get('per_page', 30))
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
        
        return jsonify({
            'cooccurrences': [dict(zip([
                'id', 'e1_id', 'e2_id', 'entity1_text', 'entity2_text',
                'sentence_distance', 'document_id', 'sentence_index'
            ], row)) for row in cooccurrences],
            'has_more': has_more
        })
    except Exception as e:
        db.logger.error(f"Error loading entity co-occurrences table: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/entity-cooccurrences/summary/table')
def entity_cooccurrences_summary_table():
    try:
        db = get_db()
        page = int(request.args.get('page', 1))
        per_page = int(request.args.get('per_page', 30))
        include_self = request.args.get('include_self', 'false').lower() == 'true'
        entity1_type = request.args.get('entity1_type')
        entity2_type = request.args.get('entity2_type')

        result = db.data_exchanger.get_cooccurrences_summary(
            page=page,
            per_page=per_page,
            include_self=include_self,
            entity1_type=entity1_type,
            entity2_type=entity2_type
        )
        
        return jsonify({
            'summaries': result['summaries'],
            'has_more': result['has_more'],
            'total': result['total']
        })
    except Exception as e:
        db.logger.error(f"Error loading entity co-occurrences summary table: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/entity-cooccurrences/plot-data')
def entity_cooccurrences_plot_data():
    try:
        db = get_db()
        freq_column = request.args.get('freq_column', 'fq_document_level')
        min_freq = int(request.args.get('min_freq', 0))
        max_freq = int(request.args.get('max_freq', 100))

        sql = f"""
            SELECT {freq_column}, COUNT(*) as count
            FROM entity_cooccurrences_summary
            WHERE {freq_column} BETWEEN ? AND ?
            GROUP BY {freq_column}
            ORDER BY {freq_column}
        """
        params = [min_freq, max_freq]

        plot_data = db.execute(sql, params)

        return jsonify({
            'plot_data': [dict(zip([col[0] for col in db.cursor.description], row)) for row in plot_data]
        })
    except Exception as e:
        db.logger.error(f"Error loading entity co-occurrences plot data: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/entity-cooccurrences/summary/plot-data')
def entity_cooccurrences_summary_plot_data():
    try:
        db = get_db()
        freq_column = request.args.get('freq_column', 'fq_document_level')
        min_freq = int(request.args.get('min_freq', 0))
        max_freq = int(request.args.get('max_freq', 100))

        sql = f"""
            SELECT {freq_column}, COUNT(*) as count
            FROM entity_cooccurrences_summary
            WHERE {freq_column} BETWEEN ? AND ?
            GROUP BY {freq_column}
            ORDER BY {freq_column}
        """
        params = [min_freq, max_freq]

        plot_data = db.execute(sql, params)

        return jsonify({
            'plot_data': [dict(zip([col[0] for col in db.cursor.description], row)) for row in plot_data]
        })
    except Exception as e:
        db.logger.error(f"Error loading entity co-occurrences summary plot data: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/debug/entity-cooccurrences-summary')
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
        
        return jsonify({
            'total_records': count,
            'schema': [dict(zip(['cid', 'name', 'type', 'notnull', 'dflt_value', 'pk'], col)) for col in schema],
            'sample_row': [dict(zip([col[0] for col in db.cursor.description], row)) for row in sample]
        })
    except Exception as e:
        db.logger.error(f"Debug endpoint error: {e}")
        return jsonify({'error': str(e)}), 500

# Enhanced table views
@app.route('/tables/<table_name>')
def view_table(table_name):
    try:
        db = get_db()
        page = int(request.args.get('page', 1))
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

        return render_template('table_view.html',
                               table_name=table_name,
                               columns=columns,
                               rows=rows,
                               page=page,
                               has_more=has_more)
    except Exception as e:
        db.logger.error(f"Error loading table {table_name}: {e}")
        return render_template('error.html', message=f"Error loading table {table_name}"), 500

if __name__ == "__main__":
    with app.app_context():
        try:
            # Initialize database before running the server
            db = init_db()
            db.logger.info("Starting Flask server...")
            
            app.run(host="127.0.0.1", 
                    port=5001, 
                    debug=True,
                    use_reloader=True,
                    threaded=True)
        except Exception as e:
            if 'db' in locals():
                db.logger.error(f"Server error: {e}")
            raise