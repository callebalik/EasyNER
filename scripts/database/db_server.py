from flask import Flask, jsonify, g, render_template, request
from db_main import EasyNerDBHandler
import os
from data_model import Document, Sentence, NamedEntity
import sass

# Set template directory to current directory/templates
template_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates')
app = Flask(__name__, template_folder=template_dir)

# Compile SCSS to CSS on server load
def compile_scss():
    scss_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'static/styles.scss')
    css_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'static/styles.css')
    partials_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'static/partials')
    static_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'static')
    with open(scss_file, 'r') as f:
        scss_content = f.read()
    css_content = sass.compile(string=scss_content, include_paths=[static_dir])
    with open(css_file, 'w') as f:
        f.write(css_content)

compile_scss()

# Serve static CSS file
@app.route('/static/styles.css')
def styles():
    return app.send_static_file('styles.css')

def get_db():
    if 'db' not in g:
        try:
            g.db = EasyNerDBHandler(db_path="/lunarc/nobackup/projects/snic2020-6-41/carl/test_eo_sentence_ref.db")
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
        
        return render_template('home.html', tables=tables_info)
    except Exception as e:
        db.logger.error(f"Error loading home page: {e}")
        return render_template('error.html', message="Error loading database information"), 500

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

@app.route('/document/<int:doc_id>')
def show_document(doc_id):
    db = get_db()
    
    document = db.get_document(doc_id)
    if not document:
        return "Document not found", 404

    return render_template('document.html', document=document, content=document.to_html())

if __name__ == "__main__":
    try:
        # Initialize database before running the server
        db = init_db()
        db.logger.info("Starting Flask server in debug mode with reloader...")
        app.run(host="127.0.0.1", 
                port=5000, 
                debug=True,  # Enable debug mode
                use_reloader=True,  # Enable automatic reloader
                threaded=True)  # Enable threading for better development experience
    except Exception as e:
        if hasattr(g, 'db'):
            g.db.logger.error(f"Server error: {e}")