from flask import Flask, jsonify, g, render_template, request
from db_main import EasyNerDBHandler
import os

# Set template directory to current directory/templates
template_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates')
app = Flask(__name__, template_folder=template_dir)

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

@app.route("/document/<int:doc_id>")
def get_document(doc_id):
    try:
        db = get_db()
        # Get document details
        doc_details = db.get_document_details(doc_id)
        if not doc_details:
            db.logger.info(f"Document {doc_id} not found")
            return jsonify({"error": "Document not found"}), 404
        
        # Get document content with entities
        doc_content = db.get_document_as_html(doc_id)
        
        return jsonify({
            "document": doc_details,
            "content": doc_content
        })
    except Exception as e:
        db.logger.error(f"Error processing request for document {doc_id}: {e}")
        return jsonify({"error": "Internal server error"}), 500

@app.route("/view/document/<int:doc_id>")
def view_document(doc_id):
    try:
        db = get_db()
        # Add debug logging
        db.logger.debug(f"Attempting to retrieve document {doc_id}")
        
        # Verify document exists first
        db.execute("SELECT COUNT(*) FROM documents WHERE id = ?", (doc_id,))
        count = db.fetchone()[0]
        if count == 0:
            db.logger.warning(f"Document {doc_id} does not exist in database")
            return render_template('error.html', message="Document not found"), 404
            
        doc_details = db.get_document_details(doc_id)
        if not doc_details:
            db.logger.error(f"Document {doc_id} exists but could not be retrieved")
            return render_template('error.html', message="Error retrieving document"), 500
        
        doc_content = db.get_document_as_html(doc_id)
        return render_template('document.html', 
                             document=doc_details, 
                             content=doc_content)
    except Exception as e:
        db.logger.error(f"Error viewing document {doc_id}: {e}")
        return render_template('error.html', message="Internal server error"), 500

@app.route("/documents")
def list_documents():
    try:
        db = get_db()
        page = int(request.args.get('page', 1))
        query = request.args.get('query', '')
        doc_id = request.args.get('doc_id', '')
        per_page = 30
        offset = (page - 1) * per_page

        params = []
        conditions = []

        if query:
            conditions.append("title LIKE ?")
            params.append(f'%{query}%')
        
        if doc_id:
            conditions.append("id = ?")
            params.append(doc_id)

        if conditions:
            where_clause = "WHERE " + " AND ".join(conditions)
        else:
            where_clause = ""

        sql = f"""
            SELECT id, title, word_count 
            FROM documents 
            {where_clause}
            ORDER BY id 
            LIMIT ? OFFSET ?
        """
        params.extend([per_page + 1, offset])

        documents = db.execute(sql, params)
        has_more = len(documents) > per_page
        documents = documents[:per_page]  # Trim to per_page items

        return render_template('documents.html',
                             documents=[dict(zip(['id', 'title', 'word_count'], doc)) for doc in documents],
                             page=page,
                             query=query,
                             doc_id=doc_id,
                             has_more=has_more)
    except Exception as e:
        db.logger.error(f"Error loading documents page: {e}")
        return render_template('error.html', message="Error loading documents"), 500

if __name__ == "__main__":
    try:
        # Initialize database before running the server
        db = init_db()
        db.logger.info("Starting Flask server in debug mode with reloader...")
        app.run(host="127.0.0.1", 
                port=8008, 
                debug=True,  # Enable debug mode
                use_reloader=True,  # Enable automatic reloader
                threaded=True)  # Enable threading for better development experience
    except Exception as e:
        if hasattr(g, 'db'):
            g.db.logger.error(f"Server error: {e}")