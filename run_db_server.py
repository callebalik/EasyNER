# filepath: /home/carloa/Desktop/EasyNer/run_db_server.py
from scripts.database.db_server import app, get_db
from flask import Flask

if __name__ == "__main__":
    # Manually create an application context
    with app.app_context():
        try:
            # Initialize database before running the server
            db = get_db()
        except Exception as e:
            print(f"Failed to initialize database: {e}")
            exit(1)

        print("Starting Flask server...")
        # Disable reloader to avoid thread issues with SQLite.
        app.run(host="127.0.0.1", port=5001, debug=True, use_reloader=False, threaded=False)