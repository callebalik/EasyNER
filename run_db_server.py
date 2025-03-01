import os
import sys
import signal
import argparse
import tempfile
import psutil
import pwd
from socket import socket, AF_INET, SOCK_STREAM
import traceback

# Set environment variables before importing app modules
# This prevents the invalid subscript error by ensuring variables are set before any tracking happens
os.environ.setdefault('PYTHONPATH', os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault('FLASK_APP', 'scripts.database.db_server')
os.environ.setdefault('FLASK_ENV', 'development')
os.environ.setdefault('LOG_LEVEL', 'DEBUG')
os.environ.setdefault('DB_PATH', '/lunarc/nobackup/projects/snic2020-6-41/carl/dev.db')
os.environ.setdefault('SERVER_PORT', '5001')

# Improved error handling for imports
try:
    # Now import Flask app after environment variables are set
    from scripts.database.db_server import app, get_db_easyner, setup_logging

    # Import the monitoring module only if it exists
    try:
        from scripts.database.db_server import operation_monitor
    except ImportError:
        operation_monitor = None
except ImportError as e:
    print(f"ERROR: Failed to import required modules: {e}")
    traceback.print_exc()
    sys.exit(1)

# Define required environment variables and their defaults
REQUIRED_ENV_VARS = {
    'PYTHONPATH': os.path.dirname(os.path.abspath(__file__)),
    'FLASK_APP': 'scripts.database.db_server',
    'FLASK_ENV': 'development',
    'LOG_LEVEL': 'DEBUG',
    'DB_PATH': os.path.join(os.path.dirname(os.path.abspath(__file__)), 'dev.db'),
    'SERVER_PORT': '5001'
}

def validate_environment():
    """Validate and set required environment variables"""
    missing_vars = []
    for var, default in REQUIRED_ENV_VARS.items():
        if var not in os.environ:
            os.environ[var] = str(default)
            print(f"Warning: Environment variable {var} not set, using default: {default}")
    if missing_vars:
        print(f"Warning: Missing recommended environment variables: {', '.join(missing_vars)}")
    return True

def find_process_using_port(port):
    """Identify the process using a specific port."""
    try:
        for process in psutil.process_iter(['pid', 'name', 'username', 'cmdline']):
            try:
                for conn in process.connections(kind='inet'):
                    if conn.laddr.port == port and conn.status in ['LISTEN', 'ESTABLISHED']:
                        # Get username for the process
                        try:
                            username = process.info['username']
                        except KeyError:
                            try:
                                username = pwd.getpwuid(process.uids().real).pw_name
                            except:
                                username = "unknown"
                        return {
                            'pid': process.pid,
                            'name': process.info['name'],
                            'username': username,
                            'cmdline': ' '.join(process.info.get('cmdline', [])) if process.info.get('cmdline') else "unknown"
                        }
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                continue
    except Exception as e:
        print(f"Error finding process using port {port}: {e}")
    return None

def cleanup_server(pid_file=None):
    """Clean up server resources on exit"""
    print("Cleaning up server resources...")
    # Remove PID file if it exists
    if pid_file and os.path.exists(pid_file):
        try:
            os.unlink(pid_file)
            print(f"Removed PID file: {pid_file}")
        except Exception as e:
            print(f"Error cleaning up PID file: {e}")

def signal_handler(sig, frame, pid_file=None):
    """Handle termination signals gracefully"""
    print(f"Received signal {sig}, shutting down server...")
    cleanup_server(pid_file)
    sys.exit(0)

def kill_process_on_port(port):
    """Kill any process using the specified port"""
    process_info = find_process_using_port(port)
    if process_info:
        try:
            process = psutil.Process(process_info['pid'])
            print(f"Force-terminating process {process_info['pid']} ({process_info['name']}) using port {port}")
            process.terminate()
            try:
                process.wait(timeout=3)
                return True  # Process terminated successfully
            except psutil.TimeoutExpired:
                process.kill()
                return True  # Process killed successfully
        except (psutil.NoSuchProcess, psutil.AccessDenied) as e:
            print(f"Failed to terminate process {process_info['pid']}: {e}")
    return False

def find_available_port(start_port=5001, end_port=5030):
    """Find an available port within the specified range"""
    for port in range(start_port, end_port):
        s = socket(AF_INET, SOCK_STREAM)
        try:
            s.bind(('127.0.0.1', port))
            s.close()
            return port
        except OSError:
            s.close()
    return None

def check_pid_file(pid_file):
    """Check for existing PID file and handle orphaned processes"""
    if os.path.exists(pid_file):
        try:
            with open(pid_file, 'r') as f:
                old_pid = int(f.read().strip())
            if psutil.pid_exists(old_pid):
                old_process = psutil.Process(old_pid)
                try:
                    proc_name = old_process.name()
                    username = old_process.username()
                    cmdline = ' '.join(old_process.cmdline()) if old_process.cmdline() else "unknown"
                    print(f"Found previous process: PID={old_pid}, name={proc_name}, user={username}, command={cmdline}")
                    if "python" in proc_name.lower() or "run_db_server" in cmdline.lower():
                        print(f"Terminating orphaned server process: {old_pid}")
                        old_process.terminate()
                        try:
                            old_process.wait(timeout=3)
                        except psutil.TimeoutExpired:
                            print(f"Process {old_pid} did not terminate gracefully, killing it")
                            old_process.kill()
                        return True
                except (psutil.AccessDenied, psutil.NoSuchProcess) as e:
                    print(f"Cannot access process {old_pid} details: {e}")
        except (ValueError, IOError, psutil.NoSuchProcess, psutil.AccessDenied) as e:
            print(f"Error checking PID file: {e}")
            # Remove invalid PID file
            try:
                os.unlink(pid_file)
                print(f"Removed invalid PID file: {pid_file}")
            except OSError:
                pass
    return False

def optimize_performance():
    """Optimize database settings for better performance"""
    try:
        # Import the monitoring module
        from scripts.database.monitoring import OperationMonitor
        from scripts.database.db_server import app

        # Create a temporary monitor if needed
        temp_monitor = OperationMonitor(app.logger)

        # Use the optimize_performance method from OperationMonitor
        result = temp_monitor.optimize_performance()

        if result:
            app.logger.info("Database optimized for performance")
        else:
            app.logger.warning("Database optimization may not have been complete")

        return result
    except Exception as e:
        print(f"ERROR: Failed to optimize database: {e}")
        # Don't raise exception to allow server to continue starting
        return False

def test_database_connection():
    """Test the database connection to verify it's working correctly"""
    try:
        # Try to get the database connection
        db = get_db_easyner()
        # Execute a simple query to verify connection is working
        db.execute("SELECT 1")
        print("Database connection test successful")
        return True
    except Exception as e:
        print(f"ERROR: Database connection test failed: {e}")
        print(traceback.format_exc())
        return False

if __name__ == "__main__":
    # Validate environment before starting
    validate_environment()

    # Set up command-line argument parsing
    parser = argparse.ArgumentParser(description='EasyNer DB Server')
    parser.add_argument('--port', type=int, help='Port to run the server on')
    parser.add_argument('--force', action='store_true', help='Force kill any process using the port')
    parser.add_argument('--optimize', action='store_true', help='Optimize database performance')
    parser.add_argument('--test-db-only', action='store_true', help='Only test database connection and exit')
    args = parser.parse_args()

    # Test database connection and exit if --test-db-only flag is set
    if args.test_db_only:
        success = test_database_connection()
        sys.exit(0 if success else 1)

    # Create PID file for process tracking
    pid_dir = os.path.join(tempfile.gettempdir(), 'easyner')
    os.makedirs(pid_dir, exist_ok=True)
    pid_file = os.path.join(pid_dir, 'db_server.pid')

    # Check for existing process
    check_pid_file(pid_file)

    # Write current PID to file
    with open(pid_file, 'w') as f:
        f.write(str(os.getpid()))

    # Register signal handlers for clean shutdown
    signal.signal(signal.SIGINT, lambda sig, frame: signal_handler(sig, frame, pid_file))
    signal.signal(signal.SIGTERM, lambda sig, frame: signal_handler(sig, frame, pid_file))

    # Determine port to use
    port = args.port
    if not port:
        port = int(os.environ.get('SERVER_PORT', 5001))

    # Check if port is in use and handle appropriately
    s = socket(AF_INET, SOCK_STREAM)
    try:
        s.bind(('127.0.0.1', port))
        s.close()
    except OSError:
        if args.force:
            killed = kill_process_on_port(port)
            if not killed:
                print(f"ERROR: Could not kill process using port {port}")
                alt_port = find_available_port(port + 1)
                if alt_port:
                    print(f"Using alternative port: {alt_port}")
                    port = alt_port
                else:
                    print("No alternative ports available. Exiting.")
                    sys.exit(1)
        else:
            process_info = find_process_using_port(port)
            if process_info:
                print(f"ERROR: Port {port} is already in use by process:")
                print(f"  PID: {process_info['pid']}")
                print(f"  Name: {process_info['name']}")
                print(f"  User: {process_info['username']}")
                print(f"  Command: {process_info['cmdline']}")
                print("Use --force to kill the process or specify a different port with --port")
                sys.exit(1)

    # Update the environment variable with the actual port
    os.environ['SERVER_PORT'] = str(port)

    # Test database connection before starting server
    print("Testing database connection...")
    if not test_database_connection():
        print("ERROR: Failed to connect to database. Exiting.")
        sys.exit(1)

    # Optimize performance if requested
    if args.optimize:
        print("Optimizing database performance...")
        optimize_performance()

    # Start the Flask application
    try:
        print(f"Starting Flask server on port {port}...")
        app.run(
            host="127.0.0.1",
            port=port,
            debug=os.environ.get('FLASK_ENV') == 'development',
            use_reloader=False,  # Disable reloader to prevent duplicate processes
            threaded=True,
        )
    except Exception as e:
        print(f"ERROR: Failed to start server: {e}")
        traceback.print_exc()
        cleanup_server(pid_file)
        sys.exit(1)