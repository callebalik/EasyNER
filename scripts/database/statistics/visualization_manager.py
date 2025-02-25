import os
import time
import importlib
import sys
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from flask import g
import threading

class VisualizationFileHandler(FileSystemEventHandler):
    """Handler for visualization file changes"""
    def __init__(self, manager):
        self.manager = manager

    def on_modified(self, event):
        if event.src_path.endswith('.py'):
            visualization_name = os.path.splitext(os.path.basename(event.src_path))[0]
            module_path = os.path.dirname(event.src_path)
            if module_path not in sys.path:
                sys.path.append(module_path)
            try:
                # Reload the module if it's already loaded
                module_name = f"statistics.{visualization_name}"
                if (module_name in sys.modules):
                    importlib.reload(sys.modules[module_name])
                self.manager.invalidate_cache(visualization_name)
            except Exception as e:
                self.manager.app.logger.error(f"Error reloading module {visualization_name}: {e}")

class VisualizationManager:
    def __init__(self, app):
        self.app = app
        self.caches = {}  # Dictionary to store multiple visualization caches
        self.last_modified = {}
        self.cache_path = os.path.join(os.path.dirname(__file__), 'cache')
        self.lock = threading.Lock()
        os.makedirs(self.cache_path, exist_ok=True)

        # Add the current directory to sys.path if not already there
        current_dir = os.path.dirname(__file__)
        if (current_dir not in sys.path):
            sys.path.append(current_dir)

        # Setup file watching
        self.event_handler = VisualizationFileHandler(self)
        self.observer = Observer()
        self.observer.schedule(
            self.event_handler,
            os.path.dirname(__file__),
            recursive=True  # Changed to True to watch subdirectories
        )
        self.observer.start()
        app.logger.info("Started visualization file monitoring")

    def invalidate_cache(self, visualization_name):
        """Invalidate the cache for a specific visualization"""
        with self.lock:
            if visualization_name in self.caches:
                self.caches[visualization_name] = None
                self.last_modified[visualization_name] = 0
                self.app.logger.info(f"{visualization_name} visualization cache invalidated")

    def get_cached_visualization(self, visualization_name, db, generator_func):
        """Get cached visualization or generate new one if needed"""
        cache_file = os.path.join(self.cache_path, f'{visualization_name}.html')
        source_file = os.path.join(os.path.dirname(__file__), f'{visualization_name}.py')

        try:
            with self.lock:
                module_name = f"statistics.{visualization_name}"
                module_modified = os.path.getmtime(source_file) if os.path.exists(source_file) else 0
                cache_exists = os.path.exists(cache_file)
                cache_modified = os.path.getmtime(cache_file) if cache_exists else 0

                # Check if module was modified or cache is invalid
                needs_update = (
                    visualization_name not in self.caches or
                    self.caches[visualization_name] is None or
                    not cache_exists or
                    module_modified > cache_modified or
                    module_modified > self.last_modified.get(visualization_name, 0)
                )

                if needs_update:
                    # Generate new visualization
                    html = generator_func(db)
                    self.last_modified[visualization_name] = time.time()

                    # Add version-based refresh script that only checks when the file is modified
                    refresh_script = """
                    <script>
                        const sourceModified = %s;
                        let lastModified = sourceModified;
                        let checkInterval;

                        async function checkForChanges() {
                            try {
                                const response = await fetch(window.location.href, {
                                    headers: {
                                        'If-Modified-Since': new Date(lastModified * 1000).toUTCString()
                                    }
                                });

                                if (response.status === 304) {
                                    return; // No changes
                                }

                                const newModified = response.headers.get('Last-Modified');
                                if (newModified) {
                                    const newModifiedTime = new Date(newModified).getTime() / 1000;
                                    if (newModifiedTime > lastModified) {
                                        window.location.reload();
                                    }
                                }
                            } catch (error) {
                                console.log('Update check failed:', error);
                                clearInterval(checkInterval); // Stop checking on error
                            }
                        }

                        // Start checking after the page loads
                        window.addEventListener('load', () => {
                            checkInterval = setInterval(checkForChanges, 5000);
                        });
                    </script>
                    """ % module_modified

                    # Insert refresh script before closing body tag
                    if "</body>" in html:
                        html = html.replace("</body>", f"{refresh_script}</body>")
                    else:
                        html += refresh_script

                    # Cache the result
                    with open(cache_file, 'w') as f:
                        f.write(html)
                    self.caches[visualization_name] = html

                return self.caches[visualization_name]

        except Exception as e:
            self.app.logger.error(f"Error handling cached visualization {visualization_name}: {e}")
            raise

    def __del__(self):
        """Clean up observer on deletion"""
        try:
            self.observer.stop()
            self.observer.join()
        except Exception as e:
            self.app.logger.error(f"Error stopping file observer: {e}")