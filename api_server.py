# from flask import Flask, jsonify, Response
# import os
# import json
# import glob
# import time
# from threading import Lock
# import logging
# from queue import Queue, Empty
# from threading import Thread

# app = Flask(__name__)
# app.lock = Lock()
# app.last_update = 0
# app.cache = []
# app.cache_time = 0
# app.event_queue = Queue()

# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger("APIServer")

# # Ensure directories exist
# os.makedirs('output', exist_ok=True)

# def file_monitor():
#     """Monitor the output directory for changes"""
#     known_files = set()
#     while True:
#         current_files = set(glob.glob(os.path.join('output', '**/*.json'), recursive=True))
#         new_files = current_files - known_files
        
#         if new_files:
#             logger.info(f"Detected {len(new_files)} new files")
#             app.event_queue.put(("new_files", list(new_files)))
#             known_files = current_files
        
#         time.sleep(1)  # Check every second

# def load_records_from_files(file_paths):
#     """Load records from specific files"""
#     records = []
#     for file_path in sorted(file_paths, key=os.path.getmtime):
#         try:
#             with open(file_path, 'r') as f:
#                 for line in f:
#                     line = line.strip()
#                     if line:
#                         try:
#                             record = json.loads(line)
#                             clean_record = {}
#                             for k, v in record.items():
#                                 if isinstance(v, str):
#                                     v = v.strip()
#                                     if v.startswith('"') and v.endswith('"'):
#                                         v = v[1:-1]
#                                 clean_record[k] = v
#                             records.append(clean_record)
#                         except json.JSONDecodeError:
#                             continue
#         except Exception as e:
#             logger.error(f"Error reading {file_path}: {str(e)}")
#             continue
#     return records

# def update_cache():
#     """Update the cache when new data arrives"""
#     while True:
#         try:
#             event_type, data = app.event_queue.get(timeout=1)
#             if event_type == "new_files":
#                 with app.lock:
#                     new_records = load_records_from_files(data)
#                     app.cache.extend(new_records)
#                     app.cache_time = time.time()
#                     logger.info(f"Added {len(new_records)} new records")
#         except Empty:
#             continue

# @app.route('/latest')
# def latest_results():
#     """Return all current results"""
#     with app.lock:
#         return jsonify({
#             "count": len(app.cache),
#             "last_update": app.cache_time,
#             "results": app.cache
#         })

# @app.route('/stream')
# def stream_results():
#     """SSE endpoint for real-time updates"""
#     def event_stream():
#         last_count = 0
#         while True:
#             with app.lock:
#                 current_count = len(app.cache)
#                 if current_count > last_count:
#                     new_records = app.cache[last_count:current_count]
#                     for record in new_records:
#                         yield f"data: {json.dumps(record)}\n\n"
#                     last_count = current_count
#             time.sleep(0.5)  # Check for updates every 0.5 seconds

#     return Response(event_stream(), mimetype="text/event-stream")

# @app.route('/health')
# def health_check():
#     return jsonify({
#         "status": "healthy",
#         "cache_size": len(app.cache),
#         "last_update": app.cache_time
#     })

# if __name__ == "__main__":
#     # Initial load of existing data
#     initial_files = glob.glob(os.path.join('output', '**/*.json'), recursive=True)
#     with app.lock:
#         app.cache = load_records_from_files(initial_files)
#         app.cache_time = time.time()
    
#     # Start monitoring threads
#     Thread(target=file_monitor, daemon=True).start()
#     Thread(target=update_cache, daemon=True).start()
    
#     app.run(host="0.0.0.0", port=5001, debug=True, threaded=True)

from flask import Flask, jsonify
from flask_cors import CORS  # Add this import
import os
import json
import glob
import time
from threading import Lock
import logging

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Alternatively, you can enable CORS for specific routes:
# CORS(app, resources={r"/latest": {"origins": "http://localhost:3000"}})
# CORS(app, resources={r"/stream": {"origins": "http://localhost:3000"}})

# Ensure output directory exists
if not os.path.exists('output'):
    os.makedirs('output')

@app.after_request  # Add CORS headers to every response
def after_request(response):
    response.headers.add('Access-Control-Allow-Origin', 'http://localhost:3000')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
    response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
    response.headers.add('Access-Control-Allow-Credentials', 'true')
    return response

@app.route('/latest')
def latest_results():
    output_dir = "output"
    
    if not os.path.exists(output_dir):
        return jsonify({"error": "Output directory not found", "status": "processing"}), 202
    
    json_files = glob.glob(os.path.join(output_dir, '**/*.json'), recursive=True)
    
    if not json_files:
        return jsonify({"error": "No results available yet", "status": "processing"}), 202
    
    latest_data = []
    
    for file_path in sorted(json_files, key=os.path.getmtime, reverse=True):
        try:
            with open(file_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            record = json.loads(line)
                            latest_data.append(record)
                        except json.JSONDecodeError:
                            continue
        except Exception as e:
            print(f"Error reading file {file_path}: {str(e)}")
            continue
    
    if not latest_data:
        return jsonify({"error": "No valid records found in files", "status": "processing"}), 202
        
    return jsonify({
        "count": len(latest_data),
        "results": latest_data
    })

@app.route('/stream')
def stream_results():
    def event_stream():
        # Your existing stream implementation
        pass
    
    return Response(event_stream(), mimetype="text/event-stream")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=True)