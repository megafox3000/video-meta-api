from flask import Flask, request, jsonify, send_from_directory
import subprocess
import json
import tempfile
import os
import requests
import re
import time
from datetime import datetime

app = Flask(__name__)

@app.route('/')
def index():
    return jsonify({"status": "✅ API is up and running!"})

@app.route('/analyze', methods=['POST'])
def analyze_video():
    if 'file' not in request.files:
        return jsonify({"error": "No file uploaded"}), 400

    file = request.files['file']
    filename = file.filename
    os.makedirs("uploads", exist_ok=True)
    filepath = os.path.join("uploads", filename)
    file.save(filepath)

    try:
        metadata = get_video_metadata(filepath)
    except Exception as e:
        return jsonify({"error": f"Metadata extraction failed: {e}"}), 500

    os.makedirs("output", exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')
    json_path = os.path.join("output", f"{timestamp}.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, ensure_ascii=False, indent=2)

    metadata["json_url"] = f"/download/{os.path.basename(json_path)}"
    return jsonify(metadata)

@app.route('/download/<filename>')
def download_file(filename):
    return send_from_directory("output", filename, as_attachment=True)

def get_video_metadata(file_path):
    cmd = [
        'ffprobe', '-v', 'quiet', '-print_format', 'json',
        '-show_format', '-show_streams', file_path
    ]
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    info = json.loads(result.stdout)

    output = {
        "file_name": os.path.basename(file_path),
        "format": info.get('format', {}),
        "streams": info.get('streams', []),
        "gps": []
    }

    tags = info.get('format', {}).get('tags', {})
    gps_tags = parse_gps_tags(tags)
    gps_i_
