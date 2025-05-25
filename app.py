from flask import Flask, request, jsonify, send_from_directory
import subprocess
import json
import tempfile
import os
import requests
import re
import time
from flask_cors import CORS
from datetime import datetime

app = Flask(__name__)
CORS(app)

# ----------- GPS & METADATA FUNCTIONS -----------

def parse_gps_tags(tags):
    gps_data = {}
    for key, value in tags.items():
        if "location" in key.lower() or "gps" in key.lower():
            gps_data[key] = value
    return gps_data

def extract_coordinates_from_tags(tags):
    gps_data = []
    for key, value in tags.items():
        if "ISO6709" in key and re.match(r"^[\+\-]\d+(\.\d+)?[\+\-]\d+(\.\d+)?", value):
            match = re.match(r"^([\+\-]\d+(\.\d+)?)([\+\-]\d+(\.\d+)?).*", value)
            if match:
                lat = match.group(1)
                lon = match.group(3)
                link = f"https://maps.google.com/?q={lat},{lon}"
                address = reverse_geocode(lat, lon)
                gps_data.append({
                    "tag": key,
                    "latitude": float(lat),
                    "longitude": float(lon),
                    "link": link,
                    "address": address
                })
    return gps_data

def reverse_geocode(lat, lon):
    try:
        time.sleep(1)
        url = "https://nominatim.openstreetmap.org/reverse"
        params = {
            "lat": lat,
            "lon": lon,
            "format": "json",
            "zoom": 14,
            "addressdetails": 1
        }
        headers = {"User-Agent": "VideoMetaApp/1.0"}
        response = requests.get(url, params=params, headers=headers)
        data = response.json()
        return data.get("display_name", "Адрес не найден.")
    except Exception as e:
        return f"Ошибка геокодинга: {e}"

# ----------- ANALYZE LOGIC -----------

def get_video_metadata(file_path):
    cmd = [
        'ffprobe', '-v', 'quiet', '-print_format', 'json',
        '-show_format', '-show_streams', file_path
    ]
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    if result.returncode != 0:
        raise Exception("Ошибка при запуске ffprobe")

    info = json.loads(result.stdout)

    output = {
        "file_name": os.path.basename(file_path),
        "format": info.get('format', {}),
        "streams": info.get('streams', []),
        "gps": []
    }

    tags = info.get('format', {}).get('tags', {})
    gps_tags = parse_gps_tags(tags)
    gps_info = extract_coordinates_from_tags(gps_tags)
    output["gps"] = gps_info

    return output

# ----------- FLASK ROUTES -----------

@app.route('/')
def index():
    return jsonify({"status": "✅ API is up and running!"})

@app.route('/analyze', methods=['POST'])
def analyze_video():
    if 'file' not in request.files:
        return jsonify({'error': 'No file uploaded'}), 400

    file = request.files['file']
    filename = file.filename
    os.makedirs("uploads", exist_ok=True)
    filepath = os.path.join("uploads", filename)
    file.save(filepath)

    try:
        metadata = get_video_metadata(filepath)
    except Exception as e:
        return jsonify({"error": f"Metadata extraction failed: {e}"}), 500

    result = {
        "filename": filename,
        "size_bytes": os.path.getsize(filepath),
        "analyzed_at": datetime.now().isoformat(),
        "metadata": metadata
    }

    os.makedirs("output", exist_ok=True)
    json_filename = f"{datetime.now().strftime('%Y%m%d-%H%M%S')}.json"
    json_path = os.path.join("output", json_filename)
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    result["json_url"] = f"/download/{json_filename}"
    return jsonify(result)

@app.route('/download/<filename>')
def download_file(filename):
    return send_from_directory("output", filename, as_attachment=True)

# ----------- ENTRYPOINT -----------

if __name__ == '__main__':
    from waitress import serve
    serve(app, host='0.0.0.0', port=8080)
