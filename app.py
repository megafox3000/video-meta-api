from flask import Flask, request, jsonify
import subprocess
import json
import tempfile
import os
import requests
import re
import time

app = Flask(__name__)

@app.route('/analyze', methods=['POST'])
def analyze_video():
    if 'file' not in request.files:
        return jsonify({"error": "No file uploaded"}), 400

    file = request.files['file']
    with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(file.filename)[1]) as tmp:
        file.save(tmp.name)
        video_path = tmp.name

    try:
        result = get_video_metadata(video_path)
        return jsonify(result)
    finally:
        os.remove(video_path)

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
    gps_info = extract_coordinates_from_tags(gps_tags)
    output["gps"] = gps_info

    return output

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
                    "lat": lat,
                    "lon": lon,
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
        return data.get("display_name", "Не удалось определить адрес.")
    except Exception as e:
        return f"Ошибка геокодинга: {e}"

if __name__ == '__main__':
    app.run(debug=True)
