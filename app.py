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
                    "lat": lat,
                    "lon": lon,
                    "link": link,
                    "address": address
                })
    return gps_data_
