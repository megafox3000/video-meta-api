import os
from flask import Flask, request, jsonify, send_from_directory
# subprocess, json, tempfile, re больше не нужны для ffprobe/локального сохранения
# import subprocess
# import json
# import tempfile
# import re
import time # Оставим для potential reverse_geocode, но пока не используется
import requests # Оставим для reverse_geocode, но пока не используется
from flask_cors import CORS
from datetime import datetime

# --- ИМПОРТЫ CLOUDINARY ---
import cloudinary
import cloudinary.uploader
import cloudinary.api

app = Flask(__name__)
CORS(app)

# --- Настройка Cloudinary ---
# ВАЖНО: Получите ваши Cloudinary API ключи из дашборда Cloudinary
# и установите их как переменные окружения на Render!
# (CLOUDINARY_CLOUD_NAME, CLOUDINARY_API_KEY, CLOUDINARY_API_SECRET)
cloudinary.config(
    cloud_name = os.environ.get('CLOUDINARY_CLOUD_NAME'),
    api_key = os.environ.get('CLOUDINARY_API_KEY'),
    api_secret = os.environ.get('CLOUDINARY_API_SECRET'),
    secure = True
)

# --- Имитация базы данных задач в памяти (Python dict) ---
# { public_id: { username, status, filename, cloudinary_url, metadata, message, timestamp } }
# В реальном приложении здесь должна быть настоящая база данных (PostgreSQL, SQLite и т.д.)
fake_task_database = {}

# ----------- GPS & METADATA FUNCTIONS (Оставлены как есть, но НЕ будут использоваться Cloudinary-потоком) -----------
# Если вы планируете продолжать использовать эти функции для анализа, который Cloudinary не предоставляет,
# вам нужно будет решить, как передавать видео сюда (например, скачивать с Cloudinary на Render или
# использовать эти функции на локальном воркере).
# В текущей версии для Cloudinary анализа, эти функции не вызываются.
def parse_gps_tags(tags):
    gps_data = {}
    for key, value in tags.items():
        if "location" in key.lower() or "gps" in key.lower():
            gps_data[key] = value
    return gps_data

def extract_coordinates_from_tags(tags):
    gps_data = []
    # Для этого требуется 're' - убедитесь, что он импортирован
    import re
    for key, value in tags.items():
        if "ISO6709" in key and re.match(r"^[\+\-]\d+(\.\d+)?[\+\-]\d+(\.\d+)?", value):
            match = re.match(r"^([\+\-]\d+(\.\d+)?)([\+\-]\d+(\.\d+)?).*", value)
            if match:
                lat = match.group(1)
                lon = match.group(3)
                # Измененная ссылка, чтобы соответствовать формату URL, который не приводит к ошибке в браузере
                link = f"https://www.google.com/maps/search/?api=1&query={lat},{lon}"
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
        # time.sleep(1) # Оставим, если вы знаете, зачем это
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

# ----------- FLASK ROUTES -----------

@app.route('/')
def index():
    return jsonify({"status": "✅ Python Backend is up and running!"})

@app.route('/analyze', methods=['POST'])
def analyze_video():
    if 'file' not in request.files:
        return jsonify({'error': 'No file uploaded'}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400

    username = request.form.get('username', 'unknown_user') # Получаем имя пользователя из формы

    print(f"\n[PYTHON BACKEND] Получен файл '{file.filename}' от пользователя '{username}' на /analyze.")

    try:
        # --- Загрузка видео на Cloudinary ---
        # Cloudinary обработает видео и предоставит метаданные
        upload_result = cloudinary.uploader.upload(
            file,
            resource_type="video",
            folder="hife_video_analysis", # Папка на вашем аккаунте Cloudinary
            overwrite=True, # Перезаписывать, если файл с таким именем уже существует (может быть полезно для отладки)
            quality="auto", # Оптимизация качества
            format="mp4", # Конвертация в mp4 (или оставьте original)
        )

        public_id = upload_result.get('public_id')
        cloudinary_url = upload_result.get('secure_url')
        
        # Получаем базовые метаданные из ответа Cloudinary
        basic_metadata = {
            "format": upload_result.get('format'),
            "duration": upload_result.get('duration'), # в секундах
            "width": upload_result.get('width'),
            "height": upload_result.get('height'),
            "size_bytes": upload_result.get('bytes'), # в байтах
            "bit_rate": upload_result.get('bit_rate') # Cloudinary может предоставить это напрямую
        }
        
        # Cloudinary может также возвращать GPS данные, если они есть в видео:
        # Пример: if 'image_metadata' in upload_result and 'GPS' in upload_result['image_metadata']:
        # Если нужен более глубокий FFprobe, можно запросить через cloudinary.api.resource
        # info = cloudinary.api.resource(public_id, all=True, type="upload")
        # print(info) # Здесь будут все детали, включая streaming_profile, metadata, etc.
        # basic_metadata['some_other_field'] = info.get('some_other_field')

        task_status = "completed" # Для Cloudinary-анализа, статус обычно "completed" сразу после загрузки
        task_message = "Видео успешно загружено на Cloudinary и получены базовые метаданные."

        # Сохраняем информацию о задаче в нашей "фиктивной базе данных"
        fake_task_database[public_id] = {
            "username": username,
            "status": task_status,
            "filename": file.filename,
            "cloudinary_url": cloudinary_url,
            "metadata": basic_metadata,
            "message": task_message,
            "timestamp": datetime.now().isoformat()
        }

        print(f"[PYTHON BACKEND] Видео '{file.filename}' загружено на Cloudinary. Public ID: {public_id}")
        return jsonify({
            "status": "task_created",
            "taskId": public_id, # Используем public_id как taskId
            "message": task_message,
            "cloudinary_url": cloudinary_url, # Отправляем Cloudinary URL фронтенду
            "metadata": basic_metadata # Отправляем базовые метаданные сразу
        }), 200

    except cloudinary.exceptions.Error as e:
        print(f"[PYTHON BACKEND] Cloudinary Error: {e}")
        return jsonify({"error": f"Cloudinary upload failed: {str(e)}"}), 500
    except Exception as e:
        print(f"[PYTHON BACKEND] Общая ошибка: {e}")
        return jsonify({"error": f"Internal server error: {str(e)}"}), 500

# --- НОВЫЙ Эндпоинт для проверки статуса задачи ---
@app.route('/task-status/<task_id>', methods=['GET'])
def get_task_status(task_id):
    task_info = fake_task_database.get(task_id)
    if task_info:
        return jsonify(task_info), 200
    else:
        return jsonify({"message": "Task not found."}), 404

# --- Эндпоинт-заглушка для будущих "тяжелых" задач (для локального воркера) ---
@app.route('/heavy-tasks/pending', methods=['GET'])
def get_heavy_tasks():
    # Здесь могла бы быть логика, которая фильтрует задачи,
    # требующие локальной обработки (например, по размеру, формату, сложности анализа).
    # Пока это просто заглушка.
    return jsonify({"message": "No heavy tasks pending for local worker yet."}), 200

# Удален эндпоинт /download/<filename>, так как файлы не сохраняются локально.
# @app.route('/download/<filename>')
# def download_file(filename):
#     return send_from_directory("output", filename, as_attachment=True)

# ----------- ENTRYPOINT -----------

if __name__ == '__main__':
    from waitress import serve
    # Render использует переменную окружения PORT для определения порта
    port = int(os.environ.get('PORT', 8080)) # Использовать 8080 по умолчанию, но Render предоставит свой порт
    serve(app, host='0.0.0.0', port=port)
