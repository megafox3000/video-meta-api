import os
from flask import Flask, request, jsonify
from flask_cors import CORS
from datetime import datetime

import cloudinary
import cloudinary.uploader
import cloudinary.api

from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, JSON
from sqlalchemy.orm import sessionmaker, declarative_base

app = Flask(__name__)
CORS(app)

cloudinary.config(
    cloud_name = os.environ.get('CLOUDINARY_CLOUD_NAME'),
    api_key = os.environ.get('CLOUDINARY_API_KEY'),
    api_secret = os.environ.get('CLOUDINARY_API_SECRET'),
    secure = True
)

DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL environment variable is not set!")

engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
Base = declarative_base()

class Task(Base):
    __tablename__ = 'tasks'

    id = Column(Integer, primary_key=True)
    task_id = Column(String, unique=True, nullable=False)
    username = Column(String)
    status = Column(String)
    filename = Column(String)
    cloudinary_url = Column(String)
    video_metadata = Column(JSON) 
    message = Column(Text)
    timestamp = Column(DateTime, default=datetime.now)

    def __repr__(self):
        return f"<Task(task_id='{self.task_id}', status='{self.status}')>"
    
    def to_dict(self):
        return {
            "taskId": self.task_id,
            "username": self.username,
            "status": self.status,
            "filename": self.filename,
            "cloudinary_url": self.cloudinary_url,
            "metadata": self.video_metadata, 
            "message": self.message,
            "timestamp": self.timestamp.isoformat()
        }

def create_tables():
    # Эта функция создает таблицы, только если их еще нет в базе данных
    Base.metadata.create_all(engine)
    print("Database tables created or already exist.")

# --- ВЫЗОВ create_tables() ПЕРЕМЕЩЕН СЮДА ---
# Теперь эта функция будет вызываться при импорте модуля 'app',
# то есть при запуске Flask-приложения Gunicorn'ом/Waitress'ом.
create_tables()

# ----------- GPS & METADATA FUNCTIONS (без изменений) -----------
def parse_gps_tags(tags):
    gps_data = {}
    for key, value in tags.items():
        if "location" in key.lower() or "gps" in key.lower():
            gps_data[key] = value
    return gps_data

def extract_coordinates_from_tags(tags):
    gps_data = []
    import re
    for key, value in tags.items():
        if "ISO6709" in key and re.match(r"^[\+\-]\d+(\.\d+)?[\+\-]\d+(\.\d+)?", value):
            match = re.match(r"^([\+\-]\d+(\.\d+)?)([\+\-]\d+(\.\d+)?).*", value)
            if match:
                lat = match.group(1)
                lon = match.group(3)
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

@app.route('/')
def index():
    # ВРЕМЕННАЯ СТРОКА ДЛЯ ДИАГНОСТИКИ:
    print("[PYTHON BACKEND] Корневой путь '/' был запрошен. Проверяем вывод print.")
    return jsonify({"status": "✅ Python Backend is up and running!"})

@app.route('/analyze', methods=['POST'])
def analyze_video():
    if 'file' not in request.files:
        return jsonify({'error': 'No file uploaded'}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400

    username = request.form.get('username', 'unknown_user')

    print(f"\n[PYTHON BACKEND] Получен файл '{file.filename}' от пользователя '{username}' на /analyze.")

    try:
        upload_result = cloudinary.uploader.upload(
            file,
            resource_type="video",
            folder="hife_video_analysis",
            overwrite=True,
            quality="auto",
            format="mp4"
        )

        public_id = upload_result.get('public_id')
        cloudinary_url = upload_result.get('secure_url')
        
        basic_metadata = {
            "format": upload_result.get('format'),
            "duration": upload_result.get('duration'),
            "width": upload_result.get('width'),
            "height": upload_result.get('height'),
            "size_bytes": upload_result.get('bytes'),
            "bit_rate": upload_result.get('bit_rate')
        }
        
        task_status = "completed"
        task_message = "Видео успешно загружено на Cloudinary и получены базовые метаданные."

        with Session() as session:
            new_task = Task(
                task_id=public_id,
                username=username,
                status=task_status,
                filename=file.filename,
                cloudinary_url=cloudinary_url,
                video_metadata=basic_metadata, 
                message=task_message
            )
            session.add(new_task)
            session.commit()
            print(f"[PYTHON BACKEND] Задача '{public_id}' сохранена в БД.")

        print(f"[PYTHON BACKEND] Видео '{file.filename}' загружено на Cloudinary. Public ID: {public_id}")
        return jsonify({
            "status": "task_created",
            "taskId": public_id,
            "message": task_message,
            "cloudinary_url": cloudinary_url,
            "metadata": basic_metadata
        }), 200

    except cloudinary.exceptions.Error as e:
        print(f"[PYTHON BACKEND] Cloudinary Error: {e}")
        with Session() as session:
            session.rollback()
        return jsonify({"error": f"Cloudinary upload failed: {str(e)}"}), 500
    except Exception as e:
        print(f"[PYTHON BACKEND] Общая ошибка: {e}")
        with Session() as session:
            session.rollback()
        return jsonify({"error": f"Internal server error: {str(e)}"}), 500

@app.route('/task-status/<path:task_id>', methods=['GET'])
def get_task_status(task_id):
    # ... (ВАШИ СУЩЕСТВУЮЩИЕ ДИАГНОСТИЧЕСКИЕ print-Ы ЗДЕСЬ) ...
    print(f"\n[PYTHON BACKEND] Получен запрос статуса для task_id: '{task_id}'")
    with Session() as session:
        print(f"[PYTHON BACKEND] Поиск задачи в БД с task_id: '{task_id}'")
        task_info = session.query(Task).filter_by(task_id=task_id).first()
        if task_info:
            print(f"[PYTHON BACKEND] Задача найдена в БД: {task_info.task_id}, статус: {task_info.status}")
            return jsonify(task_info.to_dict()), 200
        else:
            print(f"[PYTHON BACKEND] Задача с task_id '{task_id}' НЕ НАЙДЕНА в БД.")
            return jsonify({"message": "Task not found."}), 404

@app.route('/heavy-tasks/pending', methods=['GET'])
def get_heavy_tasks():
    return jsonify({"message": "No heavy tasks pending for local worker yet."}), 200

if __name__ == '__main__':
    # ЭТОТ БЛОК НЕ ВЫПОЛНЯЕТСЯ НА RENDER ПРИ ЗАПУСКЕ GUNICORN/WAITRESS
    # create_tables() # УДАЛЕНО ОТСЮДА
    from waitress import serve
    port = int(os.environ.get('PORT', 8080))
    serve(app, host='0.0.0.0', port=port)
