import os
from flask import Flask, request, jsonify
from flask_cors import CORS
from datetime import datetime

import cloudinary
import cloudinary.uploader
import cloudinary.api # Это было добавлено ранее, но важно, чтобы оно было!

from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, JSON
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import SQLAlchemyError # ИСПРАВЛЕНИЕ: Добавлен импорт SQLAlchemyError
import requests

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
    video_metadata = Column(JSON) # Это уже было JSON-поле
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
# Это позволит таблицам создаваться при запуске приложения Gunicorn'ом/Waitress'ом.
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
    print("[PYTHON BACKEND] Корневой путь '/' был запрошен. Проверяем вывод print.")
    return jsonify({"status": "✅ Python Backend is up and running!"})

# НОВАЯ/ОБНОВЛЕННАЯ ФУНКЦИЯ ДЛЯ ЗАГРУЗКИ ВИДЕО
@app.route('/upload_video', methods=['POST'])
def upload_video():
    print("\n[PYTHON BACKEND] Получен запрос на /upload_video.")

    if 'video' not in request.files:
        print("[PYTHON BACKEND] No video file provided in request.")
        return jsonify({"error": "No video file provided"}), 400
    
    video_file = request.files['video']
    if video_file.filename == '':
        print("[PYTHON BACKEND] No selected video file.")
        return jsonify({"error": "No selected video file"}), 400

    instagram_username = request.form.get('instagram_username')
    
    print(f"[PYTHON BACKEND] Загружаем файл '{video_file.filename}' для пользователя Instagram: '{instagram_username}'")

    if not instagram_username:
        print("[PYTHON BACKEND] Instagram username is missing.")
        return jsonify({"error": "Instagram username is required"}), 400
    
    # Очистка имени пользователя для безопасного использования в пути Cloudinary
    cleaned_username = "".join(c for c in instagram_username if c.isalnum() or c in ('_', '-')).strip()
    if not cleaned_username:
        print(f"[PYTHON BACKEND] Cleaned Instagram username is empty for: {instagram_username}")
        return jsonify({"error": "Invalid Instagram username"}), 400

    session = Session() # ИСПРАВЛЕНИЕ: Создаем сессию здесь, чтобы использовать её в try/except блоках
    try:
        original_filename_base = os.path.splitext(video_file.filename)[0]
        full_public_id = f"hife_video_analysis/{cleaned_username}/{original_filename_base}"

        # --- ПРОВЕРЯЕМ, СУЩЕСТВУЕТ ЛИ ЗАДАЧА УЖЕ В БД ---
        existing_task = session.query(Task).filter_by(task_id=full_public_id).first()

        cloudinary_url = None
        video_metadata = None
        resource_found_on_cloudinary = False

        if existing_task:
            print(f"[PYTHON BACKEND] Задача с task_id '{full_public_id}' уже существует. Попытка обновить информацию.")
            try:
                # Получаем информацию о ресурсе с Cloudinary
                resource_info = cloudinary.api.resource(full_public_id, resource_type="video")
                cloudinary_url = resource_info.get('secure_url')
                video_metadata = {k: v for k, v in resource_info.items() if k not in ['url', 'secure_url', 'type']}
                resource_found_on_cloudinary = True

                # Обновляем существующую запись в БД
                existing_task.status = 'completed'
                existing_task.cloudinary_url = cloudinary_url
                existing_task.video_metadata = video_metadata
                existing_task.message = 'Видео уже существует на Cloudinary. Информация в БД обновлена.'
                existing_task.timestamp = datetime.now()
                session.commit()

                return jsonify({
                    'message': 'Video already exists, info updated.',
                    'taskId': existing_task.task_id,
                    'cloudinary_url': existing_task.cloudinary_url,
                    'original_filename': existing_task.filename,
                    'metadata': existing_task.video_metadata,
                    'status': existing_task.status
                }), 200

            except cloudinary.exceptions.Error as e:
                print(f"[PYTHON BACKEND] Ошибка Cloudinary при попытке получить существующий ресурс ({full_public_id}): {e}")
                # Если файл не найден на Cloudinary, мы проигнорируем эту ошибку
                # и продолжим к загрузке нового файла ниже.
                pass 

        # Если задача не найдена в БД ИЛИ ресурс не был найден на Cloudinary
        if not existing_task or not resource_found_on_cloudinary:
            print(f"[PYTHON BACKEND] Загружаем/перезагружаем видео на Cloudinary для '{full_public_id}'.")
            upload_result = cloudinary.uploader.upload(
                video_file,
                resource_type="video",
                folder=f"hife_video_analysis/{cleaned_username}",
                public_id=original_filename_base, # Имя файла на Cloudinary без расширения, папка уже в folder
                unique_filename=False,
                overwrite=True,
                quality="auto",
                format="mp4",
                tags=["hife_analysis", cleaned_username]
            )
            
            uploaded_video_info = upload_result
            cloudinary_url = uploaded_video_info['secure_url']
            video_metadata = {k: v for k, v in uploaded_video_info.items() if k not in ['url', 'secure_url', 'type']}

            if existing_task:
                # Обновляем существующую запись
                existing_task.status = 'completed'
                existing_task.cloudinary_url = cloudinary_url
                existing_task.video_metadata = video_metadata
                existing_task.message = 'Видео загружено заново на Cloudinary и информация в БД обновлена.'
                existing_task.timestamp = datetime.now()
            else:
                # Создаем новую запись
                new_task = Task(
                    task_id=full_public_id, # Используем full_public_id как task_id
                    username=cleaned_username,
                    status='completed',
                    filename=video_file.filename,
                    cloudinary_url=cloudinary_url,
                    video_metadata=video_metadata,
                    message='Видео успешно загружено на Cloudinary и получены полные метаданные.',
                    timestamp=datetime.now()
                )
                session.add(new_task)
                existing_task = new_task
            
            session.commit()
            print(f"[PYTHON BACKEND] Задача '{existing_task.task_id}' сохранена/обновлена в БД.")

            return jsonify({
                "status": "task_created",
                "taskId": existing_task.task_id,
                "message": existing_task.message,
                "cloudinary_url": existing_task.cloudinary_url,
                "metadata": existing_task.video_metadata
            }), 200

    except cloudinary.exceptions.Error as e:
        session.rollback() # Откатываем изменения в случае ошибки
        error_message = f"Cloudinary Error during upload: {e}"
        print(f"[PYTHON BACKEND] {error_message}")
        return jsonify({"error": f"Cloudinary upload failed: {str(e)}"}), 500
    
    # ИСПРАВЛЕНИЕ: Отступ этого блока теперь правильный.
    except Exception as e:
        session.rollback() # Откатываем изменения в случае ошибки
        error_message = f"General error during upload: {e}"
        print(f"[PYTHON BACKEND] {error_message}")
        
        # ИСПРАВЛЕНИЕ: Универсальный вывод ошибок с учетом SQLAlchemyError
        if isinstance(e, SQLAlchemyError) and hasattr(e.orig, 'pginfo'):
            print(f"[SQL: {e.orig.pginfo.query}]")
            print(f"[parameters: {e.orig.pginfo.parameters}]")
        else:
            print(f"[PYTHON BACKEND] Детали ошибки: {str(e)}")

        return jsonify({'error': error_message}), 500
    finally:
        session.close() # ИСПРАВЛЕНИЕ: Обязательно закрываем сессию SQLAlchemy

@app.route('/task-status/<path:task_id>', methods=['GET'])
def get_task_status(task_id):
    print(f"\n[PYTHON BACKEND] Получен запрос статуса для task_id: '{task_id}'")
    session = Session() # ИСПРАВЛЕНИЕ: Создаем сессию
    try:
        print(f"[PYTHON BACKEND] Поиск задачи в БД с task_id: '{task_id}'")
        task_info = session.query(Task).filter_by(task_id=task_id).first()
        if task_info:
            print(f"[PYTHON BACKEND] Задача найдена в БД: {task_info.task_id}, статус: {task_info.status}")
            return jsonify(task_info.to_dict()), 200
        else:
            print(f"[PYTHON BACKEND] Задача с task_id '{task_id}' НЕ НАЙДЕНА в БД.")
            return jsonify({"message": "Task not found."}), 404
    finally:
        session.close() # ИСПРАВЛЕНИЕ: Закрываем сессию

@app.route('/heavy-tasks/pending', methods=['GET'])
def get_heavy_tasks():
    return jsonify({"message": "No heavy tasks pending for local worker yet."}), 200

if __name__ == '__main__':
    # ЭТОТ БЛОК НЕ ВЫПОЛНЯЕТСЯ НА RENDER ПРИ ЗАПУСКЕ GUNICORN/WAITRESS
    # create_tables() вызывается выше при импорте модуля
    from waitress import serve
    port = int(os.environ.get('PORT', 8080))
    serve(app, host='0.0.0.0', port=port)
