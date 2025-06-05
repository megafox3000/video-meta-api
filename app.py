import os
import cloudinary
import cloudinary.uploader
import cloudinary.api
from flask import Flask, request, jsonify, redirect, url_for
from flask_cors import CORS
from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, JSON
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime
import hashlib
import time
import requests

app = Flask(__name__)
CORS(app)

# Cloudinary configuration
# Убедитесь, что эти переменные окружения установлены на Render.com
cloudinary.config(
    cloud_name = os.environ.get('CLOUDINARY_CLOUD_NAME'),
    api_key = os.environ.get('CLOUDINARY_API_KEY'),
    api_secret = os.environ.get('CLOUDINARY_API_SECRET'),
    secure = True
)

# Database configuration
DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL environment variable is not set!")

# КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ: Добавляем параметры SSL для подключения к PostgreSQL на Render.com
connect_args = {}
if DATABASE_URL.startswith("postgresql://") or DATABASE_URL.startswith("postgres://"):
    if "sslmode=" not in DATABASE_URL:
        connect_args["sslmode"] = "require"

engine = create_engine(DATABASE_URL, connect_args=connect_args)

Base = declarative_base()
Session = sessionmaker(bind=engine)

# Task model definition
class Task(Base):
    __tablename__ = 'tasks'

    id = Column(Integer, primary_key=True)
    task_id = Column(String, unique=True, nullable=False)
    instagram_username = Column(String)
    email = Column(String)
    linkedin_profile = Column(String)
    original_filename = Column(String)
    status = Column(String)
    cloudinary_url = Column(String)
    video_metadata = Column(JSON)
    message = Column(Text)
    timestamp = Column(DateTime, default=datetime.now)

    def __repr__(self):
        return f"<Task(task_id='{self.task_id}', status='{self.status}')>"

    def to_dict(self):
        return {
            "taskId": self.task_id,
            "instagram_username": self.instagram_username,
            "email": self.email,
            "linkedin_profile": self.linkedin_profile,
            "originalFilename": self.original_filename,
            "status": self.status,
            "cloudinary_url": self.cloudinary_url,
            "metadata": self.video_metadata,
            "message": self.message,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None
        }

def create_tables():
    Base.metadata.create_all(engine)
    print("Database tables created or already exist.")

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
        return data.get("display_name", "Address not found.")
    except Exception as e:
        print(f"Geocoding error: {e}")
        return f"Geocoding error: {e}"

# ----------- API ENDPOINTS -----------

@app.route('/')
def index():
    print("[PYTHON BACKEND] Root path '/' was requested. Checking print output.")
    return jsonify({"status": "✅ Python Backend is up and running!"})

@app.route('/upload_video', methods=['POST'])
def upload_video():
    session = Session()
    try:
        if 'video' not in request.files:
            print("[UPLOAD] No video file provided in request.")
            return jsonify({"error": "No video file provided"}), 400

        file = request.files['video']
        filename = file.filename

        if filename == '':
            print("[UPLOAD] No selected video file.")
            return jsonify({"error": "No selected video file"}), 400

        instagram_username = request.form.get('instagram_username')
        email = request.form.get('email')
        linkedin_profile = request.form.get('linkedin_profile')

        cleaned_username = "".join(c for c in (instagram_username or '').strip() if c.isalnum() or c in ('_', '-')).strip()
        if not cleaned_username:
            print("[UPLOAD] Instagram username is empty or invalid after cleaning.")
            return jsonify({"error": "Instagram username is required and must be valid."}), 400

        original_filename_base = os.path.splitext(filename)[0]
        full_public_id = f"hife_video_analysis/{cleaned_username}/{original_filename_base}"

        print(f"[{full_public_id}] Received upload request for file: '{filename}'")
        print(f"[{full_public_id}] User data: Instagram='{instagram_username}', Email='{email}', LinkedIn='{linkedin_profile}'")

        existing_task = session.query(Task).filter_by(task_id=full_public_id).first()

        should_reupload = False
        cloudinary_resource_info = None

        if existing_task:
            print(f"[{full_public_id}] Task with task_id '{full_public_id}' found in DB. Checking Cloudinary for full metadata...")
            try:
                # Попытка получить свежие метаданные из Cloudinary
                cloudinary_resource_info = cloudinary.api.resource(full_public_id, resource_type="video")
                
                # Проверяем, есть ли duration и что оно не равно 0
                if cloudinary_resource_info and cloudinary_resource_info.get('duration', 0) > 0:
                    print(f"[{full_public_id}] Cloudinary resource found with valid duration. Updating DB.")
                    # Обновляем нашу БД свежими данными от Cloudinary
                    existing_task.cloudinary_url = cloudinary_resource_info.get('secure_url')
                    existing_task.video_metadata = cloudinary_resource_info
                    existing_task.status = 'completed'
                    existing_task.message = 'Video found on Cloudinary with valid metadata. DB info updated.'
                    existing_task.timestamp = datetime.now()
                    existing_task.instagram_username = instagram_username # Обновляем пользовательские данные
                    existing_task.email = email
                    existing_task.linkedin_profile = linkedin_profile
                    session.commit()
                    return jsonify({
                        'message': 'Video info updated from Cloudinary.',
                        'taskId': existing_task.task_id,
                        'cloudinary_url': existing_task.cloudinary_url,
                        'originalFilename': existing_task.original_filename,
                        'metadata': existing_task.video_metadata,
                        'status': existing_task.status
                    }), 200
                else:
                    # Ресурс найден на Cloudinary, но duration некорректно
                    print(f"[{full_public_id}] Cloudinary resource found but duration is 0 or missing. Forcing re-upload.")
                    should_reupload = True
            except cloudinary.api.NotFound:
                # Ресурс не найден на Cloudinary, хотя есть в нашей БД
                print(f"[{full_public_id}] Cloudinary resource NOT found despite DB record. Forcing re-upload.")
                should_reupload = True
            except Exception as e:
                # Другая ошибка при запросе к Cloudinary
                print(f"[{full_public_id}] Error checking Cloudinary resource: {e}. Forcing re-upload.")
                should_reupload = True
        else:
            # Задачи нет в нашей БД, это новая загрузка
            print(f"[{full_public_id}] Task not found in DB. This is a new upload.")
            should_reupload = True
        
        # Если should_reupload = True (файл не найден в БД, или найден, но нет на Cloudinary,
        # или есть, но метаданные некорректны), выполняем загрузку
        if should_reupload:
            print(f"[{full_public_id}] Uploading/re-uploading video to Cloudinary to ensure full metadata...")
            upload_result = cloudinary.uploader.upload(
                file,
                resource_type="video",
                folder=f"hife_video_analysis/{cleaned_username}",
                public_id=original_filename_base,
                unique_filename=False,
                overwrite=True, # Обязательно для обновления метаданных
                quality="auto",
                format="mp4",
                tags=["hife_analysis", cleaned_username]
            )
            print(f"[{full_public_id}] Cloudinary response after new upload: {upload_result}")

            if upload_result and upload_result.get('secure_url'):
                cloudinary_url = upload_result['secure_url']
                print(f"[{full_public_id}] Cloudinary URL: {cloudinary_url}")

                if existing_task:
                    print(f"[{full_public_id}] Updating existing task in DB after re-upload.")
                    existing_task.instagram_username = instagram_username
                    existing_task.email = email
                    existing_task.linkedin_profile = linkedin_profile
                    existing_task.original_filename = filename
                    existing_task.status = 'completed'
                    existing_task.timestamp = datetime.now()
                    existing_task.cloudinary_url = cloudinary_url
                    existing_task.video_metadata = upload_result # Обновляем метаданные из нового ответа
                    existing_task.message = 'Video re-uploaded to Cloudinary and DB info updated.'
                else:
                    print(f"[{full_public_id}] Creating a new task in DB after upload.")
                    new_task = Task(
                        task_id=full_public_id,
                        instagram_username=instagram_username,
                        email=email,
                        linkedin_profile=linkedin_profile,
                        original_filename=filename,
                        status='completed',
                        timestamp=datetime.now(),
                        cloudinary_url=cloudinary_url,
                        video_metadata=upload_result,
                        message='Video successfully uploaded to Cloudinary and full metadata obtained.'
                    )
                    session.add(new_task)
                    existing_task = new_task # Для единообразия возвращаемого значения
                session.commit()
                print(f"[{full_public_id}] DB changes successfully committed.")
                return jsonify({'message': 'Video uploaded and task created/updated', 'taskId': existing_task.task_id, 'cloudinary_url': cloudinary_url, 'metadata': existing_task.video_metadata, 'originalFilename': existing_task.original_filename}), 200
            else:
                print(f"[{full_public_id}] Cloudinary upload failed: secure_url missing in response.")
                return jsonify({'error': 'Cloudinary upload failed'}), 500
        else:
            # Этот блок не должен быть достигнут, если логика верна, но для безопасности
            print(f"[{full_public_id}] Unexpected state: should_reupload was False, but no return was made. This indicates a logic error.")
            return jsonify({'error': 'Internal server logic error'}), 500

    except SQLAlchemyError as e:
        session.rollback()
        print(f"[{full_public_id if 'full_public_id' in locals() else 'N/A'}] Database error: {e}")
        print(f"Original error info: {e.orig.pginfo if hasattr(e.orig, 'pginfo') else 'N/A'}")
        return jsonify({'error': 'Database error', 'details': str(e)}), 500
    except Exception as e:
        session.rollback()
        print(f"[{full_public_id if 'full_public_id' in locals() else 'N/A'}] An unexpected error occurred during upload: {e}")
        return jsonify({'error': 'An unexpected error occurred', 'details': str(e)}), 500
    finally:
        session.close()

@app.route('/task-status/<path:task_id>', methods=['GET'])
def get_task_status(task_id):
    session = Session()
    try:
        print(f"\n[STATUS] Received status request for task_id: '{task_id}'")
        task_info = session.query(Task).filter_by(task_id=task_id).first()
        if task_info:
            print(f"[STATUS] Task found in DB: {task_info.task_id}, status: {task_info.status}")
            return jsonify(task_info.to_dict()), 200
        else:
            print(f"[STATUS] Task with task_id '{task_id}' NOT FOUND in DB.")
            return jsonify({"message": "Task not found."}), 404
    finally:
        session.close()

@app.route('/heavy-tasks/pending', methods=['GET'])
def get_heavy_tasks():
    print("[HEAVY_TASKS] Request for heavy tasks received.")
    return jsonify({"message": "No heavy tasks pending for local worker yet."}), 200

# НОВЫЙ/ОБНОВЛЕННЫЙ ЭНДПОИНТ ДЛЯ КОНКАТЕНАЦИИ ВИДЕО
@app.route('/concatenate_videos', methods=['POST'])
def concatenate_videos():
    session = Session()
    try:
        data = request.get_json()
        public_ids_from_frontend = data.get('public_ids')

        if not public_ids_from_frontend or not isinstance(public_ids_from_frontend, list) or len(public_ids_from_frontend) < 2:
            print("[CONCAT] Concatenation request received with less than 2 public_ids.")
            return jsonify({'error': 'Please provide at least two public_ids of videos to concatenate.'}), 400

        print(f"[CONCAT] Received concatenation request for public_ids: {public_ids_from_frontend}")

        video_durations = []
        base_video_url = None 
        
        # Получаем метаданные из нашей БД
        for i, public_id_full_path in enumerate(public_ids_from_frontend):
            print(f"[CONCAT] Getting metadata from DB for video: {public_id_full_path}")
            db_task = session.query(Task).filter_by(task_id=public_id_full_path).first()

            if not db_task:
                print(f"[CONCAT] Error: Video with public_id {public_id_full_path} not found in local DB.")
                return jsonify({'error': f'Video with public_id {public_id_full_path} not found in DB. Please ensure it was uploaded successfully.'}), 404
            
            # Проверяем наличие duration и что оно больше 0
            duration = db_task.video_metadata.get('duration', 0)
            if duration <= 0:
                print(f"[CONCAT] Warning: Video {public_id_full_path} has 0 duration in DB metadata. Cannot concatenate meaningfully.")
                print(f"[CONCAT] Full DB metadata for {public_id_full_path}: {db_task.video_metadata}") 
                return jsonify({'error': f'Cannot concatenate: Video {public_id_full_path} has zero or invalid duration in DB. Please re-upload it.'}), 400 
            
            video_durations.append(duration)
            print(f"[CONCAT] Duration for {public_id_full_path} from DB: {duration} seconds.")

            # Сохраняем URL первого видео для использования в качестве базы для upload()
            if i == 0:
                base_video_url = db_task.cloudinary_url
            
        # Шаг 2: Создать список трансформаций для Cloudinary upload
        transformations = []
        # Теперь первый элемент в transformations - это overlay для второго видео (и далее)
        # Параметры video_codec, format, quality перемещены на верхний уровень upload()

        current_offset_duration = 0
        for i, public_id_for_overlay in enumerate(public_ids_from_frontend): 
            if i == 0:
                current_offset_duration += video_durations[i]
                continue

            transformations.append({
                "overlay": {"public_id": public_id_for_overlay, "resource_type": "video"},
                "flag": "splice",
                "start_offset": f"{current_offset_duration:.2f}",
            })
            current_offset_duration += video_durations[i]

        print(f"[CONCAT] Generated transformations: {transformations}")

        # Шаг 3: Загрузить объединенное видео напрямую, используя URL первого видео в качестве основы
        concat_folder = "hife_video_analysis/concatenated"
        concat_unique_string = f"concatenated-{'_'.join(public_ids_from_frontend)}-{time.time()}"
        new_concatenated_base_id = hashlib.sha256(concat_unique_string.encode()).hexdigest()[:20]
        new_concatenated_full_public_id = f"{concat_folder}/{new_concatenated_base_id}"
        new_filename = f"concatenated_video_{new_concatenated_base_id}.mp4"

        print(f"[CONCAT] Uploading concatenated video to Cloudinary with new public_id: {new_concatenated_full_public_id}")

        upload_result = cloudinary.uploader.upload(
            base_video_url, # <--- Передаем URL Cloudinary в качестве базового источника
            resource_type="video", # Указываем тип ресурса на верхнем уровне
            folder=concat_folder,
            public_id=new_concatenated_base_id,
            unique_filename=False,
            overwrite=True,
            transformation=transformations, # Список трансформаций теперь содержит только splice overlay
            video_codec="auto", # <--- Перемещено сюда
            format="mp4",       # <--- Перемещено сюда
            quality="auto"      # <--- Перемещено сюда
        )
        print(f"[CONCAT] Result of concatenated video upload to Cloudinary: {upload_result}")

        if upload_result and upload_result.get('secure_url'):
            new_video_url = upload_result['secure_url']
            print(f"[CONCAT] New URL for concatenated video: {new_video_url}")

            new_task = Task(
                task_id=new_concatenated_full_public_id,
                instagram_username=request.form.get('instagram_username', 'concatenated'),
                email=request.form.get('email', 'concatenated@example.com'),
                linkedin_profile=request.form.get('linkedin_profile', 'N/A'),
                original_filename=new_filename,
                status='concatenated',
                timestamp=datetime.now(),
                cloudinary_url=new_video_url,
                video_metadata=upload_result
            )
            session.add(new_task)
            session.commit()
            print(f"[CONCAT] Task for concatenated video saved to DB with ID: {new_task.task_id}")

            return jsonify({
                'message': 'Videos successfully concatenated',
                'new_public_id': new_task.task_id,
                'new_video_url': new_video_url,
                'metadata': upload_result
            }), 200
        else:
            print("[CONCAT] Cloudinary upload failed: secure_url missing in response.")
            return jsonify({'error': 'Cloudinary upload failed'}), 500

    except SQLAlchemyError as e:
        session.rollback()
        print(f"[CONCAT] Database error during concatenation: {e}")
        return jsonify({'error': 'Database error', 'details': str(e)}), 500
    except Exception as e:
        session.rollback()
        print(f"[CONCAT] An unexpected error occurred during concatenation: {e}")
        return jsonify({'error': 'An unexpected error occurred', 'details': str(e)}), 500
    finally:
        session.close()

if __name__ == '__main__':
    from waitress import serve
    port = int(os.environ.get('PORT', 8080))
    serve(app, host='0.0.0.0', port=port)
