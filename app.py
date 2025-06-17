# app.py
import os
import cloudinary
import cloudinary.uploader
import cloudinary.api
from flask import Flask, request, jsonify, redirect, url_for, g
from flask_cors import CORS
from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, JSON
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime
import hashlib
import time
import requests
import json
import shotstack_service
import re # Добавлен импорт re для reverse_geocode
import logging # Добавлен импорт logging

# --- Configure Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)

# --- CORS Configuration ---
# Это очень важно! Разрешает запросы с вашего фронтенда на GitHub Pages.
CORS(app, resources={r"/*": {"origins": [
    "https://megafox3000.github.io",
    "http://localhost:5500", # Пример для локального сервера (может быть другим)
    "http://127.0.0.1:5500"
], "methods": ["GET", "POST", "OPTIONS", "HEAD"], "headers": ["Content-Type", "Authorization", "X-Requested-With"]}}, supports_credentials=True)


# Конфигурация Cloudinary
cloudinary.config(
    cloud_name = os.environ.get('CLOUDINARY_CLOUD_NAME'),
    api_key = os.environ.get('CLOUDINARY_API_KEY'),
    api_secret = os.environ.get('CLOUDINARY_API_SECRET'),
    secure = True
)

# Конфигурация базы данных
DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    logger.error("DATABASE_URL environment variable is not set!")
    raise RuntimeError("DATABASE_URL environment variable is not set!")

connect_args = {}
if DATABASE_URL.startswith("postgresql://") or DATABASE_URL.startswith("postgres://"):
    if "sslmode=" not in DATABASE_URL:
        connect_args["sslmode"] = "require"

engine = create_engine(DATABASE_URL, connect_args=connect_args)

Base = declarative_base()
Session = sessionmaker(bind=engine)

# Определение модели задачи
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
    # --- НОВЫЕ ПОЛЯ ДЛЯ SHOTSTACK ---
    shotstackRenderId = Column(String) # ID, который Shotstack возвращает после запуска рендера
    shotstackUrl = Column(String)      # Итоговый URL сгенерированного видео от Shotstack
    posterUrl = Column(String) # Постер (раскомментировано и активировано)

    def __repr__(self):
        return f"<Task(task_id='{self.task_id}', status='{self.status}')>"

    def to_dict(self):
        return {
            "id": self.id, # Добавляем id для удобства
            "taskId": self.task_id,
            "instagram_username": self.instagram_username,
            "email": self.email,
            "linkedin_profile": self.linkedin_profile,
            "originalFilename": self.original_filename,
            "status": self.status,
            "cloudinary_url": self.cloudinary_url,
            "metadata": self.video_metadata,
            "message": self.message,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
            "shotstackRenderId": self.shotstackRenderId, # Добавляем в словарь
            "shotstackUrl": self.shotstackUrl,            # Добавляем в словарь
            "posterUrl": self.posterUrl # Добавляем в словарь
        }

def create_tables():
    Base.metadata.create_all(engine)
    logger.info("Database tables created or already exist.")

# Создание таблиц при запуске
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
    for key, value in tags.items():
        if "ISO6709" in key and re.match(r"^[\+\-]\d+(\.\d+)?[\+\-]\d+(\.\d+)?", str(value)): # Added str() cast
            match = re.match(r"^([\+\-]\d+(\.\d+)?)([\+\-]\d+(\.\d+)?).*", str(value))
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
        response.raise_for_status()
        data = response.json()
        return data.get("display_name", "Address not found.")
    except requests.exceptions.RequestException as e:
        logger.error(f"Geocoding error: {e}")
        return f"Geocoding error: {e}"
    except json.JSONDecodeError:
        logger.error(f"Geocoding error: Could not decode JSON from response.")
        return "Geocoding error: Invalid response from geocoding service."

# ----------- API ENDPOINTS -----------

@app.route('/')
def index():
    logger.info("Root path '/' was requested.")
    return jsonify({"status": "✅ Python Backend is up and running!"})

@app.route('/upload_video', methods=['POST'])
def upload_video():
    session = Session()
    task_id = "N/A" # Default task_id for logging in case of early failure
    try:
        if 'video' not in request.files:
            logger.warning("[UPLOAD] No video file provided in request.")
            return jsonify({"error": "No video file provided"}), 400

        file = request.files['video']
        filename = file.filename

        if filename == '':
            logger.warning("[UPLOAD] No selected video file.")
            return jsonify({"error": "No selected video file"}), 400

        instagram_username = request.form.get('instagram_username')
        email = request.form.get('email')
        linkedin_profile = request.form.get('linkedin_profile')

        cleaned_username = "".join(c for c in (instagram_username or '').strip() if c.isalnum() or c in ('_', '-')).strip()
        if not cleaned_username:
            logger.warning("[UPLOAD] Instagram username is empty or invalid after cleaning. Using 'anonymous'.")
            cleaned_username = "anonymous" # Fallback if no valid username

        original_filename_base = os.path.splitext(filename)[0]
        # Используем уникальный идентификатор на основе хэша
        unique_hash = hashlib.md5(f"{cleaned_username}/{filename}/{datetime.now().timestamp()}".encode()).hexdigest()
        task_id = f"{cleaned_username}/{original_filename_base}_{unique_hash[:8]}"

        logger.info(f"[{task_id}] Received upload request for file: '{filename}'")
        logger.info(f"[{task_id}] User data: Instagram='{instagram_username}', Email='{email}', LinkedIn='{linkedin_profile}'")

        # Проверяем, существует ли уже задача с таким task_id (хотя с уникальным хэшем это маловероятно)
        existing_task = session.query(Task).filter_by(task_id=task_id).first()
        if existing_task:
            logger.info(f"[{task_id}] Task already exists in DB. Overwriting with new upload.")
            # Можно обновить существующую задачу, а не создавать новую, если это желаемое поведение
            # Для простоты, мы перезапишем ее данные после загрузки в Cloudinary
            pass # Продолжаем, чтобы перезаписать данные Cloudinary

        logger.info(f"[{task_id}] Uploading video to Cloudinary...")
        upload_result = cloudinary.uploader.upload(
            file,
            resource_type="video",
            folder=f"hife_video_analysis/{cleaned_username}",
            public_id=f"{original_filename_base}_{unique_hash[:8]}", # Используем уникальный public_id в Cloudinary
            unique_filename=False,
            overwrite=True,
            quality="auto",
            format="mp4",
            tags=["hife_analysis", cleaned_username]
        )
        logger.info(f"[{task_id}] Cloudinary upload response keys: {upload_result.keys()}")

        if upload_result and upload_result.get('secure_url'):
            cloudinary_url = upload_result['secure_url']
            logger.info(f"[{task_id}] Cloudinary URL: {cloudinary_url}")

            # --- CHECK DURATION AND OTHER FIELDS AFTER UPLOAD ---
            new_upload_duration = upload_result.get('duration', 0)
            new_upload_width = upload_result.get('width', 0)
            new_upload_height = upload_result.get('height', 0)
            new_upload_bytes = upload_result.get('bytes', 0)

            if new_upload_duration <= 0 or new_upload_width <= 0 or new_upload_height <= 0 or new_upload_bytes <= 0:
                logger.warning(f"[{task_id}] WARNING: Video uploaded, but essential metadata (duration/resolution/size) is still 0 or missing from Cloudinary response. Full metadata: {upload_result}")
                if existing_task:
                    existing_task.status = 'cloudinary_metadata_incomplete'
                    existing_task.message = 'Video uploaded but could not retrieve complete and valid metadata from Cloudinary.'
                    existing_task.cloudinary_url = cloudinary_url
                    existing_task.video_metadata = upload_result
                else:
                    new_task = Task(
                        task_id=task_id,
                        instagram_username=instagram_username,
                        email=email,
                        linkedin_profile=linkedin_profile,
                        original_filename=filename,
                        status='cloudinary_metadata_incomplete',
                        timestamp=datetime.now(),
                        cloudinary_url=cloudinary_url,
                        video_metadata=upload_result,
                        message='Video uploaded but could not retrieve complete and valid metadata from Cloudinary.'
                    )
                    session.add(new_task)
                session.commit()
                return jsonify({
                    'error': 'Video uploaded but could not retrieve complete and valid metadata from Cloudinary. Please try again or check video file.',
                    'taskId': task_id,
                    'cloudinary_url': cloudinary_url,
                    'metadata': upload_result
                }), 500
            # --- END OF DURATION AND OTHER FIELDS CHECK AFTER UPLOAD ---

            if existing_task:
                existing_task.instagram_username = instagram_username
                existing_task.email = email
                existing_task.linkedin_profile = linkedin_profile
                existing_task.original_filename = filename
                existing_task.status = 'completed'
                existing_task.timestamp = datetime.now()
                existing_task.cloudinary_url = cloudinary_url
                existing_task.video_metadata = upload_result
                existing_task.message = 'Video successfully uploaded to Cloudinary and full metadata obtained.'
            else:
                new_task = Task(
                    task_id=task_id,
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
            session.commit()
            logger.info(f"[{task_id}] Task successfully created/updated and committed to DB.")
            return jsonify({
                'message': 'Video uploaded and task created.',
                'taskId': task_id,
                'cloudinary_url': cloudinary_url,
                'metadata': upload_result,
                'originalFilename': filename,
                'status': 'completed'
            }), 200
        else:
            if existing_task:
                existing_task.status = 'cloudinary_upload_failed'
                existing_task.message = 'Cloudinary upload failed: secure_url missing in response.'
            else:
                new_task = Task(
                    task_id=task_id,
                    instagram_username=instagram_username,
                    email=email,
                    linkedin_profile=linkedin_profile,
                    original_filename=filename,
                    status='cloudinary_upload_failed',
                    timestamp=datetime.now(),
                    cloudinary_url=None,
                    video_metadata={},
                    message='Cloudinary upload failed: secure_url missing in response.'
                )
                session.add(new_task)
            session.commit()
            logger.error(f"[{task_id}] Cloudinary upload failed: secure_url missing in response. Response: {upload_result}")
            return jsonify({'error': 'Cloudinary upload failed'}), 500

    except SQLAlchemyError as e:
        session.rollback()
        logger.exception(f"[{task_id if 'task_id' in locals() else 'N/A'}] Database error during upload:")
        return jsonify({'error': 'Database error', 'details': str(e)}), 500
    except requests.exceptions.RequestException as err:
        session.rollback()
        logger.exception(f"[{task_id if 'task_id' in locals() else 'N/A'}] Network error during Cloudinary upload:")
        if 'task_id' in locals() and task_id != "N/A":
            task_to_update = session.query(Task).filter_by(task_id=task_id).first()
            if task_to_update:
                task_to_update.status = "failed"
                task_to_update.message = f"Upload network error: {str(err)}"
                session.commit()
        return jsonify({'error': f"Error communicating with Cloudinary: {err}", "details": str(err)}), 500
    except Exception as e:
        session.rollback()
        logger.exception(f"[{task_id if 'task_id' in locals() else 'N/A'}] An unexpected error occurred during upload:")
        if 'task_id' in locals() and task_id != "N/A":
            task_to_update = session.query(Task).filter_by(task_id=task_id).first()
            if task_to_update:
                task_to_update.status = "failed"
                task_to_update.message = f"Unexpected upload error: {str(e)}"
                session.commit()
        return jsonify({'error': 'An unexpected error occurred', 'details': str(e)}), 500
    finally:
        session.close()

@app.route('/task-status/<path:task_id>', methods=['GET'])
def get_task_status(task_id):
    session = Session()
    try:
        logger.info(f"[STATUS] Received status request for task_id: '{task_id}'")
        task_info = session.query(Task).filter_by(task_id=task_id).first()

        if not task_info:
            logger.warning(f"[STATUS] Task with task_id '{task_id}' NOT FOUND in DB.")
            return jsonify({"message": "Task not found."}), 404

        # Проверяем, является ли задача связанной с Shotstack рендерингом и еще не завершена
        if task_info.shotstackRenderId and \
           task_info.status not in ['completed', 'failed', 'concatenated_completed', 'concatenated_failed']:
            logger.info(f"[STATUS] Task {task_info.task_id} has Shotstack render ID. Checking Shotstack API...")
            try:
                status_info = shotstack_service.get_shotstack_render_status(task_info.shotstackRenderId)

                shotstack_status = status_info['status']
                shotstack_url = status_info['url']
                shotstack_poster_url = status_info.get('poster')
                shotstack_error_message = status_info['error_message']

                logger.info(f"[STATUS] Shotstack render status for {task_info.shotstackRenderId}: {shotstack_status}")

                if shotstack_status == 'done' and shotstack_url:
                    if task_id.startswith('concatenated_video_'):
                        task_info.status = 'concatenated_completed'
                        task_info.message = "Concatenated video rendered successfully."
                    else:
                        task_info.status = 'completed'
                        task_info.message = "Shotstack video rendered successfully."
                    task_info.shotstackUrl = shotstack_url
                    task_info.posterUrl = shotstack_poster_url # Обновляем posterUrl
                    session.commit()
                    logger.info(f"[STATUS] Shotstack render completed for {task_id}. URL: {shotstack_url}")
                    if shotstack_poster_url:
                        logger.info(f"[STATUS] Shotstack Poster URL: {shotstack_poster_url}")
                elif shotstack_status in ['failed', 'error', 'failed_due_to_timeout']:
                    if task_id.startswith('concatenated_video_'):
                        task_info.status = 'concatenated_failed'
                        task_info.message = f"Concatenated video rendering failed: {shotstack_error_message or 'Unknown Shotstack error'}"
                    else:
                        task_info.status = 'failed'
                        task_info.message = f"Shotstack rendering failed: {shotstack_error_message or 'Unknown Shotstack error'}"
                    task_info.shotstackUrl = None # Сбрасываем URL при ошибке
                    task_info.posterUrl = None # Сбрасываем posterUrl при ошибке
                    session.commit()
                    logger.error(f"[STATUS] Shotstack render failed for {task_id}. Error: {task_info.message}")
                else:
                    task_info.message = f"Shotstack render in progress: {shotstack_status}"
                    logger.info(f"[STATUS] Shotstack render still in progress for {task_id}. Status: {shotstack_status}")
                    # No DB update needed if status is still processing and not changed.

                response_data = task_info.to_dict()
                response_data['status'] = task_info.status # Ensure status is always up-to-date
                # If Shotstack API provided a poster_url even in pending/failed, prioritize it for immediate display
                if shotstack_poster_url:
                    response_data['posterUrl'] = shotstack_poster_url
                return jsonify(response_data), 200

            except requests.exceptions.RequestException as e:
                logger.error(f"[STATUS] Error querying Shotstack API for {task_info.shotstackRenderId}: {e}")
                task_info.message = f"Error checking Shotstack status: {e}"
                response_data = task_info.to_dict()
                # Include posterUrl if it was available before the API call error
                if 'shotstack_poster_url' in locals() and shotstack_poster_url:
                    response_data['posterUrl'] = shotstack_poster_url
                return jsonify(response_data), 200
            except Exception as e:
                logger.exception(f"[STATUS] Unexpected error during Shotstack status check for {task_info.shotstackRenderId}:")
                task_info.message = f"Unexpected error during Shotstack status check: {e}"
                response_data = task_info.to_dict()
                # Include posterUrl if it was available before the API call error
                if 'shotstack_poster_url' in locals() and shotstack_poster_url:
                    response_data['posterUrl'] = shotstack_poster_url
                return jsonify(response_data), 200


        logger.info(f"[STATUS] Task found in DB: {task_info.task_id}, current_status: {task_info.status}")
        return jsonify(task_info.to_dict()), 200
    except SQLAlchemyError as e:
        session.rollback()
        logger.exception(f"[STATUS] Database error fetching task status:")
        return jsonify({"error": "Database error", "details": str(e)}), 500
    except Exception as e:
        session.rollback()
        logger.exception(f"[STATUS] An unexpected error occurred in get_task_status:")
        return jsonify({"error": "An unexpected server error occurred", "details": str(e)}), 500
    finally:
        session.close()

@app.route('/generate-shotstack-video', methods=['POST'])
def generate_shotstack_video():
    session = Session()
    try:
        data = request.get_json()
        task_id = data.get('taskId')

        if not task_id:
            logger.warning("[SHOTSTACK] No taskId provided for Shotstack generation.")
            return jsonify({"error": "No taskId provided"}), 400

        task = session.query(Task).filter_by(task_id=task_id).first()
        if not task:
            logger.warning(f"[SHOTSTACK] Task {task_id} not found in DB.")
            return jsonify({"error": "Task not found."}), 404

        if not task.cloudinary_url:
            logger.warning(f"[SHOTSTACK] Task {task_id} has no Cloudinary URL. Cannot generate Shotstack video.")
            return jsonify({"error": "Video not uploaded to Cloudinary yet or URL missing."}), 400
        
        # Если видео уже в процессе рендеринга Shotstack, не запускаем повторно
        if task.status == 'shotstack_pending' and task.shotstackRenderId:
            logger.info(f"[SHOTSTACK] Task {task_id} is already in 'shotstack_pending' status with render ID {task.shotstackRenderId}. Not re-initiating.")
            return jsonify({
                "message": "Shotstack render already initiated and in progress.",
                "shotstackRenderId": task.shotstackRenderId
            }), 200

        # Используем функцию из shotstack_service
        # Для этого эндпоинта всегда подразумеваем, что видео не объединяются
        render_id, message = shotstack_service.initiate_shotstack_render(
            cloudinary_video_url_or_urls=task.cloudinary_url, # Одиночный URL
            video_metadata=task.video_metadata or {}, # Одиночный словарь метаданных
            original_filename=task.original_filename,
            instagram_username=task.instagram_username,
            email=task.email,
            linkedin_profile=task.linkedin_profile,
            connect_videos=False
        )

        if render_id:
            logger.info(f"[SHOTSTACK] Shotstack render initiated for {task_id}. Render ID: {render_id}")
            # Обновляем статус задачи в БД
            task.status = 'shotstack_pending'
            task.message = f"Shotstack render initiated with ID: {render_id}"
            task.shotstackRenderId = render_id
            session.commit()
            return jsonify({
                "message": "Shotstack render initiated successfully.",
                "shotstackRenderId": render_id
            }), 200
        else:
            logger.error(f"[SHOTSTACK] Shotstack API did not return a render ID for task {task_id}. Unexpected. Message: {message}")
            return jsonify({"error": "Failed to get Shotstack render ID. (Service issue)", "details": message}), 500

    except ValueError as e:
        session.rollback()
        logger.error(f"[SHOTSTACK] Validation Error during Shotstack generation for task {task_id}: {e}")
        return jsonify({"error": str(e)}), 400
    except requests.exceptions.RequestException as err:
        session.rollback()
        logger.exception(f"[SHOTSTACK] Network/API Error during Shotstack initiation for task {task_id}:")
        return jsonify({"error": f"Error communicating with Shotstack API: {err}", "details": str(err)}), 500
    except Exception as e:
        session.rollback()
        logger.exception(f"[SHOTSTACK] An unexpected error occurred during Shotstack generation for task {task_id}:")
        return jsonify({"error": "An unexpected server error occurred.", "details": str(e)}), 500
    finally:
        session.close()

@app.route('/process_videos', methods=['POST'])
def process_videos():
    session = Session()
    try:
        data = request.json
        task_ids = data.get('task_ids', []) # Теперь ожидаем 'task_ids'
        connect_videos = data.get('connect_videos', False)
        instagram_username = data.get('instagram_username')
        email = data.get('email')
        linkedin_profile = data.get('linkedin_profile')

        logger.info(f"[PROCESS_VIDEOS] Received request. Task IDs: {task_ids}, Connect Videos: {connect_videos}")

        if not task_ids:
            logger.warning("[PROCESS_VIDEOS] No task IDs provided.")
            return jsonify({"error": "No task IDs provided"}), 400

        valid_tasks = []
        for tid in task_ids:
            task = session.query(Task).filter_by(task_id=tid).first()
            # Убеждаемся, что видео существует, загружено на Cloudinary и имеет метаданные
            if task and task.cloudinary_url and task.video_metadata and task.status == 'completed':
                valid_tasks.append(task)
            else:
                logger.warning(f"[PROCESS_VIDEOS] Skipping task {tid}: not found, missing Cloudinary URL/metadata, or status not 'completed'.")

        if not valid_tasks:
            logger.warning("[PROCESS_VIDEOS] No valid tasks found for provided IDs or Cloudinary URLs/metadata missing or not completed.")
            return jsonify({"error": "No valid tasks found for processing (missing or invalid data). Please ensure videos are uploaded and have full metadata."}), 404

        render_id = None
        message = ""
        concatenated_task_id = None # Для нового ID объединенного видео

        if connect_videos and len(valid_tasks) >= 2:
            logger.info(f"[PROCESS_VIDEOS] Initiating concatenation for {len(valid_tasks)} videos.")
            
            cloudinary_video_urls = [t.cloudinary_url for t in valid_tasks]
            all_tasks_metadata = [t.video_metadata for t in valid_tasks]

            # Создаем уникальное имя файла для объединенного видео
            combined_filename_base = "_".join([t.original_filename.split('.')[0] for t in valid_tasks[:3]]) # Берем первые 3 имени
            combined_filename = f"Combined_{combined_filename_base}_{hashlib.md5(str(time.time()).encode()).hexdigest()[:8]}.mp4"
            
            render_id, message = shotstack_service.initiate_shotstack_render(
                cloudinary_video_url_or_urls=cloudinary_video_urls,
                video_metadata=all_tasks_metadata,
                original_filename=combined_filename, # Передаем уникальное имя для объединенного видео
                instagram_username=instagram_username,
                email=email,
                linkedin_profile=linkedin_profile,
                connect_videos=True
            )

            if render_id:
                # Генерируем уникальный task_id для объединенного видео, чтобы оно появилось как новый "пузырек" на фронтенде
                concatenated_task_id = f"concatenated_video_{render_id}" 
                new_concatenated_task = Task(
                    task_id=concatenated_task_id,
                    instagram_username=instagram_username, # Привязываем к текущему пользователю
                    email=email,
                    linkedin_profile=linkedin_profile,
                    original_filename=combined_filename,
                    status='concatenated_pending', # Новый статус
                    timestamp=datetime.now(),
                    cloudinary_url=None, # Cloudinary URL будет, когда Shotstack завершит и мы его получим
                    video_metadata={
                        "combined_from_tasks": [t.task_id for t in valid_tasks],
                        "total_duration": sum(m.get('duration', 0) for m in all_tasks_metadata if m)
                    }, # Метаданные для объединенного видео
                    message=f"Concatenated video render initiated with ID: {render_id}",
                    shotstackRenderId=render_id,
                    shotstackUrl=None,
                    posterUrl=None # Будет обновлен при получении статуса
                )
                session.add(new_concatenated_task)
                session.commit()
                logger.info(f"[PROCESS_VIDEOS] Shotstack render initiated for connected videos. New Task ID: {concatenated_task_id}, Render ID: {render_id}")
            else:
                session.rollback()
                logger.error(f"[PROCESS_VIDEOS] Shotstack API did not return a render ID for connected videos. Unexpected.")
                return jsonify({"error": "Failed to get Shotstack render ID for concatenated video. (Service issue)"}), 500

        else: # Сценарий: индивидуальная обработка видео (даже если выбрано одно)
            # В этом случае, мы берем каждое выбранное видео и запускаем для него генерацию Shotstack
            # Мы ожидаем, что фронтенд обрабатывает это как несколько отдельных задач
            # и обновляет их статусы.
            # Если фронтенд передает 1 видео и connect_videos = False, это будет "переобработка"
            # Если фронтенд передает >1 видео и connect_videos = False, это будет
            # запуск Shotstack для каждого из них по отдельности.
            
            # Обновляем статусы всех выбранных видео до 'shotstack_pending'
            # и инициируем рендеринг для каждого.
            initiated_tasks_info = []
            for task in valid_tasks:
                if task.status != 'completed': # Пропускаем, если видео уже не в состоянии "completed"
                    logger.warning(f"[PROCESS_VIDEOS] Skipping individual processing for task {task.task_id}: not in 'completed' status.")
                    continue

                if task.shotstackRenderId and task.status == 'shotstack_pending':
                    logger.info(f"[PROCESS_VIDEOS] Task {task.task_id} already processing with Shotstack ID {task.shotstackRenderId}. Skipping re-initiation.")
                    initiated_tasks_info.append({
                        "taskId": task.task_id,
                        "shotstackRenderId": task.shotstackRenderId,
                        "message": "Already processing."
                    })
                    continue

                try:
                    render_id_single, message_single = shotstack_service.initiate_shotstack_render(
                        cloudinary_video_url_or_urls=task.cloudinary_url,
                        video_metadata=task.video_metadata or {},
                        original_filename=task.original_filename,
                        instagram_username=instagram_username,
                        email=email,
                        linkedin_profile=linkedin_profile,
                        connect_videos=False
                    )

                    if render_id_single:
                        task.shotstackRenderId = render_id_single
                        task.status = 'shotstack_pending'
                        task.message = f"Shotstack render initiated with ID: {render_id_single}"
                        session.add(task) # Добавляем для обновления
                        logger.info(f"[PROCESS_VIDEOS] Shotstack render initiated for single video {task.original_filename}. Render ID: {render_id_single}")
                        initiated_tasks_info.append({
                            "taskId": task.task_id,
                            "shotstackRenderId": render_id_single,
                            "message": message_single
                        })
                    else:
                        logger.error(f"[PROCESS_VIDEOS] Shotstack API did not return a render ID for single video {task.task_id}. Unexpected.")
                        initiated_tasks_info.append({
                            "taskId": task.task_id,
                            "error": "Failed to get Shotstack render ID for single video."
                        })
                except Exception as e:
                    logger.exception(f"[PROCESS_VIDEOS] Error initiating Shotstack for task {task.task_id}:")
                    initiated_tasks_info.append({
                        "taskId": task.task_id,
                        "error": str(e)
                    })
            
            session.commit() # Коммитим все изменения статусов
            return jsonify({
                "message": "Individual video processing initiated.",
                "initiated_tasks": initiated_tasks_info
                # "concatenated_task_id": None # Явно указываем, что объединенного ID нет
            }), 200

        # Единый возврат для успешного объединения
        if connect_videos and concatenated_task_id:
            return jsonify({
                "message": message,
                "shotstackRenderId": render_id,
                "concatenated_task_id": concatenated_task_id # Отправляем новый ID на фронтенд
            }), 200
        elif connect_videos and not concatenated_task_id:
             # Если сюда дошли, значит, что-то пошло не так в логике объединения, но без исключения
             logger.error("[PROCESS_VIDEOS] Logic error: connect_videos is True but concatenated_task_id is None.")
             return jsonify({"error": "Failed to initiate concatenation due to an internal logic error."}), 500


    except SQLAlchemyError as e:
        session.rollback()
        logger.exception(f"[PROCESS_VIDEOS] Database error:")
        return jsonify({"error": "Database error", "details": str(e)}), 500
    except requests.exceptions.RequestException as err:
        session.rollback()
        logger.exception(f"[PROCESS_VIDEOS] Network/API Error during Shotstack initiation:")
        return jsonify({"error": f"Error communicating with Shotstack API: {err}", "details": str(err)}), 500
    except Exception as e:
        session.rollback()
        logger.exception(f"[PROCESS_VIDEOS] An unexpected error occurred during video processing:")
        return jsonify({"error": "An unexpected server error occurred.", "details": str(e)}), 500
    finally:
        session.close()

@app.route('/heavy-tasks/pending', methods=['GET'])
def get_heavy_tasks():
    logger.info("[HEAVY_TASKS] Request for heavy tasks received.")
    return jsonify({"message": "No heavy tasks pending for local worker yet."}), 200

# --- NEW: Endpoint to fetch user videos by identifier ---
@app.route('/user-videos', methods=['GET'])
def get_user_videos():
    session = Session()
    try:
        instagram_username = request.args.get('instagram_username')
        email = request.args.get('email')
        linkedin_profile = request.args.get('linkedin_profile')

        logger.info(f"[USER_VIDEOS] Request received for: Instagram='{instagram_username}', Email='{email}', LinkedIn='{linkedin_profile}'")

        if not any([instagram_username, email, linkedin_profile]):
            logger.warning("[USER_VIDEOS] No identifier provided for fetching user videos.")
            return jsonify({"error": "Please provide an Instagram username, email, or LinkedIn profile to fetch videos."}), 400

        query = session.query(Task)
        if instagram_username:
            query = query.filter(Task.instagram_username == instagram_username)
        if email:
            query = query.filter(Task.email == email)
        if linkedin_profile:
            query = query.filter(Task.linkedin_profile == linkedin_profile)

        # Filter only videos that are completed, processing, or uploaded (not failed or
        # those awaiting initial upload to Cloudinary which are temporary states not meant for display)
        query = query.filter(Task.status.in_(['completed', 'processing', 'uploaded', 'shotstack_pending', 'concatenated_pending', 'concatenated_completed']))

        # Order by timestamp to get most recent first
        tasks = query.order_by(Task.timestamp.desc()).all()

        if not tasks:
            logger.info(f"[USER_VIDEOS] No videos found for provided identifiers.")
            return jsonify([]), 200 # Return an empty array if no tasks found

        video_list = [task.to_dict() for task in tasks]
        logger.info(f"[USER_VIDEOS] Found {len(video_list)} videos for provided identifiers.")
        return jsonify(video_list), 200

    except SQLAlchemyError as e:
        logger.exception(f"[USER_VIDEOS] Database error fetching user videos:")
        return jsonify({"error": "Database error", "details": str(e)}), 500
    except Exception as e:
        logger.exception(f"[USER_VIDEOS] An unexpected error occurred fetching user videos:")
        return jsonify({"error": "An unexpected server error occurred", "details": str(e)}), 500
    finally:
        session.close()

if __name__ == '__main__':
    from waitress import serve
    port = int(os.environ.get('PORT', 8080))
    serve(app, host='0.0.0.0', port=port)
