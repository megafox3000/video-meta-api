# app.py
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
import json # Добавлен импорт json
import uuid # НОВОЕ: Импортируем uuid
import shotstack_service # Импортируем наш новый сервис Shotstack
import cloudinary_service # НОВОЕ: Импортируем наш новый сервис Cloudinary

app = Flask(__name__)
CORS(app)

# Конфигурация Cloudinary (перенесено в cloudinary_service)
# cloudinary.config(
#     cloud_name = os.environ.get('CLOUDINARY_CLOUD_NAME'),
#     api_key = os.environ.get('CLOUDINARY_API_KEY'),
#     api_secret = os.environ.get('CLOUDINARY_API_SECRET'),
#     secure = True
# )

# Конфигурация базы данных
DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL environment variable is not set!")

connect_args = {}
# Для PostgreSQL на Render.com
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
# Если вы используете SSL, убедитесь, что sslmode=require присутствует.
# Render.com обычно требует этого.
# Если вы работаете локально с SQLite, это условие не будет выполняться.
if "sslmode=" not in DATABASE_URL and (DATABASE_URL.startswith("postgresql://")):
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
    posterUrl = Column(String(500), nullable=True) # Постер - РАСКОММЕНТИРОВАНО

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
            "posterUrl": self.posterUrl # РАСКОММЕНТИРОВАНО
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

        # НОВОЕ: Используем 'instagram', 'email', 'linkedin' из формы
        instagram_username = request.form.get('instagram')
        email = request.form.get('email')
        linkedin_profile = request.form.get('linkedin')

        cleaned_username = "".join(c for c in (instagram_username or '').strip() if c.isalnum() or c in ('_', '-')).strip()
        if not cleaned_username:
            print("[UPLOAD] Instagram username is empty or invalid after cleaning. Using 'anonymous'.")
            cleaned_username = 'anonymous' # Fallback for task_id prefix

        original_filename_base = os.path.splitext(filename)[0]
        # НОВОЕ: Используем UUID для создания уникального task_id
        unique_hash = str(uuid.uuid4().hex)
        task_id = f"{cleaned_username}/{original_filename_base}_{unique_hash}" # Новый, уникальный task_id

        print(f"[{task_id}] Received upload request for file: '{filename}'")
        print(f"[{task_id}] User data: Instagram='{instagram_username}', Email='{email}', LinkedIn='{linkedin_profile}'")

        # НОВОЕ: Используем cloudinary_service для загрузки
        print(f"[{task_id}] Uploading video to Cloudinary using service...")
        upload_result = cloudinary_service.upload_video(file, cleaned_username, public_id_suffix=unique_hash)
        print(f"[{task_id}] Cloudinary response after new upload: {upload_result}")

        if upload_result and upload_result.get('secure_url'):
            cloudinary_url = upload_result['secure_url']
            print(f"[{task_id}] Cloudinary URL: {cloudinary_url}")

            # --- ПРОВЕРКА DURATION И ДРУГИХ ПОЛЕЙ ПОСЛЕ ЗАГРУЗКИ ---
            new_upload_duration = upload_result.get('duration', 0)
            new_upload_width = upload_result.get('width', 0)
            new_upload_height = upload_result.get('height', 0)
            new_upload_bytes = upload_result.get('bytes', 0)

            if new_upload_duration <= 0 or new_upload_width <= 0 or new_upload_height <= 0 or new_upload_bytes <= 0:
                print(f"[{task_id}] CRITICAL WARNING: Video uploaded, but essential metadata (duration/resolution/size) is still 0 or missing from Cloudinary response. Full metadata: {upload_result}")
                session.rollback()  # Откатываем транзакцию
                return jsonify({
                    'error': 'Video uploaded but could not retrieve complete and valid metadata from Cloudinary. Please try again or check video file.',
                    'taskId': task_id,
                    'cloudinary_url': cloudinary_url,
                    'metadata': upload_result # Возвращаем все, что получили
                }), 500
            # --- КОНЕЦ ПРОВЕРКИ DURATION И ДРУГИХ ПОЛЕЙ ПОСЛЕ ЗАГРУЗКИ ---

            new_task = Task(
                task_id=task_id, # Уникальный task_id
                instagram_username=instagram_username,
                email=email,
                linkedin_profile=linkedin_profile,
                original_filename=filename,
                status='completed', # Предполагаем, что после загрузки на Cloudinary видео готово к дальнейшей обработке
                timestamp=datetime.now(),
                cloudinary_url=cloudinary_url,
                video_metadata=upload_result,
                message='Video successfully uploaded to Cloudinary and full metadata obtained.',
                shotstackRenderId=None, # Инициализируем как None
                shotstackUrl=None,     # Инициализируем как None
                posterUrl=None         # Инициализируем как None
            )
            session.add(new_task)
            session.commit()
            print(f"[{task_id}] New task successfully created and committed to DB.")
            return jsonify({
                'message': 'Video uploaded and task created.',
                'taskId': new_task.task_id, # Возвращаем новый уникальный task_id
                'cloudinary_url': cloudinary_url,
                'metadata': new_task.video_metadata,
                'originalFilename': new_task.original_filename,
                'status': new_task.status
            }), 200
        else:
            print(f"[{task_id}] Cloudinary upload failed: secure_url missing in response.")
            return jsonify({'error': 'Cloudinary upload failed'}), 500

    except SQLAlchemyError as e:
        session.rollback()
        print(f"[{task_id if 'task_id' in locals() else 'N/A'}] Database error during upload: {e}")
        return jsonify({'error': 'Database error', 'details': str(e)}), 500
    except Exception as e:
        session.rollback()
        print(f"[{task_id if 'task_id' in locals() else 'N/A'}] An unexpected error occurred during upload: {e}")
        return jsonify({'error': 'An unexpected error occurred', 'details': str(e)}), 500
    finally:
        session.close()

@app.route('/task-status/<path:task_id>', methods=['GET'])
def get_task_status(task_id):
    session = Session()
    try:
        print(f"\n[STATUS] Received status request for task_id: '{task_id}'")
        task_info = session.query(Task).filter_by(task_id=task_id).first()

        if not task_info:
            print(f"[STATUS] Task with task_id '{task_id}' NOT FOUND in DB.")
            return jsonify({"message": "Task not found."}), 404

        # Проверяем, является ли задача связанной с Shotstack рендерингом и еще не завершена
        if task_info.shotstackRenderId and \
           task_info.status not in ['completed', 'failed', 'concatenated_completed', 'concatenated_failed']:
            print(f"[STATUS] Task {task_info.task_id} has Shotstack render ID. Checking Shotstack API...")
            try:
                status_info = shotstack_service.get_shotstack_render_status(task_info.shotstackRenderId)

                shotstack_status = status_info['status']
                shotstack_url = status_info['url']
                shotstack_poster_url = status_info.get('poster')  # <-- Эту строку оставляем!
                shotstack_error_message = status_info['error_message']

                print(f"[STATUS] Shotstack render status for {task_info.shotstackRenderId}: {shotstack_status}")

                if shotstack_status == 'done' and shotstack_url:
                    # Обновляем статус в нашей БД в зависимости от типа задачи
                    if task_id.startswith('concatenated_video_'):
                        task_info.status = 'concatenated_completed'
                        task_info.message = "Concatenated video rendered successfully."
                    else:
                        task_info.status = 'completed'
                        task_info.message = "Shotstack video rendered successfully."
                    task_info.shotstackUrl = shotstack_url
                    task_info.posterUrl = shotstack_poster_url  # <-- РАСКОММЕНТИРОВАНО
                    session.commit()
                    print(f"[STATUS] Shotstack render completed for {task_id}. URL: {shotstack_url}")
                    if shotstack_poster_url: # Логируем, если URL постера был найден
                        print(f"[STATUS] Shotstack Poster URL: {shotstack_poster_url}")
                elif shotstack_status in ['failed', 'error', 'failed_due_to_timeout']:
                    if task_id.startswith('concatenated_video_'):
                        task_info.status = 'concatenated_failed'
                        task_info.message = f"Concatenated video rendering failed: {shotstack_error_message or 'Unknown Shotstack error'}"
                    else:
                        task_info.status = 'failed'
                        task_info.message = f"Shotstack rendering failed: {shotstack_error_message or 'Unknown Shotstack error'}"
                    task_info.posterUrl = shotstack_poster_url # Сохраняем даже если ошибка
                    session.commit()
                    print(f"[STATUS] Shotstack render failed for {task_id}. Error: {task_info.message}")
                else:
                    task_info.message = f"Shotstack render in progress: {shotstack_status}"
                    print(f"[STATUS] Shotstack render still in progress for {task_id}. Status: {shotstack_status}")
                
                response_data = task_info.to_dict()
                response_data['status'] = task_info.status
                response_data['posterUrl'] = shotstack_poster_url # <-- Эту строку оставляем!
                return jsonify(response_data), 200

            except requests.exceptions.RequestException as e:
                print(f"[STATUS] Error querying Shotstack API for {task_info.shotstackRenderId}: {e}")
                task_info.message = f"Error checking Shotstack status: {e}"
                # В случае ошибки, возможно, posterUrl будет недоступен, но мы все равно хотим его отправить, если он есть
                response_data = task_info.to_dict()
                response_data['posterUrl'] = shotstack_poster_url if 'shotstack_poster_url' in locals() else None # Включаем posterUrl, если он был получен до ошибки
                return jsonify(response_data), 200
            except Exception as e:
                print(f"[STATUS] Unexpected error during Shotstack status check for {task_info.shotstackRenderId}: {e}")
                task_info.message = f"Unexpected error during Shotstack status check: {e}"
                response_data = task_info.to_dict()
                response_data['posterUrl'] = shotstack_poster_url if 'shotstack_poster_url' in locals() else None # Включаем posterUrl, если он был получен до ошибки
                return jsonify(response_data), 200


        print(f"[STATUS] Task found in DB: {task_info.task_id}, current_status: {task_info.status}")
        # Для случаев, когда task_info.shotstackRenderId нет (например, обычные загрузки),
        # posterUrl в to_dict() будет None, и это нормально.
        return jsonify(task_info.to_dict()), 200
    except SQLAlchemyError as e:
        session.rollback()
        print(f"[STATUS] Database error fetching task status: {e}")
        return jsonify({"error": "Database error", "details": str(e)}), 500
    except Exception as e:
        session.rollback()
        print(f"[STATUS] An unexpected error occurred in get_task_status: {e}")
        return jsonify({"error": "An unexpected error occurred", "details": str(e)}), 500
    finally:
        session.close()

@app.route('/generate-shotstack-video', methods=['POST'])
def generate_shotstack_video():
    session = Session()
    try:
        data = request.get_json()
        task_id = data.get('taskId')

        if not task_id:
            print("[SHOTSTACK] No taskId provided for Shotstack generation.")
            return jsonify({"error": "No taskId provided"}), 400

        task = session.query(Task).filter_by(task_id=task_id).first()
        if not task:
            print(f"[SHOTSTACK] Task {task_id} not found in DB.")
            return jsonify({"error": "Task not found."}), 404

        if not task.cloudinary_url:
            print(f"[SHOTSTACK] Task {task_id} has no Cloudinary URL. Cannot generate Shotstack video.")
            return jsonify({"error": "Video not uploaded to Cloudinary yet or URL missing."}), 400
        
        # Если видео уже в процессе рендеринга Shotstack, не запускаем повторно
        if task.status == 'shotstack_pending' and task.shotstackRenderId:
            print(f"[SHOTSTACK] Task {task_id} is already in 'shotstack_pending' status with render ID {task.shotstackRenderId}. Not re-initiating.")
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
            connect_videos=False # Явно указываем False
        )

        if render_id:
            print(f"[SHOTSTACK] Shotstack render initiated for {task_id}. Render ID: {render_id}")
            # Обновляем статус задачи в БД
            task.status = 'shotstack_pending'
            task.message = f"Shotstack render initiated with ID: {render_id}"
            task.shotstackRenderId = render_id
            task.posterUrl = None # Убедимся, что posterUrl сбрасывается при новом рендеринге
            session.commit()
            return jsonify({
                "message": "Shotstack render initiated successfully.",
                "shotstackRenderId": render_id
            }), 200
        else:
            print(f"[SHOTSTACK] Shotstack API did not return a render ID for task {task_id}. Unexpected.")
            return jsonify({"error": "Failed to get Shotstack render ID. (Service issue)"}), 500

    except ValueError as e:
        session.rollback()
        print(f"[SHOTSTACK] Validation Error during Shotstack generation for task {task_id}: {e}")
        return jsonify({"error": str(e)}), 400
    except requests.exceptions.RequestException as err:
        session.rollback()
        print(f"[SHOTSTACK] Network/API Error during Shotstack initiation for task {task_id}: {err}")
        return jsonify({"error": f"Error communicating with Shotstack API: {err}", "details": str(err)}), 500
    except Exception as e:
        session.rollback()
        print(f"[SHOTSTACK] An unexpected error occurred during Shotstack generation for task {task_id}: {e}")
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
        # НОВОЕ: Получаем данные пользователя из запроса (предполагая, что фронтенд их передает)
        instagram_username = data.get('instagram_username')
        email = data.get('email')
        linkedin_profile = data.get('linkedin_profile')


        print(f"[PROCESS_VIDEOS] Received request. Task IDs: {task_ids}, Connect Videos: {connect_videos}")

        if not task_ids:
            print("[PROCESS_VIDEOS] No task IDs provided.")
            return jsonify({"error": "No task IDs provided"}), 400

        valid_tasks = []
        for tid in task_ids:
            task = session.query(Task).filter_by(task_id=tid).first()
            # Убеждаемся, что видео существует, загружено на Cloudinary и имеет метаданные
            if task and task.cloudinary_url and task.video_metadata and task.status == 'completed':
                valid_tasks.append(task)
            else:
                print(f"[PROCESS_VIDEOS] Skipping task {tid}: not found, missing Cloudinary URL/metadata, or status not 'completed'.")

        if not valid_tasks:
            print("[PROCESS_VIDEOS] No valid tasks found for provided IDs or Cloudinary URLs/metadata missing or not completed.")
            return jsonify({"error": "No valid tasks found for processing (missing or invalid data). Please ensure videos are uploaded and have full metadata."}), 404

        render_id = None
        message = ""
        concatenated_task_id = None # Для нового ID объединенного видео

        if connect_videos and len(valid_tasks) >= 2:
            print(f"[PROCESS_VIDEOS] Initiating concatenation for {len(valid_tasks)} videos.")
            
            cloudinary_video_urls = [t.cloudinary_url for t in valid_tasks]
            all_tasks_metadata = [t.video_metadata for t in valid_tasks]

            # Создаем уникальное имя файла для объединенного видео
            combined_filename_base = "_".join([t.original_filename.split('.')[0] for t in valid_tasks[:3]]) # Берем первые 3 имени
            # НОВОЕ: Используем UUID для уникальности объединенного имени
            combined_filename = f"Combined_{combined_filename_base}_{uuid.uuid4().hex[:8]}.mp4"
            
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
                concatenated_task_id = f"concatenated_video_{render_id}"  # НОВОЕ: Используем render_id
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
                    posterUrl=None # НОВОЕ: Инициализируем posterUrl как None
                )
                session.add(new_concatenated_task)
                session.commit()
                print(f"[PROCESS_VIDEOS] Shotstack render initiated for connected videos. New Task ID: {concatenated_task_id}, Render ID: {render_id}")
            else:
                session.rollback()
                print(f"[PROCESS_VIDEOS] Shotstack API did not return a render ID for connected videos. Unexpected.")
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
                    print(f"[PROCESS_VIDEOS] Skipping individual processing for task {task.task_id}: not in 'completed' status.")
                    continue

                if task.shotstackRenderId and task.status == 'shotstack_pending':
                    print(f"[PROCESS_VIDEOS] Task {task.task_id} already processing with Shotstack ID {task.shotstackRenderId}. Skipping re-initiation.")
                    initiated_tasks_info.append({
                        "taskId": task.task_id,
                        "shotstackRenderId": task.shotstackRenderId,
                        "message": "Already processing."
                    })
                    continue

                try:
                    # НОВОЕ: Передаем connect_videos=False для индивидуальной обработки
                    render_id_single, message_single = shotstack_service.initiate_shotstack_render(
                        cloudinary_video_url_or_urls=task.cloudinary_url,
                        video_metadata=task.video_metadata or {},
                        original_filename=task.original_filename,
                        instagram_username=instagram_username,
                        email=email,
                        linkedin_profile=linkedin_profile,
                        connect_videos=False # Явно указываем False
                    )

                    if render_id_single:
                        task.shotstackRenderId = render_id_single
                        task.status = 'shotstack_pending'
                        task.message = f"Shotstack render initiated with ID: {render_id_single}"
                        task.posterUrl = None # НОВОЕ: Сбрасываем posterUrl при новом рендеринге
                        session.add(task) # Добавляем для обновления
                        print(f"[PROCESS_VIDEOS] Shotstack render initiated for single video {task.original_filename}. Render ID: {render_id_single}")
                        initiated_tasks_info.append({
                            "taskId": task.task_id,
                            "shotstackRenderId": render_id_single,
                            "message": message_single
                        })
                    else:
                        print(f"[PROCESS_VIDEOS] Shotstack API did not return a render ID for single video {task.task_id}. Unexpected.")
                        initiated_tasks_info.append({
                            "taskId": task.task_id,
                            "error": "Failed to get Shotstack render ID for single video."
                        })
                except Exception as e:
                    print(f"[PROCESS_VIDEOS] Error initiating Shotstack for task {task.task_id}: {e}")
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
              print("[PROCESS_VIDEOS] Logic error: connect_videos is True but concatenated_task_id is None.")
              return jsonify({"error": "Failed to initiate concatenation due to an internal logic error."}), 500


    except SQLAlchemyError as e:
        session.rollback()
        print(f"[PROCESS_VIDEOS] Database error: {e}")
        return jsonify({"error": "Database error", "details": str(e)}), 500
    except requests.exceptions.RequestException as err:
        session.rollback()
        print(f"[PROCESS_VIDEOS] Network/API Error during Shotstack initiation: {err}")
        return jsonify({"error": f"Error communicating with Shotstack API: {err}", "details": str(err)}), 500
    except Exception as e:
        session.rollback()
        print(f"[PROCESS_VIDEOS] An unexpected error occurred during video processing: {e}")
        return jsonify({"error": "An unexpected server error occurred.", "details": str(e)}), 500
    finally:
        session.close()

@app.route('/heavy-tasks/pending', methods=['GET'])
def get_heavy_tasks():
    print("[HEAVY_TASKS] Request for heavy tasks received.")
    return jsonify({"message": "No heavy tasks pending for local worker yet."}), 200

# ---
## Тестовые эндпоинты для отладки Shotstack
#
# ВНИМАНИЕ: Эти эндпоинты предназначены только для отладки.
# Удалите их из продакшн-кода после завершения тестирования!
# ---
# @app.route('/test-shotstack-simple', methods=['GET']) # Делаем GET-запрос для удобства вызова из браузера
# def test_shotstack_simple_connection():
#     print("[TEST_SHOTSTACK_SIMPLE] Received request to test simple Shotstack connection.")
#     try:
#         # Важно: используем API_KEY и URL напрямую из переменных окружения
#         # чтобы убедиться, что они доступны в контексте app.py
#         shotstack_api_key = os.environ.get('SHOTSTACK_API_KEY')
#         shotstack_render_url = "https://api.shotstack.io/stage/render"

#         if not shotstack_api_key:
#             print("[TEST_SHOTSTACK_SIMPLE] ERROR: SHOTSTACK_API_KEY is not set!")
#             return jsonify({"status": "error", "message": "SHOTSTACK_API_KEY environment variable is not set."}), 500

#         headers = {
#             "Content-Type": "application/json",
#             "x-api-key": shotstack_api_key
#         }

#         # Максимально простой JSON-запрос для Shotstack
#         test_payload = {
#             "timeline": {
#                 "tracks": [
#                     {
#                         "clips": [
#                             {
#                                 "asset": {
#                                     "type": "title",
#                                     "text": "Hello Shotstack!",
#                                     "style": "minimal",
#                                     "color": "#FF0000",
#                                     "size": "large"
#                                 },
#                                 "start": 0,
#                                 "length": 2 # Длительность 2 секунды
#                             }
#                         ]
#                     }
#                 ],
#                 "background": "#0000FF" # Синий фон
#             },
#             "output": {
#                 "format": "mp4",
#                 "resolution": "sd",
#                 "aspectRatio": "16:9"
#             }
#         }

#         print(f"[TEST_SHOTSTACK_SIMPLE] Sending simple payload: {json.dumps(test_payload, indent=2)}")

#         # Отправляем POST-запрос
#         test_response = requests.post(shotstack_render_url, json=test_payload, headers=headers, timeout=15)
#         test_response.raise_for_status() # Вызовет исключение для 4xx/5xx ошибок

#         shotstack_result = test_response.json()
#         render_id = shotstack_result.get('response', {}).get('id')

#         if render_id:
#             print(f"[TEST_SHOTSTACK_SIMPLE] Shotstack render initiated successfully. Render ID: {render_id}")
#             return jsonify({
#                 "status": "success",
#                 "message": "Simple Shotstack render initiated!",
#                 "renderId": render_id,
#                 "shotstackResponse": shotstack_result
#             }), 200
#         else:
#             print(f"[TEST_SHOTSTACK_SIMPLE] Failed to get Shotstack render ID. Full response: {json.dumps(shotstack_result, indent=2)}")
#             return jsonify({
#                 "status": "error",
#                 "message": "Failed to get Shotstack render ID.",
#                 "shotstackResponse": shotstack_result
#             }), 500

#     except requests.exceptions.HTTPError as e:
#         error_message = f"HTTP Error during Shotstack connection test: {e}"
#         response_text = e.response.text if e.response is not None else "No response text"
#         print(f"[TEST_SHOTSTACK_SIMPLE] {error_message}. Details: {response_text}")
#         return jsonify({
#             "status": "error",
#             "message": error_message,
#             "details": response_text
#         }), e.response.status_code if e.response is not None else 500
#     except requests.exceptions.ConnectionError as e:
#         print(f"[TEST_SHOTSTACK_SIMPLE] Connection Error to Shotstack: {e}")
#         return jsonify({"status": "error", "message": f"Connection Error to Shotstack: {e}"}), 500
#     except requests.exceptions.Timeout as e:
#         print(f"[TEST_SHOTSTACK_SIMPLE] Timeout connecting to Shotstack: {e}")
#         return jsonify({"status": "error", "message": f"Timeout connecting to Shotstack: {e}"}), 500
#     except Exception as e:
#         print(f"[TEST_SHOTSTACK_SIMPLE] An unexpected error occurred during Shotstack connection test: {e}")
#         return jsonify({"status": "error", "message": f"An unexpected error occurred: {e}"}), 500

# НОВЫЙ ЭНДПОИНТ: /user-videos
@app.route('/user-videos', methods=['GET'])
def get_user_videos():
    session = Session()
    try:
        # Получаем идентификаторы пользователя из параметров запроса
        instagram_username = request.args.get('instagram_username')
        email = request.args.get('email')
        linkedin_profile = request.args.get('linkedin_profile')

        print(f"\n[USER_VIDEOS] Received request for user videos:")
        print(f"[USER_VIDEOS] Instagram: '{instagram_username}', Email: '{email}', LinkedIn: '{linkedin_profile}'")

        query = session.query(Task)
        
        # Строим запрос на основе предоставленных идентификаторов
        if instagram_username:
            query = query.filter(Task.instagram_username == instagram_username)
        elif email:
            query = query.filter(Task.email == email)
        elif linkedin_profile:
            query = query.filter(Task.linkedin_profile == linkedin_profile)
        else:
            return jsonify({"error": "No user identifier (instagram_username, email, or linkedin_profile) provided"}), 400

        user_tasks = query.order_by(Task.timestamp.desc()).all()
        
        # Преобразуем объекты Task в словари
        tasks_data = [task.to_dict() for task in user_tasks]
        
        print(f"[USER_VIDEOS] Found {len(tasks_data)} videos for user.")
        return jsonify(tasks_data), 200

    except Exception as e:
        print(f"[USER_VIDEOS] An unexpected error occurred: {e}")
        return jsonify({"error": "An unexpected error occurred", "details": str(e)}), 500
    finally:
        session.close()

if __name__ == '__main__':
    from waitress import serve
    port = int(os.environ.get('PORT', 8080))
    serve(app, host='0.0.0.0', port=port)
