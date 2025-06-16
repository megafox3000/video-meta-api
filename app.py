# app.py
import os
import cloudinary # Добавлен прямой импорт Cloudinary
import cloudinary.uploader # Добавлен прямой импорт Cloudinary uploader
import cloudinary.api # Добавлен прямой импорт Cloudinary api
from flask import Flask, request, jsonify, redirect, url_for
from flask_cors import CORS
from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, JSON
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime
import hashlib # Re-added for task_id generation
import time # Re-added for task_id generation
import requests
import json

# Импорт сервисов (cloudinary_service удален, shotstack_service остался)
from import shotstack_service 

# Инициализация Flask приложения
app = Flask(__name__) # Используем стандартное имя для Flask
CORS(app) 

# Конфигурация Cloudinary (возвращена в app.py)
cloudinary.config(
    cloud_name = os.environ.get('CLOUDINARY_CLOUD_NAME'),
    api_key = os.environ.get('CLOUDINARY_API_KEY'),
    api_secret = os.environ.get('CLOUDINARY_API_SECRET'),
    secure = True
)

# Конфигурация базы данных
DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    print("DATABASE_URL environment variable not set. Using SQLite for local development.")
    DATABASE_URL = "sqlite:///app_data.db"

connect_args = {}
# Для PostgreSQL на Render.com
if DATABASE_URL.startswith("postgresql://") or DATABASE_URL.startswith("postgres://"):
    # SQLAlchemy ожидает postgresql://, а не postgres://
    if DATABASE_URL.startswith("postgres://"):
        DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
    if "sslmode=" not in DATABASE_URL:
        connect_args["sslmode"] = "require"
    # Ensure SSL is correctly handled for Heroku/Render connections
    # connect_args["ssl_require"] = True # Эта строка может вызвать проблемы, если не настроен SSL сертификат.
                                      # Render/Heroku обычно обрабатывают это автоматически с sslmode=require.


engine = create_engine(DATABASE_URL, connect_args=connect_args)

Base = declarative_base()
Session = sessionmaker(bind=engine)

# Определение модели задачи
class Task(Base):
    __tablename__ = 'tasks'

    id = Column(Integer, primary_key=True)
    task_id = Column(String(255), unique=True, nullable=False)
    instagram_username = Column(String(255)) 
    email = Column(String(255))
    linkedin_profile = Column(String(255)) 
    original_filename = Column(String(255))
    status = Column(String(50))
    cloudinary_url = Column(String(500))
    video_metadata = Column(JSON)
    message = Column(Text)
    timestamp = Column(DateTime, default=datetime.now)
    # --- НОВЫЕ ПОЛЯ ДЛЯ SHOTSTACK ---
    shotstackRenderId = Column(String(255)) # ID, который Shotstack возвращает после запуска рендера
    shotstackUrl = Column(String(500))      # Итоговый URL сгенерированного видео от Shotstack
    posterUrl = Column(String(500), nullable=True) # URL для poster image от Shotstack
    # --- КОНЕЦ НОВЫХ ПОЛЕЙ ---

    def __repr__(self):
        return f"<Task(task_id='{self.task_id}', status='{self.status}')>"

    def to_dict(self):
        return {
            "id": self.id,
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
            "shotstackRenderId": self.shotstackRenderId,
            "shotstackUrl": self.shotstackUrl,
            "posterUrl": self.posterUrl
        }

def create_tables():
    Base.metadata.create_all(engine)
    print("Database tables created or already exist.")

create_tables()

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
            print("[UPLOAD] Instagram username is empty or invalid after cleaning. Using 'anonymous'.")
            cleaned_username = 'anonymous' # Fallback for task_id prefix

        original_filename_base = os.path.splitext(filename)[0]
        # Используем хэш для уникальности task_id, чтобы избежать конфликтов при повторной загрузке
        unique_hash = hashlib.md5(f"{cleaned_username}/{filename}/{datetime.now().timestamp()}".encode()).hexdigest()
        task_id = f"{cleaned_username}/{original_filename_base}_{unique_hash}" # Новый, уникальный task_id

        print(f"[{task_id}] Received upload request for file: '{filename}'")
        print(f"[{task_id}] User data: Instagram='{instagram_username}', Email='{email}', LinkedIn='{linkedin_profile}'")

        # Загрузка видео в Cloudinary (логика возвращена в app.py)
        print(f"[{task_id}] Uploading video to Cloudinary...")
        upload_result = cloudinary.uploader.upload(
            file,
            resource_type="video",
            folder=f"hife_video_analysis/{cleaned_username}",
            public_id=f"{original_filename_base}_{unique_hash}", # Используем уникальный public_id в Cloudinary
            unique_filename=False, # public_id уже уникален
            overwrite=True, # В случае перезаливки (например, если файл уже был загружен с таким же хешем)
            quality="auto",
            format="mp4",
            tags=["hife_analysis", cleaned_username]
        )
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
                # Используем функцию из shotstack_service для получения статуса рендера
                # Убедитесь, что get_shotstack_render_status возвращает поле 'poster'
                status_info = shotstack_service.get_shotstack_render_status(task_info.shotstackRenderId)

                shotstack_status = status_info['status']
                shotstack_url = status_info['url']
                shotstack_poster_url = status_info.get('poster')  # <--- Получаем URL постера
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
                    task_info.posterUrl = shotstack_poster_url  # <--- Сохраняем URL постера
                    session.commit()
                    print(f"[STATUS] Shotstack render completed for {task_id}. URL: {shotstack_url}")
                    if shotstack_poster_url: # Логируем, если URL постера был найден
                        print(f"[STATUS] Shotstack Poster URL: {shotstack_poster_url}")
                elif shotstack_status in ['failed', 'error', 'failed_due_to_timeout']: # Добавлены возможные статусы ошибок
                    if task_id.startswith('concatenated_video_'):
                        task_info.status = 'concatenated_failed'
                        task_info.message = f"Concatenated video rendering failed: {shotstack_error_message or 'Unknown Shotstack error'}"
                    else:
                        task_info.status = 'failed'
                        task_info.message = f"Shotstack rendering failed: {shotstack_error_message or 'Unknown Shotstack error'}"
                    session.commit()
                    print(f"[STATUS] Shotstack render failed for {task_id}. Error: {task_info.message}")
                else:
                    # Рендеринг еще в процессе, ничего не меняем в task_info.status, только сообщение
                    task_info.message = f"Shotstack render in progress: {shotstack_status}"
                    print(f"[STATUS] Shotstack render still in progress for {task_id}. Status: {shotstack_status}")
                
                response_data = task_info.to_dict()
                response_data['status'] = task_info.status # Убедимся, что возвращаемый статус актуален
                response_data['posterUrl'] = shotstack_poster_url # <--- Включаем posterUrl в ответ
                return jsonify(response_data), 200

            except requests.exceptions.RequestException as e:
                print(f"[STATUS] Error querying Shotstack API for {task_info.shotstackRenderId}: {e}")
                task_info.message = f"Error checking Shotstack status: {e}"
                # НЕ делаем session.rollback() здесь, так как мы только пытались обновить сообщение, а не данные
                # Используем shotstack_poster_url если он был получен до ошибки
                response_data = task_info.to_dict()
                response_data['posterUrl'] = shotstack_poster_url if 'shotstack_poster_url' in locals() else None
                return jsonify(response_data), 200 # Возвращаем текущее состояние из БД, чтобы фронтенд мог обновить сообщение
            except Exception as e:
                print(f"[STATUS] Unexpected error during Shotstack status check for {task_info.shotstackRenderId}: {e}")
                task_info.message = f"Unexpected error during Shotstack status check: {e}"
                # Используем shotstack_poster_url если он был получен до ошибки
                response_data = task_info.to_dict()
                response_data['posterUrl'] = shotstack_poster_url if 'shotstack_poster_url' in locals() else None
                return jsonify(response_data), 200 # Возвращаем текущее состояние из БД


        print(f"[STATUS] Task found in DB: {task_info.task_id}, current_status: {task_info.status}")
        # Если сюда дошли, значит, Shotstack API не опрашивался, и posterUrl должен быть None из to_dict()
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
        task_ids = data.get('task_ids', [])
        connect_videos = data.get('connect_videos', False)
        
        print(f"[PROCESS_VIDEOS] Received request. Task IDs: {task_ids}, Connect Videos: {connect_videos}")

        if not task_ids:
            print("[PROCESS_VIDEOS] No task IDs provided.")
            return jsonify({"error": "No task IDs provided"}), 400

        valid_tasks = []
        for tid in task_ids:
            task = session.query(Task).filter_by(task_id=tid).first()
            if task and task.cloudinary_url and task.video_metadata and task.status == 'completed':
                valid_tasks.append(task)
            else:
                print(f"[PROCESS_VIDEOS] Skipping task {tid}: not found, missing Cloudinary URL/metadata, or status not 'completed'.")

        if not valid_tasks:
            print("[PROCESS_VIDEOS] No valid tasks found for provided IDs or Cloudinary URLs/metadata missing or not completed.")
            return jsonify({"error": "No valid tasks found for processing (missing or invalid data). Please ensure videos are uploaded and have full metadata."}), 404

        # Extract user info from the first valid task
        # Используем старые имена полей
        instagram_username = valid_tasks[0].instagram_username
        email = valid_tasks[0].email
        linkedin_profile = valid_tasks[0].linkedin_profile

        render_id = None
        message = ""
        concatenated_task_id = None

        if connect_videos and len(valid_tasks) >= 2:
            print(f"[PROCESS_VIDEOS] Initiating concatenation for {len(valid_tasks)} videos.")

            cloudinary_video_urls = [t.cloudinary_url for t in valid_tasks]
            all_tasks_metadata = [t.video_metadata for t in valid_tasks]

            # Create a unique filename for the combined video
            combined_filename_base = "_".join([os.path.splitext(t.original_filename)[0] for t in valid_tasks[:3]])
            # Используем hashlib для генерации хэша
            combined_filename = f"Combined_{combined_filename_base}_{hashlib.md5(str(time.time()).encode()).hexdigest()[:8]}.mp4"

            # shotstack_service.initiate_shotstack_render теперь возвращает render_id, message и poster_url
            render_id, message, poster_url = shotstack_service.initiate_shotstack_render(
                cloudinary_video_url_or_urls=cloudinary_video_urls,
                video_metadata=all_tasks_metadata,
                original_filename=combined_filename,
                instagram_username=instagram_username,
                email=email,
                linkedin_profile=linkedin_profile,
                connect_videos=True
            )

            if render_id:
                # Generate a unique task_id for the concatenated video using hashlib
                concatenated_task_id = f"concatenated_video_{hashlib.md5(render_id.encode()).hexdigest()}"
                new_concatenated_task = Task(
                    task_id=concatenated_task_id,
                    instagram_username=instagram_username,
                    email=email,
                    linkedin_profile=linkedin_profile,
                    original_filename=combined_filename, # Use the generated combined filename
                    status='concatenated_pending',
                    timestamp=datetime.now(),
                    cloudinary_url=None, # Cloudinary URL will be set once Shotstack finishes
                    video_metadata={
                        "combined_from_tasks": [t.task_id for t in valid_tasks],
                        "total_duration": sum(m.get('duration', 0) for m in all_tasks_metadata if m)
                    },
                    message=f"Concatenated video render initiated with ID: {render_id}",
                    shotstackRenderId=render_id,
                    shotstackUrl=None,
                    posterUrl=poster_url # Сохраняем posterUrl для объединенного видео
                )
                session.add(new_concatenated_task)
                session.commit()
                print(f"[PROCESS_VIDEOS] Shotstack render initiated for connected videos. New Task ID: {concatenated_task_id}, Render ID: {render_id}")
            else:
                session.rollback()
                print(f"[PROCESS_VIDEOS] Shotstack API did not return a render ID for connected videos. Unexpected. Message: {message}")
                return jsonify({"error": f"Failed to get Shotstack render ID for concatenated video. (Service issue): {message}"}), 500

        else: # Scenario: individual video processing (even if only one is selected)
            # In this scenario, we take each selected video and initiate Shotstack generation for it
            # We expect the frontend to handle this as several separate tasks
            # and update their statuses.
            # If the frontend sends 1 video and connect_videos = False, it will be "re-processing"
            # If the frontend sends >1 video and connect_videos = False, it will be
            # initiating Shotstack for each of them separately.
            
            initiated_tasks_info = []
            for task in valid_tasks:
                if task.status != 'completed': # Skip if video is not in "completed" status
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
                    # initiate_shotstack_render now returns render_id, message, and poster_url
                    render_id_single, message_single, poster_url_single = shotstack_service.initiate_shotstack_render(
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
                        task.posterUrl = poster_url_single # Save posterUrl for individual video
                        session.add(task) # Add for update
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
            
            session.commit() # Commit all status changes
            return jsonify({
                "message": "Individual video processing initiated.",
                "initiated_tasks": initiated_tasks_info
            }), 200

        # Unified return for successful concatenation
        if connect_videos and concatenated_task_id:
            return jsonify({
                "message": message,
                "shotstackRenderId": render_id,
                "concatenated_task_id": concatenated_task_id
            }), 200
        elif connect_videos and not concatenated_task_id:
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
                # Используем функцию из shotstack_service для получения статуса рендера
                status_info = shotstack_service.get_shotstack_render_status(task_info.shotstackRenderId)

                shotstack_status = status_info['status']
                shotstack_url = status_info['url']
                shotstack_poster_url = status_info.get('poster')  # <--- Получаем URL постера
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
                    task_info.posterUrl = shotstack_poster_url  # <--- Сохраняем URL постера
                    session.commit()
                    print(f"[STATUS] Shotstack render completed for {task_id}. URL: {shotstack_url}")
                    if shotstack_poster_url: # Логируем, если URL постера был найден
                        print(f"[STATUS] Shotstack Poster URL: {shotstack_poster_url}")
                elif shotstack_status in ['failed', 'error', 'failed_due_to_timeout']: # Добавлены возможные статусы ошибок
                    if task_id.startswith('concatenated_video_'):
                        task_info.status = 'concatenated_failed'
                        task_info.message = f"Concatenated video rendering failed: {shotstack_error_message or 'Unknown Shotstack error'}"
                    else:
                        task_info.status = 'failed'
                        task_info.message = f"Shotstack rendering failed: {shotstack_error_message or 'Unknown Shotstack error'}"
                    session.commit()
                    print(f"[STATUS] Shotstack render failed for {task_id}. Error: {task_info.message}")
                else:
                    # Рендеринг еще в процессе, ничего не меняем в task_info.status, только сообщение
                    task_info.message = f"Shotstack render in progress: {shotstack_status}"
                    print(f"[STATUS] Shotstack render still in progress for {task_id}. Status: {shotstack_status}")
                
                response_data = task_info.to_dict()
                response_data['status'] = task_info.status # Убедимся, что возвращаемый статус актуален
                response_data['posterUrl'] = shotstack_poster_url # <--- Включаем posterUrl в ответ
                return jsonify(response_data), 200

            except requests.exceptions.RequestException as e:
                print(f"[STATUS] Error querying Shotstack API for {task_info.shotstackRenderId}: {e}")
                task_info.message = f"Error checking Shotstack status: {e}"
                # НЕ делаем session.rollback() здесь, так как мы только пытались обновить сообщение, а не данные
                # Используем shotstack_poster_url если он был получен до ошибки
                response_data = task_info.to_dict()
                response_data['posterUrl'] = shotstack_poster_url if 'shotstack_poster_url' in locals() else None
                return jsonify(response_data), 200 # Возвращаем текущее состояние из БД, чтобы фронтенд мог обновить сообщение
            except Exception as e:
                print(f"[STATUS] Unexpected error during Shotstack status check for {task_info.shotstackRenderId}: {e}")
                task_info.message = f"Unexpected error during Shotstack status check: {e}"
                # Используем shotstack_poster_url если он был получен до ошибки
                response_data = task_info.to_dict()
                response_data['posterUrl'] = shotstack_poster_url if 'shotstack_poster_url' in locals() else None
                return jsonify(response_data), 200 # Возвращаем текущее состояние из БД


        print(f"[STATUS] Task found in DB: {task_info.task_id}, current_status: {task_info.status}")
        # Если сюда дошли, значит, Shotstack API не опрашивался, и posterUrl должен быть None из to_dict()
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

@app.route('/heavy-tasks/pending', methods=['GET'])
def get_heavy_tasks():
    print("[HEAVY_TASKS] Request for heavy tasks received.")
    return jsonify({"message": "No heavy tasks pending for local worker yet."}), 200

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
