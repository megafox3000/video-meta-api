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
import logging # Added for better logging
import re
import uuid # For unique task IDs

# Import your Shotstack service (ensure this file exists and is correctly structured)
import shotstack_service 

# --- Configure Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)

# --- CORS Configuration ---
# This is crucial for handling CORS correctly, especially for preflight (OPTIONS) requests.
# It allows requests from any origin (*), specifically for GET, POST, and OPTIONS methods,
# and permits Content-Type and Authorization headers. supports_credentials=True
# is added in case you plan to use cookies or HTTP authentication.
CORS(app, resources={r"/*": {"origins": "*", "methods": ["GET", "POST", "OPTIONS", "HEAD"], "headers": ["Content-Type", "Authorization", "X-Requested-With"]}}, supports_credentials=True)


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
    shotstackUrl = Column(String)       # Итоговый URL сгенерированного видео от Shotstack
    # - posterUrl = Column(String(500), nullable=True) # Постер


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
            # - "posterUrl": self.posterUrl
        }

def create_tables():
    Base.metadata.create_all(engine)
    logger.info("Database tables created or already exist.")

create_tables()

# Request-scoped database session
@app.before_request
def before_request():
    """Establishes a database session before each request and stores it in g."""
    g.db_session = Session()

@app.teardown_request
def teardown_request(exception=None):
    """Closes the database session after each request."""
    session = g.pop('db_session', None)
    if session is not None:
        session.close()

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
        if "ISO6709" in key and re.match(r"^[\+\-]\d+(\.\d+)?[\+\-]\d+(\.\d+)?", str(value)):
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
        headers = {"User-Agent": "VideoMetaApp/1.0"} # Required by Nominatim
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
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
    session = g.db_session # Use session from g object
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
        user_id_from_frontend = request.form.get('userId', 'anonymous_user') # Get userId from frontend form data

        cleaned_username = "".join(c for c in (instagram_username or '').strip() if c.isalnum() or c in ('_', '-')).strip()
        if not cleaned_username:
            logger.warning("[UPLOAD] Instagram username is empty or invalid after cleaning. Using frontend userId.")
            cleaned_username = "".join(c for c in (user_id_from_frontend or '').strip() if c.isalnum() or c in ('_', '-')).strip()
            if not cleaned_username:
                logger.error("[UPLOAD] Both instagram_username and frontend userId are invalid. Cannot proceed.")
                return jsonify({"error": "A valid user identifier (Instagram username or userId) is required."}), 400

        original_filename_base = os.path.splitext(filename)[0]
        # Use UUID for task_id to ensure global uniqueness across all uploads, not just per username/filename
        task_id = str(uuid.uuid4()) 
        
        logger.info(f"[{task_id}] Received upload request for file: '{filename}'")
        logger.info(f"[{task_id}] User data: Instagram='{instagram_username}', Email='{email}', LinkedIn='{linkedin_profile}', Frontend UserID='{user_id_from_frontend}'")
        logger.info(f"[{task_id}] Cleaned username for folder: '{cleaned_username}'")

        # Initial status in DB as 'uploading_to_cloudinary'
        new_task = Task(
            task_id=task_id, # Unique task_id
            instagram_username=instagram_username,
            email=email,
            linkedin_profile=linkedin_profile,
            original_filename=filename,
            status='uploading_to_cloudinary', # New status for initial DB entry
            timestamp=datetime.now(),
            cloudinary_url=None, # Will be updated after upload
            video_metadata={},
            message='Video upload initiated to Cloudinary.'
        )
        session.add(new_task)
        session.commit()
        logger.info(f"[{task_id}] Initial task record created in DB.")


        logger.info(f"[{task_id}] Uploading video to Cloudinary...")
        upload_result = cloudinary.uploader.upload(
            file,
            resource_type="video",
            folder=f"hife_video_analysis/{cleaned_username}",
            public_id=f"{original_filename_base}_{task_id[:8]}", # Use first 8 chars of task_id for uniqueness
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
                new_task.status = 'cloudinary_metadata_incomplete'
                new_task.message = 'Video uploaded but could not retrieve complete and valid metadata from Cloudinary.'
                session.commit()
                return jsonify({
                    'error': 'Video uploaded but could not retrieve complete and valid metadata from Cloudinary. Please try again or check video file.',
                    'taskId': task_id,
                    'cloudinary_url': cloudinary_url,
                    'metadata': upload_result 
                }), 500
            # --- END OF DURATION AND OTHER FIELDS CHECK AFTER UPLOAD ---

            new_task.status = 'completed' # Update status after successful Cloudinary upload and metadata check
            new_task.message = 'Video successfully uploaded to Cloudinary and full metadata obtained.'
            new_task.cloudinary_url = cloudinary_url
            new_task.video_metadata = upload_result
            
            session.commit()
            logger.info(f"[{task_id}] Task successfully updated with Cloudinary details and committed to DB.")
            return jsonify({
                'message': 'Video uploaded and task created.',
                'taskId': new_task.task_id, 
                'cloudinary_url': cloudinary_url,
                'metadata': new_task.video_metadata,
                'originalFilename': new_task.original_filename,
                'status': new_task.status
            }), 200
        else:
            new_task.status = 'cloudinary_upload_failed'
            new_task.message = 'Cloudinary upload failed: secure_url missing in response.'
            session.commit()
            logger.error(f"[{task_id}] Cloudinary upload failed: secure_url missing in response. Response: {upload_result}")
            return jsonify({'error': 'Cloudinary upload failed'}), 500

    except SQLAlchemyError as e:
        session.rollback()
        logger.exception(f"[{task_id}] Database error during upload:")
        return jsonify({'error': 'Database error', 'details': str(e)}), 500
    except Exception as e:
        session.rollback()
        logger.exception(f"[{task_id}] An unexpected error occurred during upload:")
        return jsonify({'error': 'An unexpected error occurred', 'details': str(e)}), 500


@app.route('/task-status/<path:task_id>', methods=['GET'])
def get_task_status(task_id):
    session = g.db_session
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
                    # task_info.posterUrl = shotstack_poster_url # Keep this line commented or remove if column not in DB
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
                    session.commit()
                    logger.error(f"[STATUS] Shotstack render failed for {task_id}. Error: {task_info.message}")
                else:
                    task_info.message = f"Shotstack render in progress: {shotstack_status}"
                    logger.info(f"[STATUS] Shotstack render still in progress for {task_id}. Status: {shotstack_status}")
                
                response_data = task_info.to_dict()
                response_data['status'] = task_info.status
                response_data['posterUrl'] = shotstack_poster_url 
                return jsonify(response_data), 200

            except requests.exceptions.RequestException as e:
                logger.error(f"[STATUS] Error querying Shotstack API for {task_info.shotstackRenderId}: {e}")
                task_info.message = f"Error checking Shotstack status: {e}"
                response_data = task_info.to_dict()
                response_data['posterUrl'] = status_info.get('poster') if 'status_info' in locals() else None 
                return jsonify(response_data), 200
            except Exception as e:
                logger.exception(f"[STATUS] Unexpected error during Shotstack status check for {task_info.shotstackRenderId}:")
                task_info.message = f"Unexpected error during Shotstack status check: {e}"
                response_data = task_info.to_dict()
                response_data['posterUrl'] = status_info.get('poster') if 'status_info' in locals() else None
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
        return jsonify({"error": "An unexpected error occurred", "details": str(e)}), 500


@app.route('/generate-shotstack-video', methods=['POST'])
def generate_shotstack_video():
    session = g.db_session
    task_id = "N/A"
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
            
        if task.status == 'shotstack_pending' and task.shotstackRenderId:
            logger.info(f"[SHOTSTACK] Task {task_id} is already in 'shotstack_pending' status with render ID {task.shotstackRenderId}. Not re-initiating.")
            return jsonify({
                "message": "Shotstack render already initiated and in progress.",
                "shotstackRenderId": task.shotstackRenderId
            }), 200

        render_id, message = shotstack_service.initiate_shotstack_render(
            cloudinary_video_url_or_urls=task.cloudinary_url, 
            video_metadata=task.video_metadata or {}, 
            original_filename=task.original_filename,
            instagram_username=task.instagram_username,
            email=task.email,
            linkedin_profile=task.linkedin_profile,
            connect_videos=False 
        )

        if render_id:
            logger.info(f"[SHOTSTACK] Shotstack render initiated for {task_id}. Render ID: {render_id}")
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


@app.route('/process_videos', methods=['POST'])
def process_videos():
    session = g.db_session
    try:
        data = request.json
        task_ids = data.get('task_ids', []) 
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
            if task and task.cloudinary_url and task.video_metadata and task.status == 'completed':
                valid_tasks.append(task)
            else:
                logger.warning(f"[PROCESS_VIDEOS] Skipping task {tid}: not found, missing Cloudinary URL/metadata, or status not 'completed'.")

        if not valid_tasks:
            logger.warning("[PROCESS_VIDEOS] No valid tasks found for provided IDs or Cloudinary URLs/metadata missing or not completed.")
            return jsonify({"error": "No valid tasks found for processing (missing or invalid data). Please ensure videos are uploaded and have full metadata."}), 404

        render_id = None
        message = ""
        concatenated_task_id = None 

        if connect_videos and len(valid_tasks) >= 2:
            logger.info(f"[PROCESS_VIDEOS] Initiating concatenation for {len(valid_tasks)} videos.")
            
            cloudinary_video_urls = [t.cloudinary_url for t in valid_tasks]
            all_tasks_metadata = [t.video_metadata for t in valid_tasks]

            combined_filename_base = "_".join([os.path.splitext(t.original_filename)[0] for t in valid_tasks[:3]]) 
            combined_filename = f"Combined_{combined_filename_base}_{hashlib.md5(str(time.time()).encode()).hexdigest()[:8]}.mp4"
            
            render_id, message = shotstack_service.initiate_shotstack_render(
                cloudinary_video_url_or_urls=cloudinary_video_urls,
                video_metadata=all_tasks_metadata,
                original_filename=combined_filename, 
                instagram_username=instagram_username,
                email=email,
                linkedin_profile=linkedin_profile,
                connect_videos=True
            )

            if render_id:
                concatenated_task_id = f"concatenated_video_{render_id}" 
                new_concatenated_task = Task(
                    task_id=concatenated_task_id,
                    instagram_username=instagram_username, 
                    email=email,
                    linkedin_profile=linkedin_profile,
                    original_filename=combined_filename,
                    status='concatenated_pending', 
                    timestamp=datetime.now(),
                    cloudinary_url=None, 
                    video_metadata={
                        "combined_from_tasks": [t.task_id for t in valid_tasks],
                        "total_duration": sum(m.get('duration', 0) for m in all_tasks_metadata if m)
                    }, 
                    message=f"Concatenated video render initiated with ID: {render_id}",
                    shotstackRenderId=render_id,
                    shotstackUrl=None
                )
                session.add(new_concatenated_task)
                session.commit()
                logger.info(f"[PROCESS_VIDEOS] Shotstack render initiated for connected videos. New Task ID: {concatenated_task_id}, Render ID: {render_id}")
            else:
                session.rollback()
                logger.error(f"[PROCESS_VIDEOS] Shotstack API did not return a render ID for connected videos. Unexpected. Message: {message}")
                return jsonify({"error": "Failed to get Shotstack render ID for concatenated video. (Service issue)", "details": message}), 500

        else: 
            initiated_tasks_info = []
            for task in valid_tasks:
                if task.status != 'completed': 
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
                        session.add(task) 
                        logger.info(f"[PROCESS_VIDEOS] Shotstack render initiated for single video {task.original_filename}. Render ID: {render_id_single}")
                        initiated_tasks_info.append({
                            "taskId": task.task_id,
                            "shotstackRenderId": render_id_single,
                            "message": message_single
                        })
                    else:
                        logger.error(f"[PROCESS_VIDEOS] Shotstack API did not return a render ID for single video {task.task_id}. Unexpected. Message: {message_single}")
                        initiated_tasks_info.append({
                            "taskId": task.task_id,
                            "error": f"Failed to get Shotstack render ID for single video: {message_single}"
                        })
                except Exception as e:
                    logger.exception(f"[PROCESS_VIDEOS] Error initiating Shotstack for task {task.task_id}:")
                    initiated_tasks_info.append({
                        "taskId": task.task_id,
                        "error": str(e)
                    })
            
            session.commit() 
            return jsonify({
                "message": "Individual video processing initiated.",
                "initiated_tasks": initiated_tasks_info
            }), 200

        if connect_videos and concatenated_task_id:
            return jsonify({
                "message": message,
                "shotstackRenderId": render_id,
                "concatenated_task_id": concatenated_task_id 
            }), 200
        elif connect_videos and not concatenated_task_id:
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


@app.route('/heavy-tasks/pending', methods=['GET'])
def get_heavy_tasks():
    logger.info("[HEAVY_TASKS] Request for heavy tasks received.")
    return jsonify({"message": "No heavy tasks pending for local worker yet."}), 200

# ---
## Тестовые эндпоинты для отладки Shotstack
#
# ВНИМАНИЕ: Эти эндпоинты предназначены только для отладки.
# Удалите их из продакшн-кода после завершения тестирования!
# ---

@app.route('/test-shotstack-simple', methods=['GET']) 
def test_shotstack_simple_connection():
    logger.info("[TEST_SHOTSTACK_SIMPLE] Received request to test simple Shotstack connection.")
    try:
        shotstack_api_key = os.environ.get('SHOTSTACK_API_KEY')
        shotstack_render_url = "https://api.shotstack.io/stage/render"

        if not shotstack_api_key:
            logger.error("[TEST_SHOTSTACK_SIMPLE] ERROR: SHOTSTACK_API_KEY is not set!")
            return jsonify({"status": "error", "message": "SHOTSTACK_API_KEY environment variable is not set."}), 500

        headers = {
            "Content-Type": "application/json",
            "x-api-key": shotstack_api_key
        }

        test_payload = {
            "timeline": {
                "tracks": [
                    {
                        "clips": [
                            {
                                "asset": {
                                    "type": "title",
                                    "text": "Hello Shotstack!",
                                    "style": "minimal",
                                    "color": "#FF0000",
                                    "size": "large"
                                },
                                "start": 0,
                                "length": 2 
                            }
                        ]
                    }
                ],
                "background": "#0000FF"
            },
            "output": {
                "format": "mp4",
                "resolution": "sd"
            }
        }

        logger.info(f"[TEST_SHOTSTACK_SIMPLE] Sending test request to Shotstack API: {shotstack_render_url}")
        response = requests.post(shotstack_render_url, headers=headers, json=test_payload)
        response.raise_for_status() # Raise an exception for HTTP errors (4xx or 5xx)
        
        response_data = response.json()
        logger.info(f"[TEST_SHOTSTACK_SIMPLE] Shotstack API response: {response_data}")

        if response_data and response_data.get('success'):
            render_id = response_data['id']
            logger.info(f"[TEST_SHOTSTACK_SIMPLE] Shotstack render initiated successfully. Render ID: {render_id}")
            return jsonify({"status": "success", "message": "Shotstack test render initiated.", "render_id": render_id}), 200
        else:
            logger.error(f"[TEST_SHOTSTACK_SIMPLE] Shotstack API returned an error or unexpected response: {response_data}")
            return jsonify({"status": "error", "message": "Shotstack test render failed or returned unexpected response.", "details": response_data}), 500

    except requests.exceptions.RequestException as e:
        logger.exception(f"[TEST_SHOTSTACK_SIMPLE] Network or API error during Shotstack test:")
        return jsonify({"status": "error", "message": f"Network or API error: {e}", "details": str(e)}), 500
    except json.JSONDecodeError:
        logger.exception(f"[TEST_SHOTSTACK_SIMPLE] JSON decode error from Shotstack response:")
        return jsonify({"status": "error", "message": "Failed to decode JSON from Shotstack response."}), 500
    except Exception as e:
        logger.exception(f"[TEST_SHOTSTACK_SIMPLE] An unexpected error occurred during Shotstack test:")
        return jsonify({"status": "error", "message": "An unexpected server error occurred.", "details": str(e)}), 500

if __name__ == '__main__':
    # Используем порт из переменной окружения PORT (для Render) или 5000 по умолчанию (для локальной разработки)
    port = int(os.environ.get('PORT', 5000))
    app.run(debug=True, host='0.0.0.0', port=port)
