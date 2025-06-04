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

# ====================================================================================================
# КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ: Добавляем параметры SSL для подключения к PostgreSQL на Render.com
# Render's PostgreSQL обычно требует SSL.
# 'sslmode=require' гарантирует использование SSL-соединения.
# Мы добавляем это через connect_args, чтобы быть уверенными, что оно применяется.
connect_args = {}
if DATABASE_URL.startswith("postgresql://") or DATABASE_URL.startswith("postgres://"):
    # Проверяем, есть ли sslmode уже в URL, чтобы избежать дублирования
    if "sslmode=" not in DATABASE_URL:
        connect_args["sslmode"] = "require" # Или "verify-full" если требуется проверка CA сертификата

engine = create_engine(DATABASE_URL, connect_args=connect_args)
# ====================================================================================================

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
        cloudinary_resource_exists = False

        if existing_task:
            print(f"[{full_public_id}] Task with task_id '{full_public_id}' found in DB. Checking Cloudinary...")
            try:
                resource_info = cloudinary.api.resource(full_public_id, resource_type="video")
                cloudinary_resource_exists = True
                print(f"[{full_public_id}] Resource found on Cloudinary.")
                existing_task.cloudinary_url = resource_info.get('secure_url')
                existing_task.video_metadata = resource_info
                existing_task.status = 'completed'
                existing_task.message = 'Video already exists on Cloudinary. DB info updated.'
                existing_task.timestamp = datetime.now()
                session.commit()
                print(f"[{full_public_id}] Existing DB task updated based on Cloudinary data.")

                return jsonify({
                    'message': 'Video already exists, info updated.',
                    'taskId': existing_task.task_id,
                    'cloudinary_url': existing_task.cloudinary_url,
                    'originalFilename': existing_task.original_filename,
                    'metadata': existing_task.video_metadata,
                    'status': existing_task.status
                }), 200

            except cloudinary.api.NotFound:
                print(f"[{full_public_id}] Resource NOT found on Cloudinary despite DB record. Will re-upload.")
                existing_task = None
            except Exception as e:
                print(f"[{full_public_id}] Error checking Cloudinary resource: {e}. Will re-upload.")
                existing_task = None

        if not existing_task or not cloudinary_resource_exists:
            print(f"[{full_public_id}] Uploading/re-uploading video to Cloudinary...")
            upload_result = cloudinary.uploader.upload(
                file,
                resource_type="video",
                folder=f"hife_video_analysis/{cleaned_username}",
                public_id=original_filename_base,
                unique_filename=False,
                overwrite=True,
                quality="auto",
                format="mp4",
                tags=["hife_analysis", cleaned_username]
            )
            print(f"[{full_public_id}] Cloudinary response after upload: {upload_result}")

            if upload_result and upload_result.get('secure_url'):
                cloudinary_url = upload_result['secure_url']
                print(f"[{full_public_id}] Cloudinary URL: {cloudinary_url}")

                if existing_task:
                    print(f"[{full_public_id}] Updating existing task in DB after upload.")
                    existing_task.instagram_username = instagram_username
                    existing_task.email = email
                    existing_task.linkedin_profile = linkedin_profile
                    existing_task.original_filename = filename
                    existing_task.status = 'completed'
                    existing_task.timestamp = datetime.now()
                    existing_task.cloudinary_url = cloudinary_url
                    existing_task.video_metadata = upload_result
                    existing_task.message = 'Video re-uploaded to Cloudinary and DB info updated.'
                else:
                    print(f"[{full_public_id}] Creating a new task in DB.")
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
                    existing_task = new_task
                session.commit()
                print(f"[{full_public_id}] DB changes successfully committed.")
                return jsonify({'message': 'Video uploaded and task created/updated', 'taskId': existing_task.task_id, 'cloudinary_url': cloudinary_url, 'metadata': existing_task.video_metadata, 'originalFilename': existing_task.original_filename}), 200
            else:
                print(f"[{full_public_id}] Cloudinary upload failed: secure_url missing in response.")
                return jsonify({'error': 'Cloudinary upload failed'}), 500

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

# NEW ENDPOINT FOR VIDEO CONCATENATION
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

        video_durations = [] # Для хранения длительностей для расчета start_offset
        all_resource_infos = [] # Для хранения полных метаданных каждого видео

        # Шаг 1: Получить метаданные для каждого видео, чтобы рассчитать длительность
        for public_id_full_path in public_ids_from_frontend:
            print(f"[CONCAT] Getting metadata for video: {public_id_full_path}")
            try:
                resource = cloudinary.api.resource(public_id_full_path, resource_type="video")
                all_resource_infos.append(resource)
                
                duration = resource.get('duration', 0)
                if duration == 0:
                    print(f"[CONCAT] Warning: Video {public_id_full_path} has 0 duration. This might affect concatenation.")
                    print(f"[CONCAT] Full Cloudinary metadata for {public_id_full_path}: {resource}")
                video_durations.append(duration)
                print(f"[CONCAT] Duration for {public_id_full_path}: {duration} seconds.")
            except cloudinary.api.NotFound:
                print(f"[CONCAT] Error: Video with public_id {public_id_full_path} not found on Cloudinary.")
                return jsonify({'error': f'Video with public_id {public_id_full_path} not found.'}), 404
            except Exception as e:
                print(f"[CONCAT] Error getting metadata for {public_id_full_path}: {e}")
                return jsonify({'error': f'Error getting metadata for {public_id_full_path}: {str(e)}'}), 500

        if all(d == 0 for d in video_durations):
            print("[CONCAT] All selected videos have 0 duration. Cannot concatenate meaningfully.")
            return jsonify({'error': 'Cannot concatenate: All selected videos have 0 duration. Please upload videos with actual content.'}), 400

        # Шаг 2: Создать список трансформаций для Cloudinary upload
        transformations = []
        transformations.append({"video_codec": "auto", "format": "mp4", "quality": "auto"})

        current_offset_duration = 0
        for i, public_id_full_path in enumerate(public_ids_from_frontend):
            if i == 0:
                current_offset_duration += video_durations[i]
                continue

            transformations.append({
                "overlay": public_id_full_path,
                "flag": "splice",
                "start_offset": f"{current_offset_duration:.2f}",
                "resource_type": "video"
            })
            current_offset_duration += video_durations[i]

        print(f"[CONCAT] Generated transformations: {transformations}")

        # Шаг 3: Загрузить объединенное видео напрямую, используя public_id первого видео в качестве основы
        concat_folder = "hife_video_analysis/concatenated"
        concat_unique_string = f"concatenated-{'_'.join(public_ids_from_frontend)}-{time.time()}"
        new_concatenated_base_id = hashlib.sha256(concat_unique_string.encode()).hexdigest()[:20]
        new_concatenated_full_public_id = f"{concat_folder}/{new_concatenated_base_id}"
        new_filename = f"concatenated_video_{new_concatenated_base_id}.mp4"

        print(f"[CONCAT] Uploading concatenated video to Cloudinary with new public_id: {new_concatenated_full_public_id}")

        upload_result = cloudinary.uploader.upload(
            public_ids_from_frontend[0],
            resource_type="video",
            folder=concat_folder,
            public_id=new_concatenated_base_id,
            unique_filename=False,
            overwrite=True,
            transformation=transformations
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
