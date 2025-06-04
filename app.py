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
# Убедитесь, что переменная окружения DATABASE_URL установлена на Render.com
DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL environment variable is not set!")

engine = create_engine(DATABASE_URL)
Base = declarative_base()
Session = sessionmaker(bind=engine)

# Task model definition
class Task(Base):
    __tablename__ = 'tasks'

    id = Column(Integer, primary_key=True) # Автоматически генерируемый целочисленный ID
    task_id = Column(String, unique=True, nullable=False) # Ваш уникальный строковый ID (теперь полный путь Cloudinary)
    instagram_username = Column(String)
    email = Column(String)
    linkedin_profile = Column(String)
    original_filename = Column(String) # Исходное имя файла при загрузке
    status = Column(String) # Например: 'uploaded', 'processing', 'completed', 'error', 'concatenated'
    cloudinary_url = Column(String)
    video_metadata = Column(JSON) # Хранит полные метаданные Cloudinary
    message = Column(Text)
    timestamp = Column(DateTime, default=datetime.now)

    def __repr__(self):
        return f"<Task(task_id='{self.task_id}', status='{self.status}')>"

    def to_dict(self):
        # Преобразование объекта Task в словарь для JSON-ответа фронтенду
        return {
            "taskId": self.task_id, # Используйте taskId для фронтенда
            "instagram_username": self.instagram_username,
            "email": self.email,
            "linkedin_profile": self.linkedin_profile,
            "originalFilename": self.original_filename, # camelCase для совместимости с фронтендом
            "status": self.status,
            "cloudinary_url": self.cloudinary_url,
            "metadata": self.video_metadata, # Все еще "metadata" для совместимости с фронтендом
            "message": self.message,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None
        }

# Function to create database tables
def create_tables():
    # Создает таблицы в базе данных, если они еще не существуют
    Base.metadata.create_all(engine)
    print("Database tables created or already exist.")

# Call table creation function on app startup
create_tables()

# ----------- GPS & METADATA FUNCTIONS (без изменений) -----------
# Эти функции остаются без изменений, так как они не влияют на проблему конкатенации.
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

        # Очистка имени пользователя для безопасного использования в пути Cloudinary
        # Убедитесь, что cleaned_username не пуст и содержит только разрешенные символы
        cleaned_username = "".join(c for c in (instagram_username or '').strip() if c.isalnum() or c in ('_', '-')).strip()
        if not cleaned_username:
            print("[UPLOAD] Instagram username is empty or invalid after cleaning.")
            return jsonify({"error": "Instagram username is required and must be valid."}), 400

        # Генерация public_id на основе исходного имени файла и папки
        original_filename_base = os.path.splitext(filename)[0]
        # task_id теперь будет полным путем Cloudinary, включая папку
        full_public_id = f"hife_video_analysis/{cleaned_username}/{original_filename_base}"

        print(f"[{full_public_id}] Received upload request for file: '{filename}'")
        print(f"[{full_public_id}] User data: Instagram='{instagram_username}', Email='{email}', LinkedIn='{linkedin_profile}'")

        # Проверка, существует ли задача с этим full_public_id уже в нашей БД
        existing_task = session.query(Task).filter_by(task_id=full_public_id).first()
        cloudinary_resource_exists = False

        if existing_task:
            print(f"[{full_public_id}] Task with task_id '{full_public_id}' found in DB. Checking Cloudinary...")
            try:
                # Попытка получить информацию о ресурсе из Cloudinary по полному public_id
                resource_info = cloudinary.api.resource(full_public_id, resource_type="video")
                cloudinary_resource_exists = True
                print(f"[{full_public_id}] Resource found on Cloudinary.")
                # Если ресурс найден, обновить существующую задачу в БД текущими данными Cloudinary
                existing_task.cloudinary_url = resource_info.get('secure_url')
                existing_task.video_metadata = resource_info # Сохранить полные метаданные
                existing_task.status = 'completed' # Изменено с 'uploaded' на 'completed'
                existing_task.message = 'Video already exists on Cloudinary. DB info updated.'
                existing_task.timestamp = datetime.now() # Обновить временную метку
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
                existing_task = None # Обрабатывать как новую загрузку
            except Exception as e:
                print(f"[{full_public_id}] Error checking Cloudinary resource: {e}. Will re-upload.")
                existing_task = None # При любой ошибке проверки предполагать повторную загрузку

        # Если задача не найдена в БД ИЛИ ресурс не найден в Cloudinary (т.е. требуется загрузка)
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
                    # Обновить существующую задачу в БД после успешной загрузки
                    print(f"[{full_public_id}] Updating existing task in DB after upload.")
                    existing_task.instagram_username = instagram_username
                    existing_task.email = email
                    existing_task.linkedin_profile = linkedin_profile
                    existing_task.original_filename = filename
                    existing_task.status = 'completed' # Изменено с 'uploaded' на 'completed'
                    existing_task.timestamp = datetime.now()
                    existing_task.cloudinary_url = cloudinary_url
                    existing_task.video_metadata = upload_result
                    existing_task.message = 'Video re-uploaded to Cloudinary and DB info updated.'
                else:
                    # Создать новую задачу в БД
                    print(f"[{full_public_id}] Creating a new task in DB.")
                    new_task = Task(
                        task_id=full_public_id,
                        instagram_username=instagram_username,
                        email=email,
                        linkedin_profile=linkedin_profile,
                        original_filename=filename,
                        status='completed', # Изменено с 'uploaded' на 'completed'
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

        # Шаг 1: Получить метаданные для каждого видео, чтобы рассчитать длительность
        # Нам нужно получить длительность каждого видео, чтобы правильно рассчитать start_offset для склейки.
        for public_id_full_path in public_ids_from_frontend:
            print(f"[CONCAT] Getting metadata for video: {public_id_full_path}")
            try:
                # Используйте cloudinary.api.resource для получения метаданных, включая длительность
                resource = cloudinary.api.resource(public_id_full_path, resource_type="video")
                duration = resource.get('duration', 0)
                # Проверка, является ли длительность допустимой. Если 0, это может указывать на проблему или очень короткое видео.
                if duration == 0:
                    print(f"[CONCAT] Warning: Video {public_id_full_path} has 0 duration. This might affect concatenation.")
                video_durations.append(duration)
                print(f"[CONCAT] Duration for {public_id_full_path}: {duration} seconds.")
            except cloudinary.api.NotFound:
                print(f"[CONCAT] Error: Video with public_id {public_id_full_path} not found on Cloudinary.")
                return jsonify({'error': f'Video with public_id {public_id_full_path} not found.'}), 404
            except Exception as e:
                print(f"[CONCAT] Error getting metadata for {public_id_full_path}: {e}")
                return jsonify({'error': f'Error getting metadata for {public_id_full_path}: {str(e)}'}), 500

        # Шаг 2: Создать список трансформаций для генерации URL Cloudinary
        transformations = []
        # Глобальные настройки для выходного объединенного видео
        # Для конкатенации видео Cloudinary рекомендует устанавливать video_codec и format
        transformations.append({"video_codec": "auto", "format": "mp4", "quality": "auto"})

        # Добавить наложения для последующих видео с использованием флага 'splice'
        current_offset_duration = 0
        for i, public_id_full_path in enumerate(public_ids_from_frontend):
            if i == 0:
                # Первое видео является базой для генерации URL.
                # Его длительность добавляется к смещению для последующих видео.
                current_offset_duration += video_durations[i]
                continue # Пропустить добавление его в качестве наложения в список трансформаций

            transformations.append({
                "overlay": public_id_full_path, # Public ID видео для наложения
                "flag": "splice", # Флаг для конкатенации
                "start_offset": f"{current_offset_duration:.2f}", # Начать после предыдущих видео
                "resource_type": "video" # Указать тип ресурса для наложения
            })
            current_offset_duration += video_durations[i] # Добавить длительность текущего видео к смещению для следующего

        print(f"[CONCAT] Generated transformations for URL: {transformations}")

        # Шаг 3: Сгенерировать "на лету" URL для объединенного видео
        # Этот URL представляет видео, как если бы оно уже было объединено.
        concatenated_stream_url = cloudinary.utils.cloudinary_url(
            public_ids_from_frontend[0], # Базовый public_id для URL
            resource_type="video",
            transformation=transformations,
            type="upload" # Важно: тип 'upload' для существующих загруженных ресурсов
        )[0]
        print(f"[CONCAT] Generated concatenated stream URL: {concatenated_stream_url}")

        # Шаг 4: Загрузить этот "на лету" URL, чтобы создать новый, постоянный ресурс
        concat_folder = "hife_video_analysis/concatenated"
        # Сгенерировать уникальный public_id для нового объединенного видео
        # Использование хэша выбранных public ID и текущего времени для уникальности
        concat_unique_string = f"concatenated-{'_'.join(public_ids_from_frontend)}-{time.time()}"
        new_concatenated_base_id = hashlib.sha256(concat_unique_string.encode()).hexdigest()[:20]
        new_concatenated_full_public_id = f"{concat_folder}/{new_concatenated_base_id}"
        new_filename = f"concatenated_video_{new_concatenated_base_id}.mp4"

        print(f"[CONCAT] Uploading the generated stream URL to create a new asset with public_id: {new_concatenated_full_public_id}")

        upload_result = cloudinary.uploader.upload(
            concatenated_stream_url, # Передать сгенерированный URL в качестве источника для загрузки
            resource_type="video",
            folder=concat_folder,
            public_id=new_concatenated_base_id,
            unique_filename=False,
            overwrite=True,
            # Здесь нет параметра 'transformation', так как URL уже содержит трансформации
            # Если только вы не хотите применить *дополнительные* трансформации к конечному объединенному видео.
        )
        print(f"[CONCAT] Result of final concatenated video upload to Cloudinary: {upload_result}")

        if upload_result and upload_result.get('secure_url'):
            new_video_url = upload_result['secure_url']
            print(f"[CONCAT] New persistent concatenated video URL: {new_video_url}")

            # Создать новую запись задачи для объединенного видео
            new_task = Task(
                task_id=new_concatenated_full_public_id,
                instagram_username=request.form.get('instagram_username', 'concatenated'), # Использовать заполнитель
                email=request.form.get('email', 'concatenated@example.com'),
                linkedin_profile=request.form.get('linkedin_profile', 'N/A'),
                original_filename=new_filename,
                status='concatenated', # Новый статус для объединенных видео
                timestamp=datetime.now(),
                cloudinary_url=new_video_url,
                video_metadata=upload_result, # Сохранить полные метаданные объединенного видео
                message='Video successfully concatenated and saved as new asset.'
            )
            session.add(new_task)
            session.commit()
            print(f"[CONCAT] New concatenated video task created in DB: {new_concatenated_full_public_id}")

            return jsonify({
                'message': 'Videos successfully concatenated.',
                'new_public_id': new_concatenated_full_public_id,
                'new_video_url': new_video_url,
                'metadata': upload_result # Вернуть полные метаданные для нового видео
            }), 200
        else:
            print("[CONCAT] Cloudinary final upload failed: secure_url missing in response.")
            return jsonify({'error': 'Cloudinary concatenation failed', 'details': upload_result}), 500

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
    # Используйте waitress для продакшн-развертывания
    from waitress import serve
    port = int(os.environ.get('PORT', 8080))
    serve(app, host='0.0.0.0', port=port)
    # Для локальной разработки вы можете использовать:
    # app.run(debug=True, host='0.0.0.0', port=os.environ.get('PORT', 5000))
