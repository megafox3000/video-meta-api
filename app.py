import os
from flask import Flask, request, jsonify # send_from_directory больше не нужен, т.к. не сохраняем локально
from flask_cors import CORS
from datetime import datetime

# --- ИМПОРТЫ CLOUDINARY ---
import cloudinary
import cloudinary.uploader
import cloudinary.api

# --- ИМПОРТЫ ДЛЯ POSTGRESQL ---
from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, JSON
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy_json import mutable_json_type # Для хранения JSON в PostgreSQL

app = Flask(__name__)
CORS(app)

# --- Настройка Cloudinary ---
cloudinary.config(
    cloud_name = os.environ.get('CLOUDINARY_CLOUD_NAME'),
    api_key = os.environ.get('CLOUDINARY_API_KEY'),
    api_secret = os.environ.get('CLOUDINARY_API_SECRET'),
    secure = True
)

# --- Настройка PostgreSQL ---
# Получаем URL базы данных из переменной окружения Render
DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL environment variable is not set!")

engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
Base = declarative_base()

# --- Определение модели данных для задач ---
class Task(Base):
    __tablename__ = 'tasks' # Имя таблицы в базе данных

    id = Column(Integer, primary_key=True) # Автоматический ID для записи
    task_id = Column(String, unique=True, nullable=False) # Public ID от Cloudinary, будет нашим taskId
    username = Column(String)
    status = Column(String) # Например, 'processing', 'completed', 'failed'
    filename = Column(String)
    cloudinary_url = Column(String)
    # Используем JSON тип для хранения словаря с метаданными
    metadata = Column(JSON) 
    message = Column(Text)
    timestamp = Column(DateTime, default=datetime.now)

    def __repr__(self):
        return f"<Task(task_id='{self.task_id}', status='{self.status}')>"
    
    # Метод для преобразования объекта в словарь, который можно отправить как JSON
    def to_dict(self):
        return {
            "taskId": self.task_id,
            "username": self.username,
            "status": self.status,
            "filename": self.filename,
            "cloudinary_url": self.cloudinary_url,
            "metadata": self.metadata,
            "message": self.message,
            "timestamp": self.timestamp.isoformat()
        }

# Функция для создания таблиц в базе данных
def create_tables():
    Base.metadata.create_all(engine)
    print("Database tables created or already exist.")

# ----------- GPS & METADATA FUNCTIONS (Неизменны, но не используются Cloudinary-потоком) -----------
# Оставил их, как в вашем исходном коде, но они не будут вызываться при обработке через Cloudinary.
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

    username = request.form.get('username', 'unknown_user')

    print(f"\n[PYTHON BACKEND] Получен файл '{file.filename}' от пользователя '{username}' на /analyze.")

    try:
        # --- Загрузка видео на Cloudinary ---
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

        # --- Сохранение задачи в PostgreSQL ---
        with Session() as session:
            new_task = Task(
                task_id=public_id,
                username=username,
                status=task_status,
                filename=file.filename,
                cloudinary_url=cloudinary_url,
                metadata=basic_metadata, # SQLAlchemy_JSON позволяет хранить словарь
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
        return jsonify({"error": f"Cloudinary upload failed: {str(e)}"}), 500
    except Exception as e:
        print(f"[PYTHON BACKEND] Общая ошибка: {e}")
        # Если есть активная сессия, откатываем изменения в случае ошибки
        with Session() as session:
            session.rollback()
        return jsonify({"error": f"Internal server error: {str(e)}"}), 500

# --- Эндпоинт для проверки статуса задачи ---
@app.route('/task-status/<task_id>', methods=['GET'])
def get_task_status(task_id):
    with Session() as session:
        task_info = session.query(Task).filter_by(task_id=task_id).first()
        if task_info:
            return jsonify(task_info.to_dict()), 200 # Возвращаем данные задачи из БД
        else:
            return jsonify({"message": "Task not found."}), 404

# --- Эндпоинт-заглушка для будущих "тяжелых" задач (для локального воркера) ---
@app.route('/heavy-tasks/pending', methods=['GET'])
def get_heavy_tasks():
    return jsonify({"message": "No heavy tasks pending for local worker yet."}), 200

# ----------- ENTRYPOINT -----------
if __name__ == '__main__':
    # При запуске приложения, создаем таблицы (если их нет)
    create_tables() 
    from waitress import serve
    port = int(os.environ.get('PORT', 8080))
    serve(app, host='0.0.0.0', port=port)
