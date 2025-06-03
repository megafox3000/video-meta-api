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
import hashlib # ДОБАВЛЕНО для конкатенации
import time # ДОБАВЛЕНО для конкатенации
import requests # Для геокодинга

app = Flask(__name__)
CORS(app)

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
    raise RuntimeError("DATABASE_URL environment variable is not set!")

engine = create_engine(DATABASE_URL)
Base = declarative_base() # ИСПРАВЛЕНО: Объявление Base
Session = sessionmaker(bind=engine)

# Определение модели Task
class Task(Base):
    __tablename__ = 'tasks'

    id = Column(String, primary_key=True, unique=True, nullable=False) # Используем String для task_id/public_id
    instagram_username = Column(String)
    email = Column(String)
    linkedin_profile = Column(String)
    original_filename = Column(String)
    status = Column(String) # Например: 'uploaded', 'processing', 'completed', 'error', 'concatenated'
    cloudinary_url = Column(String)
    metadata = Column(JSON) # Хранение полных метаданных Cloudinary
    message = Column(Text)
    timestamp = Column(DateTime, default=datetime.now)

    def __repr__(self):
        return f"<Task(id='{self.id}', status='{self.status}')>"
    
    def to_dict(self):
        return {
            "taskId": self.id,
            "instagram_username": self.instagram_username,
            "email": self.email,
            "linkedin_profile": self.linkedin_profile,
            "original_filename": self.original_filename,
            "status": self.status,
            "cloudinary_url": self.cloudinary_url,
            "metadata": self.metadata,
            "message": self.message,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None
        }

# Функция для создания таблиц в базе данных
def create_tables():
    Base.metadata.create_all(engine)
    print("Database tables created or already exist.")

# Вызов функции создания таблиц при запуске приложения
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
        print(f"Ошибка геокодинга: {e}")
        return f"Ошибка геокодинга: {e}"

# ----------- ЭНДПОИНТЫ API -----------

@app.route('/')
def index():
    print("[PYTHON BACKEND] Корневой путь '/' был запрошен. Проверяем вывод print.")
    return jsonify({"status": "✅ Python Backend is up and running!"})

@app.route('/upload_video', methods=['POST'])
def upload_video():
    session = Session() # Сессия SQLAlchemy создается внутри эндпоинта
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

        # Генерируем уникальный task_id, который будет также public_id Cloudinary
        # Используем хэш от комбинации имени файла и текущего времени для уникальности
        unique_string = f"{filename}-{datetime.now().isoformat()}-{os.urandom(4).hex()}"
        task_id = hashlib.sha256(unique_string.encode()).hexdigest()[:20] # Сокращаем до 20 символов

        print(f"[{task_id}] Получен запрос на загрузку файла: '{filename}'")
        print(f"[{task_id}] Данные пользователя: Instagram='{instagram_username}', Email='{email}', LinkedIn='{linkedin_profile}'")

        # Проверяем, существует ли задача с таким ID в нашей БД
        existing_task = session.query(Task).filter_by(id=task_id).first()
        cloudinary_resource_exists = False

        if existing_task:
            print(f"[{task_id}] Задача с ID '{task_id}' найдена в БД. Проверяем Cloudinary...")
            try:
                # Пытаемся получить информацию о ресурсе из Cloudinary
                cloudinary.api.resource(task_id, resource_type="video")
                cloudinary_resource_exists = True
                print(f"[{task_id}] Ресурс найден на Cloudinary.")
            except cloudinary.api.NotFound:
                print(f"[{task_id}] Ресурс НЕ найден на Cloudinary, несмотря на запись в БД. Будет перезагружен.")
                existing_task = None # Обрабатываем как новую загрузку

        # Если задача не найдена в БД ИЛИ ресурс не найден на Cloudinary (т.е. нужна загрузка)
        if not existing_task or not cloudinary_resource_exists:
            print(f"[{task_id}] Загружаем/перезагружаем видео на Cloudinary...")
            upload_result = cloudinary.uploader.upload(
                file,
                resource_type="video",
                public_id=task_id, # Используем наш сгенерированный task_id как public_id
                unique_filename=False, # Мы контролируем имя файла
                overwrite=True, # Перезаписываем, если public_id уже существует
                quality="auto", # Оптимизируем качество видео
                format="mp4" # Обеспечиваем формат mp4
            )
            print(f"[{task_id}] Ответ от Cloudinary после загрузки: {upload_result}")

            if upload_result and upload_result.get('secure_url'):
                cloudinary_url = upload_result['secure_url']
                print(f"[{task_id}] Cloudinary URL: {cloudinary_url}")

                if existing_task:
                    # Обновляем существующую задачу в БД
                    print(f"[{task_id}] Обновляем существующую задачу в БД.")
                    existing_task.instagram_username = instagram_username
                    existing_task.email = email
                    existing_task.linkedin_profile = linkedin_profile
                    existing_task.original_filename = filename
                    existing_task.status = 'uploaded' # Статус после успешной загрузки
                    existing_task.timestamp = datetime.now()
                    existing_task.cloudinary_url = cloudinary_url
                    existing_task.metadata = upload_result # Сохраняем полный ответ Cloudinary
                else:
                    # Создаем новую задачу в БД
                    print(f"[{task_id}] Создаем новую задачу в БД.")
                    new_task = Task(
                        id=task_id,
                        instagram_username=instagram_username,
                        email=email,
                        linkedin_profile=linkedin_profile,
                        original_filename=filename,
                        status='uploaded', # Изначальный статус после загрузки
                        timestamp=datetime.now(),
                        cloudinary_url=cloudinary_url,
                        metadata=upload_result # Сохраняем полный ответ Cloudinary
                    )
                    session.add(new_task)
                session.commit()
                print(f"[{task_id}] Изменения в БД успешно зафиксированы.")
                return jsonify({'message': 'Видео загружено и задача создана/обновлена', 'taskId': task_id, 'cloudinary_url': cloudinary_url}), 200
            else:
                print(f"[{task_id}] Загрузка в Cloudinary не удалась: отсутствует secure_url в ответе.")
                return jsonify({'error': 'Загрузка в Cloudinary не удалась'}), 500
        else:
            # Видео уже существует в БД и на Cloudinary, просто возвращаем его информацию
            print(f"[{task_id}] Видео уже существует в БД и на Cloudinary. Возвращаем существующую информацию.")
            return jsonify({
                'message': 'Видео уже существует',
                'taskId': existing_task.id,
                'cloudinary_url': existing_task.cloudinary_url
            }), 200

    except SQLAlchemyError as e:
        session.rollback() # Откатываем изменения в БД в случае ошибки
        print(f"[{task_id if 'task_id' in locals() else 'N/A'}] Ошибка базы данных: {e}")
        print(f"Информация об оригинальной ошибке: {e.orig.pginfo if hasattr(e.orig, 'pginfo') else 'N/A'}")
        return jsonify({'error': 'Ошибка базы данных', 'details': str(e)}), 500
    except Exception as e:
        session.rollback() # Откатываем изменения в БД даже при других исключениях
        print(f"[{task_id if 'task_id' in locals() else 'N/A'}] Произошла непредвиденная ошибка во время загрузки: {e}")
        return jsonify({'error': 'Произошла непредвиденная ошибка', 'details': str(e)}), 500
    finally:
        session.close() # Гарантированное закрытие сессии SQLAlchemy

@app.route('/task-status/<path:task_id>', methods=['GET'])
def get_task_status(task_id):
    session = Session() # Сессия SQLAlchemy создается внутри эндпоинта
    try:
        print(f"\n[STATUS] Получен запрос статуса для task_id: '{task_id}'")
        task_info = session.query(Task).filter_by(id=task_id).first()
        if task_info:
            print(f"[STATUS] Задача найдена в БД: {task_info.id}, статус: {task_info.status}")
            return jsonify(task_info.to_dict()), 200
        else:
            print(f"[STATUS] Задача с task_id '{task_id}' НЕ НАЙДЕНА в БД.")
            return jsonify({"message": "Task not found."}), 404
    finally:
        session.close() # Гарантированное закрытие сессии

@app.route('/heavy-tasks/pending', methods=['GET'])
def get_heavy_tasks():
    # Этот эндпоинт, предположительно, для фоновых воркеров.
    # В текущей реализации Cloudinary выполняет тяжелую работу.
    # Если у вас есть отдельный воркер, он мог бы опрашивать этот эндпоинт.
    print("[HEAVY_TASKS] Запрос на получение тяжелых задач.")
    return jsonify({"message": "No heavy tasks pending for local worker yet."}), 200

# НОВЫЙ ЭНДПОИНТ ДЛЯ КОНКАТЕНАЦИИ ВИДЕО
@app.route('/concatenate_videos', methods=['POST'])
def concatenate_videos():
    session = Session() # Сессия SQLAlchemy создается внутри эндпоинта
    try:
        data = request.get_json()
        public_ids = data.get('public_ids')

        if not public_ids or not isinstance(public_ids, list) or len(public_ids) < 2:
            print("[CONCAT] Запрос на конкатенацию получен с менее чем 2 public_ids.")
            return jsonify({'error': 'Пожалуйста, предоставьте как минимум два public_ids видео для объединения.'}), 400

        print(f"[CONCAT] Получен запрос на конкатенацию для public_ids: {public_ids}")

        transformations = []
        current_duration = 0

        # Шаг 1: Получаем метаданные для каждого видео, чтобы рассчитать start_offset
        for i, public_id in enumerate(public_ids):
            print(f"[CONCAT] Получение метаданных для видео: {public_id}")
            try:
                resource = cloudinary.api.resource(public_id, resource_type="video")
                duration = resource.get('duration', 0)
                print(f"[CONCAT] Длительность для {public_id}: {duration} секунд.")

                if i == 0:
                    # Первое видео является базовым, без оверлея и смещения
                    # Добавляем трансформацию для кодека, чтобы обеспечить совместимость
                    transformations.append({"video_codec": "auto"})
                else:
                    # Последующие видео добавляются как оверлеи с флагом 'splice' и start_offset
                    transformations.append({
                        "overlay": public_id,
                        "flag": "splice",
                        "start_offset": f"{current_duration:.2f}", # Используем текущую общую длительность как start_offset
                        "resource_type": "video" # Указываем, что оверлей - это видео
                    })
                current_duration += duration
            except cloudinary.api.NotFound:
                print(f"[CONCAT] Ошибка: Видео с public_id {public_id} не найдено на Cloudinary.")
                return jsonify({'error': f'Видео с public_id {public_id} не найдено.'}), 404
            except Exception as e:
                print(f"[CONCAT] Ошибка при получении метаданных для {public_id}: {e}")
                return jsonify({'error': f'Ошибка при получении метаданных для {public_id}: {str(e)}'}), 500

        # Шаг 2: Генерируем URL объединенного видео
        # public_id первого видео используется как основа для трансформаций
        base_public_id = public_ids[0]
        print(f"[CONCAT] Базовый public_id для конкатенации: {base_public_id}")
        print(f"[CONCAT] Сгенерированные трансформации: {transformations}")

        # Создаем уникальный public_id для нового объединенного видео
        concat_unique_string = f"concatenated-{'-'.join(public_ids)}-{time.time()}-{os.urandom(4).hex()}"
        new_public_id = hashlib.sha256(concat_unique_string.encode()).hexdigest()[:20]
        new_filename = f"concatenated_video_{new_public_id}.mp4"

        # Генерируем временный URL с трансформациями, который затем будет "загружен"
        # Это позволяет Cloudinary выполнить конкатенацию и создать новый ресурс
        concatenated_temp_url = cloudinary.utils.cloudinary_url(
            base_public_id,
            resource_type="video",
            transformation=transformations,
            format="mp4",
            fetch_format="auto",
            quality="auto",
            type="upload" # Важно: указываем тип 'upload' для загруженных ресурсов
        )[0] # cloudinary_url возвращает кортеж, берем первый элемент (URL)
        print(f"[CONCAT] Сгенерированный временный URL объединенного видео: {concatenated_temp_url}")


        # Шаг 3: Загружаем объединенное видео (по его сгенерированному URL)
        # Это делает его постоянным ресурсом в Cloudinary
        print(f"[CONCAT] Загружаем объединенное видео в Cloudinary с новым public_id: {new_public_id}")
        upload_result = cloudinary.uploader.upload(
            concatenated_temp_url, # Загружаем URL сгенерированной трансформации
            resource_type="video",
            public_id=new_public_id,
            unique_filename=False,
            overwrite=True,
            quality="auto",
            format="mp4"
        )
        print(f"[CONCAT] Результат загрузки объединенного видео в Cloudinary: {upload_result}")

        if upload_result and upload_result.get('secure_url'):
            new_video_url = upload_result['secure_url']
            print(f"[CONCAT] Новый URL объединенного видео: {new_video_url}")

            # Сохраняем информацию о новом объединенном видео в базу данных
            new_task = Task(
                id=new_public_id,
                instagram_username="N/A", # Можно расширить, чтобы брать из первого видео или запросить
                email="N/A",
                linkedin_profile="N/A",
                original_filename=new_filename,
                status='concatenated', # Новый статус для объединенных видео
                timestamp=datetime.now(),
                cloudinary_url=new_video_url,
                metadata=upload_result # Сохраняем полные метаданные объединенного видео
            )
            session.add(new_task)
            session.commit()
            print(f"[CONCAT] Задача для объединенного видео сохранена в БД с ID: {new_public_id}")

            return jsonify({
                'message': 'Видео успешно объединены',
                'new_public_id': new_public_id,
                'new_video_url': new_video_url
            }), 200
        else:
            print("[CONCAT] Не удалось загрузить объединенное видео в Cloudinary.")
            return jsonify({'error': 'Не удалось загрузить объединенное видео в Cloudinary.'}), 500

    except SQLAlchemyError as e:
        session.rollback()
        print(f"[CONCAT] Ошибка базы данных во время конкатенации: {e}")
        return jsonify({'error': 'Ошибка базы данных во время конкатенации', 'details': str(e)}), 500
    except Exception as e:
        session.rollback()
        print(f"[CONCAT] Произошла непредвиденная ошибка во время конкатенации: {e}")
        return jsonify({'error': 'Произошла непредвиденная ошибка во время конкатенации', 'details': str(e)}), 500
    finally:
        session.close() # Гарантированное закрытие сессии

if __name__ == '__main__':
    from waitress import serve
    port = int(os.environ.get('PORT', 8080))
    serve(app, host='0.0.0.0', port=port)
