import os
import datetime
from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from flask_cors import CORS
import cloudinary
import cloudinary.uploader
import cloudinary.api # ИСПРАВЛЕНИЕ: Добавлен импорт cloudinary.api
import json
from sqlalchemy.exc import SQLAlchemyError # ИСПРАВЛЕНИЕ: Добавлен импорт SQLAlchemyError


# --- Конфигурация Flask и SQLAlchemy ---
app = Flask(__name__)
# Используйте вашу реальную строку подключения к базе данных Render PostgreSQL
app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get('DATABASE_URL')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)
CORS(app)

# --- Конфигурация Cloudinary ---
cloudinary.config(
    cloud_name=os.environ.get('CLOUDINARY_CLOUD_NAME'),
    api_key=os.environ.get('CLOUDINARY_API_KEY'),
    api_secret=os.environ.get('CLOUDINARY_API_SECRET')
)

# --- Модель базы данных ---
class Task(db.Model):
    __tablename__ = 'tasks'
    id = db.Column(db.Integer, primary_key=True)
    task_id = db.Column(db.String(255), unique=True, nullable=False)
    username = db.Column(db.String(255), nullable=False)
    status = db.Column(db.String(50), nullable=False, default='pending')
    filename = db.Column(db.String(255), nullable=False)
    cloudinary_url = db.Column(db.String(500), nullable=True)
    video_metadata = db.Column(db.JSON, nullable=True)
    message = db.Column(db.Text, nullable=True)
    timestamp = db.Column(db.DateTime, default=datetime.datetime.now)

    def __repr__(self):
        return f"<Task {self.task_id}>"

# !!! Обязательно создайте таблицы, если они еще не существуют.
#    В продакшене это обычно делается через миграции, но для простоты
#    можно использовать db.create_all() при первом запуске, если таблиц нет.
#
# with app.app_context():
#    db.create_all()
#    print("Database tables created or already exist.") # Можно добавить для логов при деплое


# --- Endpoint для загрузки видео ---
@app.route('/upload_video', methods=['POST'])
def upload_video():
    if 'video' not in request.files:
        return jsonify({'error': 'No video file provided'}), 400

    video_file = request.files['video']
    instagram_username = request.form.get('instagram_username')

    if not instagram_username:
        return jsonify({'error': 'Instagram username is required'}), 400

    filename = video_file.filename
    if not filename:
        return jsonify({'error': 'Filename is missing'}), 400

    # --- Формируем task_id (без уникального суффикса для проверки существования) ---
    filename_no_ext = os.path.splitext(filename)[0]
    generated_task_id = f"hife_video_analysis/{instagram_username}/{filename_no_ext}"

    print(f"[PYTHON BACKEND] Загружаем файл '{filename}' для пользователя Instagram: '{instagram_username}'")

    try:
        # --- ПРОВЕРЯЕМ, СУЩЕСТВУЕТ ЛИ ЗАДАЧА УЖЕ В БД ---
        existing_task = Task.query.filter_by(task_id=generated_task_id).first()

        cloudinary_url = None
        video_metadata = None
        
        # Переменная для отслеживания, был ли ресурс найден на Cloudinary
        resource_found_on_cloudinary = False

        if existing_task:
            print(f"[PYTHON BACKEND] Задача с task_id '{generated_task_id}' уже существует. Попытка обновить информацию.")
            # Попытаемся получить метаданные по public_id (который является частью task_id)
            cloudinary_public_id = generated_task_id # Предполагаем, что ваш public_id совпадает с task_id

            try:
                # Получаем информацию о ресурсе с Cloudinary
                resource_info = cloudinary.api.resource(cloudinary_public_id, resource_type="video")
                cloudinary_url = resource_info.get('secure_url')
                # Удаляем ненужные поля из метаданных Cloudinary перед сохранением в БД
                video_metadata = {k: v for k, v in resource_info.items() if k not in ['url', 'secure_url', 'type']}
                resource_found_on_cloudinary = True # Ресурс найден на Cloudinary

                # Обновляем существующую запись в БД
                existing_task.status = 'completed'
                existing_task.cloudinary_url = cloudinary_url
                existing_task.video_metadata = video_metadata
                existing_task.message = 'Видео уже существует на Cloudinary. Информация в БД обновлена.'
                existing_task.timestamp = datetime.datetime.now()
                db.session.commit()

                return jsonify({
                    'message': 'Video already exists, info updated.',
                    'taskId': existing_task.task_id,
                    'cloudinary_url': existing_task.cloudinary_url,
                    'original_filename': existing_task.filename,
                    'metadata': existing_task.video_metadata,
                    'status': existing_task.status
                }), 200

            except cloudinary.exceptions.Error as e:
                # Если файл не найден на Cloudinary, хотя запись в БД есть (например, удален вручную)
                print(f"[PYTHON BACKEND] Ошибка Cloudinary при попытке получить существующий ресурс ({generated_task_id}): {e}")
                # Мы не удаляем запись из БД здесь. Вместо этого, мы просто продолжим,
                # и код ниже попытается загрузить файл заново.
                # Это позволит "восстановить" запись в БД, если Cloudinary-ресурс был удален.
                pass # Продолжаем выполнение, чтобы попытаться загрузить файл заново

        # Если задача не найдена в БД ИЛИ возникла ошибка при получении из Cloudinary
        # (т.е. resource_found_on_cloudinary == False)
        if not existing_task or not resource_found_on_cloudinary:
            print(f"[PYTHON BACKEND] Загружаем/перезагружаем видео на Cloudinary для '{generated_task_id}'.")
            # --- Загрузка/Перезагрузка видео на Cloudinary ---
            upload_result = cloudinary.uploader.upload(
                video_file,
                resource_type="video",
                folder=f"hife_video_analysis/{instagram_username}", # Папка на Cloudinary
                public_id=filename_no_ext, # Имя файла на Cloudinary без расширения
                overwrite=True # Разрешаем перезапись, если файл с таким public_id уже есть на Cloudinary
            )
            
            uploaded_video_info = upload_result
            cloudinary_url = uploaded_video_info['secure_url']
            
            # Удаляем ненужные поля из метаданных Cloudinary перед сохранением в БД
            video_metadata = {k: v for k, v in uploaded_video_info.items() if k not in ['url', 'secure_url', 'type']}

            # --- Создание или обновление записи в БД ---
            if existing_task:
                # Обновляем существующую запись, если мы дошли сюда, потому что Cloudinary-ресурс не был найден
                existing_task.status = 'completed'
                existing_task.cloudinary_url = cloudinary_url
                existing_task.video_metadata = video_metadata
                existing_task.message = 'Видео загружено заново на Cloudinary и информация в БД обновлена.'
                existing_task.timestamp = datetime.datetime.now()
            else:
                # Создаем новую запись, если задачи не было в БД изначально
                new_task = Task(
                    task_id=generated_task_id,
                    username=instagram_username,
                    status='completed',
                    filename=filename,
                    cloudinary_url=cloudinary_url,
                    video_metadata=video_metadata,
                    message='Видео успешно загружено на Cloudinary и получены полные метаданные.',
                    timestamp=datetime.datetime.now()
                )
                db.session.add(new_task)
                existing_task = new_task # Присваиваем new_task к existing_task для унификации ответа
            
            db.session.commit()

            return jsonify({
                'message': 'Video uploaded and saved to DB.',
                'taskId': existing_task.task_id,
                'cloudinary_url': existing_task.cloudinary_url,
                'original_filename': existing_task.filename,
                'metadata': existing_task.video_metadata,
                'status': existing_task.status
            }), 200

    # ИСПРАВЛЕНИЕ: Исправлен отступ для except-блока, он должен быть на том же уровне, что и try
    except Exception as e:
        db.session.rollback() # Откатываем изменения в случае ошибки
        error_message = f"General error during upload: {e}"
        print(f"[PYTHON BACKEND] {error_message}")
        
        # ИСПРАВЛЕНИЕ: Более универсальный вывод ошибок
        if isinstance(e, SQLAlchemyError) and hasattr(e.orig, 'pginfo'):
            print(f"[SQL: {e.orig.pginfo.query}]")
            print(f"[parameters: {e.orig.pginfo.parameters}]")
        else:
            print(f"[PYTHON BACKEND] Детали ошибки: {str(e)}")

        return jsonify({'error': error_message}), 500

# --- Endpoint для получения статуса задачи ---
@app.route('/task-status/<task_id>', methods=['GET'])
def get_task_status(task_id):
    task = Task.query.filter_by(task_id=task_id).first()
    if task:
        return jsonify({
            'taskId': task.task_id,
            'status': task.status,
            'filename': task.filename,
            'cloudinary_url': task.cloudinary_url,
            'metadata': task.video_metadata,
            'message': task.message,
            'timestamp': task.timestamp.isoformat()
        }), 200
    return jsonify({'error': 'Task not found'}), 404

# --- Запуск приложения ---
if __name__ == '__main__':
    # В продакшене используйте Gunicorn или аналогичное
    # Render автоматически устанавливает переменную окружения PORT
    # Убедитесь, что ваш скрипт Build Command на Render не запускает Flask в debug режиме,
    # а использует Gunicorn, например: gunicorn app:app
    app.run(debug=True, port=os.environ.get('PORT', 5000))
