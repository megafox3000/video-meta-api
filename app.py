import os
import datetime
from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from flask_cors import CORS
import cloudinary
import cloudinary.uploader

import json # Добавить импорт json

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

# --- Модель базы данных (должна быть уже у вас) ---
class Task(db.Model):
    __tablename__ = 'tasks'
    id = db.Column(db.Integer, primary_key=True)
    task_id = db.Column(db.String(255), unique=True, nullable=False) # Уникальность остается
    username = db.Column(db.String(255), nullable=False)
    status = db.Column(db.String(50), nullable=False, default='pending')
    filename = db.Column(db.String(255), nullable=False)
    cloudinary_url = db.Column(db.String(500), nullable=True)
    # Используем SQLAlchemy.JSON для хранения JSON-данных
    video_metadata = db.Column(db.JSON, nullable=True) 
    message = db.Column(db.Text, nullable=True)
    timestamp = db.Column(db.DateTime, default=datetime.datetime.now)

    def __repr__(self):
        return f"<Task {self.task_id}>"

# !!! Обязательно создайте таблицы, если они еще не существуют:
# with app.app_context():
#    db.create_all()


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
        # --- ПРОВЕРЯЕМ, СУЩЕСТВУЕТ ЛИ ЗАДАЧА УЖЕ ---
        existing_task = Task.query.filter_by(task_id=generated_task_id).first()

        if existing_task:
            print(f"[PYTHON BACKEND] Задача с task_id '{generated_task_id}' уже существует. Обновляем её.")
            # Если задача существует, НЕ загружаем файл заново.
            # Вместо этого, получаем существующие данные из Cloudinary или просто обновляем статус.
            # В данном случае, мы хотим получить МЕТАДАННЫЕ, если они уже есть на Cloudinary.

            # Попытаемся получить метаданные по public_id (который является частью task_id)
            # Public ID для Cloudinary будет частью generated_task_id
            cloudinary_public_id = generated_task_id # Предполагаем, что ваш public_id совпадает с task_id
                                                      # Если public_id отличается, используйте existing_task.cloudinary_url для его извлечения

            # !!! ВАЖНО: Если ваш public_id в Cloudinary содержит расширение файла,
            # то его нужно будет удалить при формировании public_id.
            # Если вы используете uploaded_video['public_id'] как task_id, то все ок.
            # Если ваш public_id в Cloudinary: 'hife_video_analysis/1/AKHO6881', то это public_id.
            # Если ваш public_id в Cloudinary: 'hife_video_analysis/1/AKHO6881.mp4', то нужен public_id без расширения.

            # Предположим, что public_id совпадает с task_id (без расширения).
            # Если ваш public_id в Cloudinary включает расширение (e.g. .mp4), вам нужно будет его убрать.
            # Пример: 'hife_video_analysis/1/AKHO6881'
            
            try:
                # Получаем информацию о ресурсе с Cloudinary
                resource_info = cloudinary.api.resource(cloudinary_public_id, resource_type="video")
                cloudinary_url = resource_info.get('secure_url')
                # Удаляем ненужные поля из метаданных Cloudinary перед сохранением в БД
                video_metadata = {k: v for k, v in resource_info.items() if k not in ['url', 'secure_url', 'type']}

                existing_task.status = 'completed' # Или другой статус, если вы хотите переанализировать
                existing_task.cloudinary_url = cloudinary_url
                existing_task.video_metadata = video_metadata
                existing_task.message = 'Видео уже существует на Cloudinary. Информация обновлена.'
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
                # Если файл не найден на Cloudinary, хотя запись в БД есть (возможно, был удален вручную)
                print(f"[PYTHON BACKEND] Ошибка Cloudinary при попытке получить существующий ресурс: {e}")
                # В этом случае, возможно, лучше удалить запись из БД и загрузить заново
                db.session.delete(existing_task)
                db.session.commit()
                # ИЛИ продолжить, как будто записи нет, и загрузить заново:
                pass # Пройдет к блоку else (загрузка нового файла)

        # Если задача не найдена ИЛИ возникла ошибка при получении из Cloudinary
        # (в блоке except cloudinary.exceptions.Error)
        # -> продолжаем загрузку нового файла
        if not existing_task or (existing_task and 'resource_info' not in locals()): # 'resource_info' not in locals() checks if Cloudinary lookup failed
            # --- Загрузка нового видео на Cloudinary ---
            # public_id будет таким же, как generated_task_id
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
                # Обновляем существующую запись, если мы дошли сюда из-за ошибки Cloudinary lookup
                existing_task.status = 'completed'
                existing_task.cloudinary_url = cloudinary_url
                existing_task.video_metadata = video_metadata
                existing_task.message = 'Видео загружено заново и информация обновлена.'
                existing_task.timestamp = datetime.datetime.now()
            else:
                # Создаем новую запись, если задачи не было в БД
                new_task = Task(
                    task_id=generated_task_id, # Используем generated_task_id для consistency
                    username=instagram_username,
                    status='completed', # Статус "completed" после загрузки и получения метаданных
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

   except Exception as e:
        db.session.rollback() # Откатываем изменения в случае ошибки
        error_message = f"General error during upload: {e}"
        print(f"[PYTHON BACKEND] {error_message}")
        
        # --- ИСПРАВЛЕНИЕ: БОЛЕЕ УНИВЕРСАЛЬНЫЙ ВЫВОД ОШИБОК ---
        # Проверяем, является ли исключение ошибкой SQLAlchemy с оригинальными деталями
        from sqlalchemy.exc import SQLAlchemyError # Добавьте этот импорт в начало файла!
        if isinstance(e, SQLAlchemyError) and hasattr(e.orig, 'pginfo'):
            print(f"[SQL: {e.orig.pginfo.query}]")
            print(f"[parameters: {e.orig.pginfo.parameters}]")
        else:
            # Для всех остальных типов ошибок просто выводим их строковое представление
            print(f"[PYTHON BACKEND] Детали ошибки: {str(e)}")
        # --- КОНЕЦ ИСПРАВЛЕНИЯ ---

        return jsonify({'error': error_message}), 500

# --- Endpoint для получения статуса задачи (без изменений, но убедитесь, что он есть) ---
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
    app.run(debug=True, port=os.environ.get('PORT', 5000))
