# app.py
import os
import cloudinary
from flask import Flask, request, jsonify
from flask_cors import CORS
from datetime import datetime
import hashlib
import logging

# Импортируем наши сервисы
import shotstack_service
import cloudinary_service
import db_service

# --- Конфигурация логирования ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)

# --- ИСПРАВЛЕНО: Конфигурация CORS с добавленным методом DELETE ---
CORS(app, resources={r"/*": {
    "origins": [
        "https://megafox3000.github.io",
        "http://localhost:5500",
        "http://127.0.0.1:5500"
    ],
    "methods": ["GET", "POST", "OPTIONS", "HEAD", "DELETE"],
    "allow_headers": ["Content-Type", "Authorization", "X-Requested-With"]
}}, supports_credentials=True)


# Конфигурация Cloudinary
cloudinary.config(
    cloud_name=os.environ.get('CLOUDINARY_CLOUD_NAME'),
    api_key=os.environ.get('CLOUDINARY_API_KEY'),
    api_secret=os.environ.get('CLOUDINARY_API_SECRET'),
    secure=True
)

# --- Эндпоинты API ---

@app.route('/')
def index():
    return jsonify({"status": "✅ Python Backend is up and running!"})

@app.route('/upload_video', methods=['POST'])
def upload_video():
    # ... логика этого эндпоинта остается такой же, как в вашем файле ...
    # Убедитесь, что вы сохраняете `public_id` из `upload_result` в базу данных.
    # Пример:
    # upload_result = cloudinary_service.upload_video_to_cloudinary(...)
    # task_data['cloudinary_public_id'] = upload_result.get('public_id')
    # db_service.add_task(task_data)
    pass # Замените pass на вашу полную логику

@app.route('/task-status/<path:task_id>', methods=['GET'])
def get_task_status(task_id):
    # ... логика этого эндпоинта остается такой же ...
    pass

@app.route('/process_videos', methods=['POST'])
def process_videos():
    # ... логика этого эндпоинта остается такой же ...
    pass

@app.route('/user-videos', methods=['GET'])
def get_user_videos():
    # ... логика этого эндпоинта остается такой же ...
    pass

@app.route('/delete_video/<path:public_id>', methods=['DELETE'])
def delete_video(public_id):
    if not public_id:
        return jsonify({"message": "Public ID is required"}), 400

    logger.info(f"[DELETE] Received delete request for public_id: {public_id}")
    try:
        # 1. Удаляем из Cloudinary
        cloudinary_service.delete_video(public_id)
        
        # 2. Находим и удаляем из БД по public_id
        task = db_service.get_task_by_public_id(public_id) # Предполагается, что вы создадите эту функцию
        if task:
            db_service.delete_task_by_id(task.task_id) # Удаляем по основному ID задачи
        
        return jsonify({"message": f"Video '{public_id}' deleted successfully"}), 200
    except Exception as e:
        logger.error(f"[DELETE] Error deleting video '{public_id}': {e}", exc_info=True)
        return jsonify({"message": f"An error occurred: {str(e)}"}), 500

if __name__ == '__main__':
    # Эта часть не используется на Render.com, но полезна для локального тестирования
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)
