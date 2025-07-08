import os
import cloudinary
from flask import Flask, request, jsonify
from flask_cors import CORS
from datetime import datetime
import hashlib
import time
import requests
import json
import re
import logging

# Импортируем наши новые сервисы
import shotstack_service
import cloudinary_service
import db_service

# --- Configure Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)

# --- CORS Configuration ---
CORS(app, resources={r"/*": {"origins": [
    "https://megafox3000.github.io",
    "http://localhost:5500",
    "http://127.0.0.1:5500"
], "methods": ["GET", "POST", "OPTIONS", "HEAD"], "allow_headers": ["Content-Type", "Authorization", "X-Requested-With"]}}, supports_credentials=True)


# Конфигурация Cloudinary
cloudinary.config(
    cloud_name = os.environ.get('CLOUDINARY_CLOUD_NAME'),
    api_key = os.environ.get('CLOUDINARY_API_KEY'),
    api_secret = os.environ.get('CLOUDINARY_API_SECRET'),
    secure = True
)

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
        headers = {"User-Agent": "VideoMetaApp/1.0"}
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()
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
    task_id = "N/A"
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

        cleaned_username = "".join(c for c in (instagram_username or '').strip() if c.isalnum() or c in ('_', '-')).strip()
        if not cleaned_username:
            logger.warning("[UPLOAD] Instagram username is empty or invalid after cleaning. Using 'anonymous'.")
            cleaned_username = "anonymous"

        original_filename_base = os.path.splitext(filename)[0]
        unique_hash = hashlib.md5(f"{cleaned_username}/{filename}/{datetime.now().timestamp()}".encode()).hexdigest()
        task_id = f"{cleaned_username}/{original_filename_base}_{unique_hash[:8]}"

        logger.info(f"[{task_id}] Received upload request for file: '{filename}'")
        logger.info(f"[{task_id}] User data: Instagram='{instagram_username}', Email='{email}', LinkedIn='{linkedin_profile}'")

        existing_task = db_service.get_task_by_id(task_id) # Используем db_service
        if existing_task:
            logger.info(f"[{task_id}] Task already exists in DB. Overwriting with new upload.")
            pass # Продолжаем, чтобы перезаписать данные Cloudinary

        logger.info(f"[{task_id}] Calling Cloudinary service to upload video...")
        upload_result = cloudinary_service.upload_video_to_cloudinary(
            file_stream=file,
            original_filename=filename,
            instagram_username=instagram_username
        )
        logger.info(f"[{task_id}] Cloudinary service upload successful. Response keys: {upload_result.keys()}")

        cloudinary_url = upload_result['secure_url']
        logger.info(f"[{task_id}] Cloudinary URL: {cloudinary_url}")

        new_upload_duration = upload_result.get('duration', 0)
        new_upload_width = upload_result.get('width', 0)
        new_upload_height = upload_result.get('height', 0)
        new_upload_bytes = upload_result.get('bytes', 0)

        task_status = 'completed'
        task_message = 'Video successfully uploaded to Cloudinary and full metadata obtained.'

        if new_upload_duration <= 0 or new_upload_width <= 0 or new_upload_height <= 0 or new_upload_bytes <= 0:
            logger.warning(f"[{task_id}] WARNING: Video uploaded, but essential metadata (duration/resolution/size) is still 0 or missing from Cloudinary response. Full metadata: {upload_result}")
            task_status = 'cloudinary_metadata_incomplete'
            task_message = 'Video uploaded but could not retrieve complete and valid metadata from Cloudinary.'
        
        task_data = {
            "task_id": task_id,
            "instagram_username": instagram_username,
            "email": email,
            "linkedin_profile": linkedin_profile,
            "original_filename": filename,
            "status": task_status,
            "timestamp": datetime.now(),
            "cloudinary_url": cloudinary_url,
            "video_metadata": upload_result,
            "message": task_message
        }

        if existing_task:
            updated_task = db_service.update_task(existing_task, task_data)
        else:
            new_task = db_service.add_task(task_data)

        logger.info(f"[{task_id}] Task successfully created/updated and committed to DB.")
        return jsonify({
            'message': task_message,
            'taskId': task_id,
            'cloudinary_url': cloudinary_url,
            'metadata': upload_result,
            'originalFilename': filename,
            'status': task_status
        }), 200

    except Exception as e:
        logger.exception(f"[{task_id if 'task_id' in locals() else 'N/A'}] An unexpected error occurred during upload:")
        # Попытка обновить статус задачи до "failed" только если task_id известен
        if 'task_id' in locals() and task_id != "N/A":
            try:
                task_to_update = db_service.get_task_by_id(task_id)
                if task_to_update:
                    db_service.update_task(task_to_update, {
                        "status": "failed",
                        "message": f"Unexpected upload error: {str(e)}"
                    })
            except Exception as db_exc:
                logger.error(f"Failed to update task status in DB after upload error: {db_exc}")
        return jsonify({'error': 'An unexpected error occurred', 'details': str(e)}), 500

@app.route('/task-status/<path:task_id>', methods=['GET'])
def get_task_status(task_id):
    try:
        logger.info(f"[STATUS] Received status request for task_id: '{task_id}'")
        task_info = db_service.get_task_by_id(task_id)

        if not task_info:
            logger.warning(f"[STATUS] Task with task_id '{task_id}' NOT FOUND in DB.")
            return jsonify({"message": "Task not found."}), 404

        if task_info.shotstackRenderId and \
           task_info.status not in ['completed', 'failed', 'concatenated_completed', 'concatenated_failed']:
            logger.info(f"[STATUS] Task {task_info.task_id} has Shotstack render ID. Checking Shotstack API...")
            shotstack_poster_url = None # Инициализируем, чтобы не было UnboundLocalError
            try:
                status_info = shotstack_service.get_shotstack_render_status(task_info.shotstackRenderId)

                shotstack_status = status_info['status']
                shotstack_url = status_info['url']
                shotstack_poster_url = status_info.get('poster')
                shotstack_error_message = status_info['error_message']

                logger.info(f"[STATUS] Shotstack render status for {task_info.shotstackRenderId}: {shotstack_status}")

                updates = {}
                if shotstack_status == 'done' and shotstack_url:
                    if task_id.startswith('concatenated_video_'):
                        updates['status'] = 'concatenated_completed'
                        updates['message'] = "Concatenated video rendered successfully."
                    else:
                        updates['status'] = 'completed'
                        updates['message'] = "Shotstack video rendered successfully."
                    updates['shotstackUrl'] = shotstack_url
                    updates['posterUrl'] = shotstack_poster_url
                    logger.info(f"[STATUS] Shotstack render completed for {task_id}. URL: {shotstack_url}")
                    if shotstack_poster_url:
                        logger.info(f"[STATUS] Shotstack Poster URL: {shotstack_poster_url}")
                elif shotstack_status in ['failed', 'error', 'failed_due_to_timeout']:
                    if task_id.startswith('concatenated_video_'):
                        updates['status'] = 'concatenated_failed'
                        updates['message'] = f"Concatenated video rendering failed: {shotstack_error_message or 'Unknown Shotstack error'}"
                    else:
                        updates['status'] = 'failed'
                        updates['message'] = f"Shotstack rendering failed: {shotstack_error_message or 'Unknown Shotstack error'}"
                    updates['shotstackUrl'] = None
                    updates['posterUrl'] = None
                    logger.error(f"[STATUS] Shotstack render failed for {task_id}. Error: {updates['message']}")
                else:
                    updates['message'] = f"Shotstack render in progress: {shotstack_status}"
                    logger.info(f"[STATUS] Shotstack render still in progress for {task_id}. Status: {shotstack_status}")
                
                if updates: # Обновляем, только если есть изменения
                    task_info = db_service.update_task(task_info, updates)

                response_data = task_info.to_dict()
                response_data['status'] = task_info.status # Ensure status is always up-to-date
                if shotstack_poster_url:
                    response_data['posterUrl'] = shotstack_poster_url
                return jsonify(response_data), 200

            except requests.exceptions.RequestException as e:
                logger.error(f"[STATUS] Error querying Shotstack API for {task_info.shotstackRenderId}: {e}")
                updates = {"message": f"Error checking Shotstack status: {e}"}
                db_service.update_task(task_info, updates)
                response_data = task_info.to_dict()
                if shotstack_poster_url:
                    response_data['posterUrl'] = shotstack_poster_url
                return jsonify(response_data), 200
            except Exception as e:
                logger.exception(f"[STATUS] Unexpected error during Shotstack status check for {task_info.shotstackRenderId}:")
                updates = {"message": f"Unexpected error during Shotstack status check: {e}"}
                db_service.update_task(task_info, updates)
                response_data = task_info.to_dict()
                if shotstack_poster_url:
                    response_data['posterUrl'] = shotstack_poster_url
                return jsonify(response_data), 200

        logger.info(f"[STATUS] Task found in DB: {task_info.task_id}, current_status: {task_info.status}")
        return jsonify(task_info.to_dict()), 200
    except Exception as e:
        logger.exception(f"[STATUS] An unexpected error occurred in get_task_status:")
        return jsonify({"error": "An unexpected server error occurred", "details": str(e)}), 500

# NEW: Endpoint for concatenated video status, reusing existing logic
@app.route('/concatenated-video-status/<path:task_id>', methods=['GET'])
def get_concatenated_video_status(task_id):
    """
    Returns the current status of a concatenated video.
    This endpoint reuses the logic from `get_task_status` as it already handles
    checking Shotstack status and database updates for both individual and
    concatenated video tasks (which are identified by their 'concatenated_video_' prefix).
    """
    logger.info(f"[CONCATENATED_STATUS] Received request for concatenated video status: '{task_id}'. Delegating to general task status check.")
    return get_task_status(task_id) # Просто вызываем существующую функцию

@app.route('/generate-shotstack-video', methods=['POST'])
def generate_shotstack_video():
    try:
        data = request.get_json()
        task_id = data.get('taskId')

        if not task_id:
            logger.warning("[SHOTSTACK] No taskId provided for Shotstack generation.")
            return jsonify({"error": "No taskId provided"}), 400

        task = db_service.get_task_by_id(task_id)
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
            db_service.update_task(task, {
                "status": 'shotstack_pending',
                "message": f"Shotstack render initiated with ID: {render_id}",
                "shotstackRenderId": render_id
            })
            return jsonify({
                "message": "Shotstack render initiated successfully.",
                "shotstackRenderId": render_id
            }), 200
        else:
            logger.error(f"[SHOTSTACK] Shotstack API did not return a render ID for task {task_id}. Unexpected. Message: {message}")
            return jsonify({"error": "Failed to get Shotstack render ID. (Service issue)", "details": message}), 500

    except Exception as e:
        logger.exception(f"[SHOTSTACK] An unexpected error occurred during Shotstack generation for task {task_id}:")
        # Попытка обновить статус задачи до "failed" только если task_id известен
        if 'task_id' in locals() and task_id != "N/A":
            try:
                task_to_update = db_service.get_task_by_id(task_id)
                if task_to_update:
                    db_service.update_task(task_to_update, {
                        "status": "failed",
                        "message": f"Unexpected Shotstack generation error: {str(e)}"
                    })
            except Exception as db_exc:
                logger.error(f"Failed to update task status in DB after Shotstack error: {db_exc}")
        return jsonify({"error": "An unexpected server error occurred.", "details": str(e)}), 500

@app.route('/process_videos', methods=['POST'])
def process_videos():
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
            task = db_service.get_task_by_id(tid)
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

            combined_filename_base = "_".join([t.original_filename.split('.')[0] for t in valid_tasks[:3]])
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
                new_concatenated_task_data = {
                    "task_id": concatenated_task_id,
                    "instagram_username": instagram_username,
                    "email": email,
                    "linkedin_profile": linkedin_profile,
                    "original_filename": combined_filename,
                    "status": 'concatenated_pending',
                    "timestamp": datetime.now(),
                    "cloudinary_url": None,
                    "video_metadata": {
                        "combined_from_tasks": [t.task_id for t in valid_tasks],
                        "total_duration": sum(m.get('duration', 0) for m in all_tasks_metadata if m)
                    },
                    "message": f"Concatenated video render initiated with ID: {render_id}",
                    "shotstackRenderId": render_id,
                    "shotstackUrl": None,
                    "posterUrl": None
                }
                db_service.add_task(new_concatenated_task_data)
                logger.info(f"[PROCESS_VIDEOS] Shotstack render initiated for connected videos. New Task ID: {concatenated_task_id}, Render ID: {render_id}")
            else:
                logger.error(f"[PROCESS_VIDEOS] Shotstack API did not return a render ID for connected videos. Unexpected.")
                return jsonify({"error": "Failed to get Shotstack render ID for concatenated video. (Service issue)"}), 500

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
                        db_service.update_task(task, {
                            "shotstackRenderId": render_id_single,
                            "status": 'shotstack_pending',
                            "message": f"Shotstack render initiated with ID: {render_id_single}"
                        })
                        logger.info(f"[PROCESS_VIDEOS] Shotstack render initiated for single video {task.original_filename}. Render ID: {render_id_single}")
                        initiated_tasks_info.append({
                            "taskId": task.task_id,
                            "shotstackRenderId": render_id_single,
                            "message": message_single
                        })
                    else:
                        logger.error(f"[PROCESS_VIDEOS] Shotstack API did not return a render ID for single video {task.task_id}. Unexpected.")
                        initiated_tasks_info.append({
                            "taskId": task.task_id,
                            "error": "Failed to get Shotstack render ID for single video."
                        })
                except Exception as e:
                    logger.exception(f"[PROCESS_VIDEOS] Error initiating Shotstack for task {task.task_id}:")
                    initiated_tasks_info.append({
                        "taskId": task.task_id,
                        "error": str(e)
                    })
            
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


    except Exception as e:
        logger.exception(f"[PROCESS_VIDEOS] An unexpected error occurred during video processing:")
        return jsonify({"error": "An unexpected server error occurred.", "details": str(e)}), 500

@app.route('/heavy-tasks/pending', methods=['GET'])
def get_heavy_tasks():
    logger.info("[HEAVY_TASKS] Request for heavy tasks received.")
    return jsonify({"message": "No heavy tasks pending for local worker yet."}), 200

@app.route('/user-videos', methods=['GET'])
def get_user_videos():
    # 1. Получаем идентификаторы из запроса
    instagram_username = request.args.get('instagram_username')
    email = request.args.get('email')
    linkedin_profile = request.args.get('linkedin_profile')

    if not any([instagram_username, email, linkedin_profile]):
        return jsonify({"error": "Please provide an identifier"}), 400

    try:
        # 2. Получаем список словарей из сервиса БД
        tasks_from_db = db_service.get_user_videos(
            instagram_username=instagram_username,
            email=email,
            linkedin_profile=linkedin_profile
        )

        verified_tasks = []
        tasks_to_delete_ids = []

        # 3. Проверяем каждую задачу (которая теперь является словарем)
        for task_dict in tasks_from_db:
            # ИСПРАВЛЕНО: Доступ к данным по ключу словаря (в camelCase)
            public_id = task_dict.get('cloudinaryPublicId')
            
            video_exists = cloudinary_service.check_video_existence(public_id)
            
            if video_exists:
                # ИСПРАВЛЕНО: Добавляем в список сам словарь, так как он уже готов
                verified_tasks.append(task_dict)
            else:
                # Если видео НЕ существует, помечаем его на удаление из нашей БД
                logger.warning(f"Видео для задачи {task_dict.get('taskId')} не найдено в Cloudinary. Помечаем на удаление из БД.")
                tasks_to_delete_ids.append(task_dict.get('id'))

        # 4. Удаляем "мертвые" записи из нашей БД
        if tasks_to_delete_ids:
            logger.info(f"Удаление {len(tasks_to_delete_ids)} несуществующих записей из БД...")
            for task_id in tasks_to_delete_ids:
                if task_id: # Дополнительная проверка, что ID не None
                    db_service.delete_task_by_id(task_id)

        # 5. Возвращаем фронтенду только проверенный, "чистый" список видео
        return jsonify(verified_tasks), 200

    except Exception as e:
        logger.error(f"[USER_VIDEOS] Ошибка при получении и проверке видео: {e}", exc_info=True)
        return jsonify({"error": "An unexpected server error occurred"}), 500
        
if __name__ == '__main__':
    from waitress import serve
    port = int(os.environ.get('PORT', 8080))
    serve(app, host='0.0.0.0', port=port)
