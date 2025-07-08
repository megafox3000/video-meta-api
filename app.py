# app.py
import os
import cloudinary
from flask import Flask, request, jsonify
from flask_cors import CORS
import db_service
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

# ИНИЦИАЛИЗАЦИЯ БАЗЫ ДАННЫХ ПРИ СТАРТЕ ПРИЛОЖЕНИЯ
with app.app_context():
    db_service.create_tables()

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
# This section contains all the route definitions for the Flask application.

@app.route('/')
def index():
    """
    A simple health-check endpoint to confirm the server is running.
    """
    logger.info("Root path '/' was requested.")
    return jsonify({"status": "✅ Python Backend is up and running!"})


@app.route('/upload_video', methods=['POST'])
def upload_video():
    """
    Handles video file uploads. It uploads the file to Cloudinary,
    then creates a corresponding task record in the database.
    """
    try:
        # --- File and Form Validation ---
        if 'video' not in request.files:
            logger.warning("[UPLOAD] No video file provided in request.")
            return jsonify({"error": "No video file provided"}), 400

        file = request.files['video']
        if file.filename == '':
            logger.warning("[UPLOAD] No selected video file.")
            return jsonify({"error": "No selected video file"}), 400

        instagram_username = request.form.get('instagram_username')
        email = request.form.get('email')
        linkedin_profile = request.form.get('linkedin_profile')

        if not any([instagram_username, email, linkedin_profile]):
             return jsonify({"error": "At least one identifier (Instagram, Email, etc.) is required"}), 400

        # --- Cloudinary Upload ---
        logger.info(f"Calling Cloudinary service for file: '{file.filename}'")
        upload_result = cloudinary_service.upload_video_to_cloudinary(
            file_stream=file,
            original_filename=file.filename,
            instagram_username=instagram_username
        )
        
        # --- Database Task Creation ---
        # Generate a unique task_id for our system
        task_id = f"{instagram_username or 'anon'}/{os.path.splitext(file.filename)[0]}_{upload_result.get('asset_id', 'xxxx')[-8:]}"
        
        # Prepare the data for the new database record
        task_data = {
            "task_id": task_id,
            "cloudinary_public_id": upload_result.get('public_id'),
            "instagram_username": instagram_username,
            "email": email,
            "linkedin_profile": linkedin_profile,
            "original_filename": file.filename,
            "status": 'completed',
            "cloudinary_url": upload_result.get('secure_url'),
            "video_metadata": upload_result,
            "message": "Video uploaded successfully."
        }

        # Add the new task to the database. db_service now returns a dictionary.
        new_task_dict = db_service.add_task(task_data)
        logger.info(f"Task '{task_id}' successfully created in DB.")
        
        # Return the newly created task data (already in dict format) to the frontend
        return jsonify(new_task_dict), 201

    except Exception as e:
        logger.exception(f"An unexpected error occurred during upload:")
        return jsonify({'error': 'An unexpected server error occurred', 'details': str(e)}), 500


@app.route('/delete_video/<path:public_id>', methods=['DELETE'])
def delete_video(public_id):
    """
    Deletes a video from Cloudinary and its corresponding record from the database.
    It uses the Cloudinary public_id as the primary identifier for the resource.
    """
    if not public_id:
        return jsonify({"message": "Public ID is required"}), 400

    logger.info(f"[DELETE] Request for public_id: {public_id}")
    try:
        # Step 1: Delete the resource from Cloudinary
        cloudinary_service.delete_video(public_id)
        
        # Step 2: Find the task in our database using the public_id
        task_object = db_service.get_task_by_public_id(public_id)
        
        # Step 3: If a corresponding task is found, delete it from our database
        if task_object:
            db_service.delete_task_by_id(task_object.id)
        else:
            logger.warning(f"Video with public_id '{public_id}' was deleted from Cloudinary, but no matching task was found in the DB.")
        
        # Return 204 No Content, which is the standard for a successful DELETE request
        return ('', 204)
        
    except Exception as e:
        logger.error(f"[DELETE] Error deleting video '{public_id}': {e}", exc_info=True)
        return jsonify({"message": f"An error occurred: {str(e)}"}), 500


@app.route('/task-status/<path:task_id>', methods=['GET'])
def get_task_status(task_id):
    """
    Retrieves the status of a specific task. If the task is being processed by Shotstack,
    it polls the Shotstack API for the latest status and updates the database.
    """
    try:
        logger.info(f"[STATUS] Request for task_id: '{task_id}'")
        # CHANGED: db_service.get_task_by_id now returns a dictionary or None
        task_dict = db_service.get_task_by_id(task_id)

        if not task_dict:
            logger.warning(f"[STATUS] Task '{task_id}' NOT FOUND in DB.")
            return jsonify({"message": "Task not found."}), 404

        # Check if we need to poll Shotstack for an update
        render_id = task_dict.get('shotstackRenderId')
        current_status = task_dict.get('status')
        
        if render_id and current_status not in ['completed', 'failed', 'concatenated_completed', 'concatenated_failed']:
            logger.info(f"[STATUS] Task {task_id} has a Shotstack render ID. Checking API...")
            
            status_info = shotstack_service.get_shotstack_render_status(render_id)
            shotstack_status = status_info.get('status')
            
            updates = {}
            # Logic to determine if the status has changed based on Shotstack's response
            if shotstack_status == 'done':
                updates['status'] = 'concatenated_completed' if task_id.startswith('concatenated_') else 'completed'
                updates['message'] = "Render completed successfully."
                updates['shotstackUrl'] = status_info.get('url')
                updates['posterUrl'] = status_info.get('poster')
            elif shotstack_status in ['failed', 'error']:
                updates['status'] = 'concatenated_failed' if task_id.startswith('concatenated_') else 'failed'
                updates['message'] = status_info.get('error_message', 'Render failed in Shotstack.')
            
            # If there are changes, update the database
            if updates:
                logger.info(f"Updating task {task_id} with new status: {updates.get('status')}")
                # The update function returns the updated dictionary
                task_dict = db_service.update_task_by_id(task_id, updates)

        # Return the latest task data (either original or updated)
        return jsonify(task_dict), 200

    except Exception as e:
        logger.exception(f"[STATUS] An unexpected error occurred in get_task_status:")
        return jsonify({"error": "An unexpected server error occurred", "details": str(e)}), 500


@app.route('/concatenated-video-status/<path:task_id>', methods=['GET'])
def get_concatenated_video_status(task_id):
    """An alias route that delegates to the main get_task_status function."""
    return get_task_status(task_id)


@app.route('/generate-shotstack-video', methods=['POST'])
def generate_shotstack_video():
    """
    Initiates a render process for a single video in Shotstack.
    This is typically for adding effects or overlays, not for concatenation.
    """
    try:
        data = request.get_json()
        if not data or 'taskId' not in data:
            return jsonify({"error": "taskId is required"}), 400
            
        task_id = data['taskId']

        task_dict = db_service.get_task_by_id(task_id)
        if not task_dict:
            return jsonify({"error": "Task not found."}), 404

        if not task_dict.get('cloudinaryUrl'):
            return jsonify({"error": "Video URL is missing."}), 400
        
        if task_dict.get('status') == 'shotstack_pending' and task_dict.get('shotstackRenderId'):
            return jsonify({
                "message": "Shotstack render already in progress.",
                "shotstackRenderId": task_dict.get('shotstackRenderId')
            }), 200

        render_id, message = shotstack_service.initiate_shotstack_render(
            cloudinary_video_url_or_urls=task_dict.get('cloudinaryUrl'),
            video_metadata=task_dict.get('videoMetadata', {}),
            original_filename=task_dict.get('originalFilename'),
            instagram_username=task_dict.get('instagramUsername'),
            email=task_dict.get('email'),
            linkedin_profile=task_dict.get('linkedinProfile'),
            connect_videos=False
        )

        db_service.update_task_by_id(task_id, {
            "status": 'shotstack_pending',
            "message": f"Shotstack render initiated with ID: {render_id}",
            "shotstackRenderId": render_id
        })
        return jsonify({
            "message": "Shotstack render initiated successfully.",
            "shotstackRenderId": render_id
        }), 200

    except Exception as e:
        logger.exception(f"[SHOTSTACK] An unexpected error occurred:")
        return jsonify({"error": "An unexpected server error occurred.", "details": str(e)}), 500


@app.route('/process_videos', methods=['POST'])
def process_videos():
    """
    Processes a batch of videos. It can either initiate individual renders for all of them
    or concatenate multiple videos into one.
    """
    try:
        data = request.json
        task_ids = data.get('task_ids', [])
        connect_videos = data.get('connect_videos', False)

        if not task_ids:
            return jsonify({"error": "No task IDs provided"}), 400

        # --- Validate and collect tasks ---
        valid_tasks_dicts = []
        for tid in task_ids:
            task_dict = db_service.get_task_by_id(tid)
            if task_dict and task_dict.get('cloudinaryUrl') and task_dict.get('status') == 'completed':
                valid_tasks_dicts.append(task_dict)
            else:
                logger.warning(f"[PROCESS_VIDEOS] Skipping task {tid}: not found or status not 'completed'.")

        if not valid_tasks_dicts:
            return jsonify({"error": "No valid tasks found for processing."}), 404

        # --- Concatenation Logic ---
        if connect_videos:
            if len(valid_tasks_dicts) < 2:
                return jsonify({"error": "At least two videos are required to concatenate."}), 400
            
            logger.info(f"Initiating concatenation for {len(valid_tasks_dicts)} videos.")
            
            cloudinary_video_urls = [t.get('cloudinaryUrl') for t in valid_tasks_dicts]
            all_tasks_metadata = [t.get('videoMetadata') for t in valid_tasks_dicts]
            
            render_id, _ = shotstack_service.initiate_shotstack_render(
                cloudinary_video_url_or_urls=cloudinary_video_urls,
                video_metadata=all_tasks_metadata,
                connect_videos=True,
                instagram_username=data.get('instagram_username'),
                email=data.get('email'),
                linkedin_profile=data.get('linkedin_profile')
            )
            
            concatenated_task_id = f"concatenated_video_{render_id}"
            db_service.add_task({
                "task_id": concatenated_task_id,
                "status": 'concatenated_pending',
                "shotstackRenderId": render_id,
                "video_metadata": {"source_tasks": task_ids},
                "instagram_username": data.get('instagram_username'),
                "email": data.get('email'),
                "linkedin_profile": data.get('linkedin_profile')
            })
            
            return jsonify({
                "message": "Video concatenation initiated.",
                "concatenatedTaskId": concatenated_task_id,
                "shotstackRenderId": render_id
            }), 200
        
        # --- Individual Processing Logic (If not concatenating) ---
        else:
            # This part can be implemented if needed, following the same pattern
            logger.warning("Individual processing for multiple videos is not yet fully implemented.")
            return jsonify({"message": "Individual processing not implemented"}), 501

    except Exception as e:
        logger.exception(f"[PROCESS_VIDEOS] An unexpected error occurred:")
        return jsonify({"error": "An unexpected server error occurred.", "details": str(e)}), 500


@app.route('/heavy-tasks/pending', methods=['GET'])
def get_heavy_tasks():
    """Placeholder for a potential local worker system."""
    logger.info("[HEAVY_TASKS] Request for heavy tasks received.")
    return jsonify({"message": "No heavy tasks pending for local worker yet."}), 200


@app.route('/user-videos', methods=['GET'])
def get_user_videos():
    """
    Retrieves all videos for a user and performs a self-cleanup by checking
    if the videos still exist in Cloudinary before returning them.
    """
    try:
        instagram_username = request.args.get('instagram_username')
        email = request.args.get('email')
        linkedin_profile = request.args.get('linkedin_profile')

        if not any([instagram_username, email, linkedin_profile]):
            return jsonify({"error": "Please provide an identifier"}), 400
        
        tasks_from_db = db_service.get_user_videos(
            instagram_username=instagram_username,
            email=email,
            linkedin_profile=linkedin_profile
        )

        verified_tasks = []
        tasks_to_delete_ids = []

        for task_dict in tasks_from_db:
            public_id = task_dict.get('cloudinaryPublicId')
            
            if cloudinary_service.check_video_existence(public_id):
                verified_tasks.append(task_dict)
            else:
                logger.warning(f"Video for task {task_dict.get('taskId')} (public_id: {public_id}) not found in Cloudinary. Marking for deletion.")
                tasks_to_delete_ids.append(task_dict.get('id'))

        if tasks_to_delete_ids:
            logger.info(f"Deleting {len(tasks_to_delete_ids)} orphaned records from DB...")
            for task_primary_key in tasks_to_delete_ids:
                if task_primary_key:
                    db_service.delete_task_by_id(task_primary_key)

        return jsonify(verified_tasks), 200

    except Exception as e:
        logger.error(f"[USER_VIDEOS] Error during video fetch and verification: {e}", exc_info=True)
        return jsonify({"error": "An unexpected server error occurred"}), 500

if __name__ == '__main__':
    from waitress import serve
    port = int(os.environ.get('PORT', 8080))
    serve(app, host='0.0.0.0', port=port)
