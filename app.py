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

engine = create_engine(DATABASE_URL)
Base = declarative_base()
Session = sessionmaker(bind=engine)

# Task model definition
class Task(Base):
    __tablename__ = 'tasks'

    id = Column(Integer, primary_key=True) # Automatically generated integer ID
    task_id = Column(String, unique=True, nullable=False) # Your unique string ID (now full Cloudinary path)
    instagram_username = Column(String)
    email = Column(String)
    linkedin_profile = Column(String)
    original_filename = Column(String) # Original filename as uploaded
    status = Column(String) # E.g., 'uploaded', 'processing', 'completed', 'error', 'concatenated'
    cloudinary_url = Column(String)
    video_metadata = Column(JSON) # Store full Cloudinary metadata
    message = Column(Text)
    timestamp = Column(DateTime, default=datetime.now)

    def __repr__(self):
        return f"<Task(task_id='{self.task_id}', status='{self.status}')>"

    def to_dict(self):
        return {
            "taskId": self.task_id, # Use taskId for frontend
            "instagram_username": self.instagram_username,
            "email": self.email,
            "linkedin_profile": self.linkedin_profile,
            "originalFilename": self.original_filename, # Corrected: camelCase for frontend
            "status": self.status,
            "cloudinary_url": self.cloudinary_url, # Ensure this is included
            "metadata": self.video_metadata, # Still "metadata" for frontend compatibility
            "message": self.message,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None
        }

# Function to create database tables
def create_tables():
    Base.metadata.create_all(engine)
    print("Database tables created or already exist.")

# Call table creation function on app startup
create_tables()

# ----------- GPS & METADATA FUNCTIONS (no changes) -----------
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

        # Corrected: Clean username for safe use in Cloudinary path
        # Ensure cleaned_username is not empty and contains only allowed characters
        cleaned_username = "".join(c for c in (instagram_username or '').strip() if c.isalnum() or c in ('_', '-')).strip()
        if not cleaned_username:
            print("[UPLOAD] Instagram username is empty or invalid after cleaning.")
            return jsonify({"error": "Instagram username is required and must be valid."}), 400

        # Corrected: Generate public_id based on original filename and folder
        original_filename_base = os.path.splitext(filename)[0]
        # task_id will now be the full Cloudinary path, including the folder
        full_public_id = f"hife_video_analysis/{cleaned_username}/{original_filename_base}"

        print(f"[{full_public_id}] Received upload request for file: '{filename}'")
        print(f"[{full_public_id}] User data: Instagram='{instagram_username}', Email='{email}', LinkedIn='{linkedin_profile}'")

        # Corrected: Check if a task with this full_public_id already exists in our DB
        existing_task = session.query(Task).filter_by(task_id=full_public_id).first()
        cloudinary_resource_exists = False

        if existing_task:
            print(f"[{full_public_id}] Task with task_id '{full_public_id}' found in DB. Checking Cloudinary...")
            try:
                # Corrected: Try to get resource info from Cloudinary using the full public_id
                resource_info = cloudinary.api.resource(full_public_id, resource_type="video")
                cloudinary_resource_exists = True
                print(f"[{full_public_id}] Resource found on Cloudinary.")
                # If resource found, update the existing task in DB with current Cloudinary data
                existing_task.cloudinary_url = resource_info.get('secure_url')
                existing_task.video_metadata = resource_info # Store full metadata
                existing_task.status = 'completed' # Changed from 'uploaded' to 'completed'
                existing_task.message = 'Video already exists on Cloudinary. DB info updated.'
                existing_task.timestamp = datetime.now() # Update timestamp
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
                existing_task = None # Treat as new upload
            except Exception as e:
                print(f"[{full_public_id}] Error checking Cloudinary resource: {e}. Will re-upload.")
                existing_task = None # On any check error, assume re-upload is needed

        # If task not found in DB OR resource not found on Cloudinary (i.e., upload is needed)
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
                    # Update existing task in DB after successful upload
                    print(f"[{full_public_id}] Updating existing task in DB after upload.")
                    existing_task.instagram_username = instagram_username
                    existing_task.email = email
                    existing_task.linkedin_profile = linkedin_profile
                    existing_task.original_filename = filename
                    existing_task.status = 'completed' # Changed from 'uploaded' to 'completed'
                    existing_task.timestamp = datetime.now()
                    existing_task.cloudinary_url = cloudinary_url
                    existing_task.video_metadata = upload_result
                    existing_task.message = 'Video re-uploaded to Cloudinary and DB info updated.'
                else:
                    # Create a new task in DB
                    print(f"[{full_public_id}] Creating a new task in DB.")
                    new_task = Task(
                        task_id=full_public_id,
                        instagram_username=instagram_username,
                        email=email,
                        linkedin_profile=linkedin_profile,
                        original_filename=filename,
                        status='completed', # Changed from 'uploaded' to 'completed'
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

        video_durations = [] # To store durations for start_offset calculation

        # Step 1: Get metadata for each video to calculate duration
        # We need to fetch each video's duration to correctly calculate start_offset for splicing
        for public_id_full_path in public_ids_from_frontend:
            print(f"[CONCAT] Getting metadata for video: {public_id_full_path}")
            try:
                # Use cloudinary.api.resource to get metadata including duration
                resource = cloudinary.api.resource(public_id_full_path, resource_type="video")
                duration = resource.get('duration', 0)
                video_durations.append(duration)
                print(f"[CONCAT] Duration for {public_id_full_path}: {duration} seconds.")
            except cloudinary.api.NotFound:
                print(f"[CONCAT] Error: Video with public_id {public_id_full_path} not found on Cloudinary.")
                return jsonify({'error': f'Video with public_id {public_id_full_path} not found.'}), 404
            except Exception as e:
                print(f"[CONCAT] Error getting metadata for {public_id_full_path}: {e}")
                return jsonify({'error': f'Error getting metadata for {public_id_full_path}: {str(e)}'}), 500

        # Step 2: Build transformations list for Cloudinary upload
        transformations = []
        # Start with global settings for the concatenated video output
        # For video concatenation, Cloudinary recommends setting video_codec and format
        transformations.append({"video_codec": "auto", "format": "mp4", "quality": "auto"})

        # Add overlays for subsequent videos using 'splice' flag
        current_offset_duration = 0
        for i, public_id_full_path in enumerate(public_ids_from_frontend):
            if i == 0:
                # The first video is the base, no overlay needed for it in the transformation list itself.
                # Its public_id will be passed as the first argument to cloudinary.uploader.upload.
                # We just need to add its duration to the offset for the next video.
                current_offset_duration += video_durations[i]
                continue # Skip adding it as an overlay since it's the base

            transformations.append({
                "overlay": public_id_full_path, # Public ID of the video to overlay
                "flag": "splice", # Flag to concatenate
                "start_offset": f"{current_offset_duration:.2f}", # Start after previous videos
                "resource_type": "video" # Specify resource type for overlay
            })
            current_offset_duration += video_durations[i] # Add current video's duration to offset for next

        print(f"[CONCAT] Generated transformations: {transformations}")

        # Step 3: Upload the concatenated video directly using the first video's public_id as base
        concat_folder = "hife_video_analysis/concatenated"
        # Generate a unique public_id for the new concatenated video
        # Using a hash of selected public IDs and current time for uniqueness
        concat_unique_string = f"concatenated-{'_'.join(public_ids_from_frontend)}-{time.time()}"
        new_concatenated_base_id = hashlib.sha256(concat_unique_string.encode()).hexdigest()[:20]
        new_concatenated_full_public_id = f"{concat_folder}/{new_concatenated_base_id}"
        new_filename = f"concatenated_video_{new_concatenated_base_id}.mp4"

        print(f"[CONCAT] Uploading concatenated video to Cloudinary with new public_id: {new_concatenated_full_public_id}")

        # The crucial change: Pass the first video's public_id and the transformations directly to upload
        # Cloudinary will use the first public_id as the "base" video and apply transformations
        # to splice the others onto it.
        upload_result = cloudinary.uploader.upload(
            public_ids_from_frontend[0], # The public_id of the first video acts as the base
            resource_type="video",
            folder=concat_folder,
            public_id=new_concatenated_base_id, # The base part of the new public_id
            unique_filename=False, # We're managing uniqueness with the hash
            overwrite=True, # Overwrite if a public_id collision occurs (unlikely with hash)
            transformation=transformations # Pass the entire transformation list here
        )
        print(f"[CONCAT] Result of concatenated video upload to Cloudinary: {upload_result}")

        if upload_result and upload_result.get('secure_url'):
            new_video_url = upload_result['secure_url']
            print(f"[CONCAT] New concatenated video URL: {new_video_url}")

            # Create a new task entry for the concatenated video
            new_task = Task(
                task_id=new_concatenated_full_public_id,
                instagram_username=request.form.get('instagram_username', 'concatenated'), # Use a placeholder username
                email=request.form.get('email', 'concatenated@example.com'),
                linkedin_profile=request.form.get('linkedin_profile', 'N/A'),
                original_filename=new_filename,
                status='concatenated', # New status for concatenated videos
                timestamp=datetime.now(),
                cloudinary_url=new_video_url,
                video_metadata=upload_result, # Store full metadata of the concatenated video
                message='Video successfully concatenated.'
            )
            session.add(new_task)
            session.commit()
            print(f"[CONCAT] New concatenated video task created in DB: {new_concatenated_full_public_id}")

            return jsonify({
                'message': 'Videos successfully concatenated.',
                'new_public_id': new_concatenated_full_public_id,
                'new_video_url': new_video_url,
                'metadata': upload_result # Return full metadata for the new video
            }), 200
        else:
            print("[CONCAT] Cloudinary concatenation upload failed: secure_url missing in response.")
            return jsonify({'error': 'Cloudinary concatenation failed', 'details': upload_result}), 500

    except SQLAlchemyError as e:
        session.rollback()
        print(f"[CONCAT] Database error during concatenation: {e}")
        return jsonify({'error': 'Database error', 'details': str(e)}), 500
    except Exception as e:
        session.rollback()
        print(f"[CONCAT] An unexpected error occurred during concatenation: {e}")
        return jsonify({'error': 'An unexpected error occurred during concatenation', 'details': str(e)}), 500
    finally:
        session.close()

if __name__ == '__main__':
    # Use waitress for production deployment
    from waitress import serve
    port = int(os.environ.get('PORT', 8080))
    serve(app, host='0.0.0.0', port=port)
    # For local development, you might use:
    # app.run(debug=True, host='0.0.0.0', port=os.environ.get('PORT', 5000))
