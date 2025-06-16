# app.py
import os
from flask import Flask, request, jsonify, redirect, url_for
from flask_cors import CORS
from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, JSON
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime
import hashlib # Re-added for task_id generation
import time # Re-added for task_id generation
import requests
import json

from services import cloudinary_service # Import our new Cloudinary service
from services import shotstack_service # Import our Shotstack service

app = Flask(__app_id__) # Using __app_id for Flask app name
CORS(app)

# Database configuration
DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    print("DATABASE_URL environment variable not set. Using SQLite for local development.")
    DATABASE_URL = "sqlite:///app_data.db"

connect_args = {}
# For PostgreSQL connections on platforms like Heroku, SSL is often required
if DATABASE_URL.startswith("postgresql://") or DATABASE_URL.startswith("postgres://"):
    # sqlalchemy 2.0+ expects postgresql:// not postgres://
    if DATABASE_URL.startswith("postgres://"):
        DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
    if "sslmode=" not in DATABASE_URL:
        connect_args["sslmode"] = "require"
    # Ensure SSL is correctly handled for Heroku/Render connections
    connect_args["ssl_require"] = True


engine = create_engine(DATABASE_URL, connect_args=connect_args)

Base = declarative_base()
Session = sessionmaker(bind=engine)

# Task model definition
class Task(Base):
    __tablename__ = 'tasks'

    id = Column(Integer, primary_key=True)
    task_id = Column(String(255), unique=True, nullable=False)
    instagram_username = Column(String(255)) # Reverted to instagram_username
    email = Column(String(255))
    linkedin_profile = Column(String(255)) # Reverted to linkedin_profile
    original_filename = Column(String(255))
    status = Column(String(50))
    cloudinary_url = Column(String(500))
    video_metadata = Column(JSON)
    message = Column(Text)
    timestamp = Column(DateTime, default=datetime.now)
    # --- NEW FIELDS FOR SHOTSTACK ---
    shotstackRenderId = Column(String(255)) # ID that Shotstack returns after initiating render
    shotstackUrl = Column(String(500))      # Final URL of the generated video from Shotstack
    posterUrl = Column(String(500), nullable=True) # URL for the poster image from Shotstack
    # --- END NEW FIELDS ---

    def __repr__(self):
        return f"<Task(task_id='{self.task_id}', status='{self.status}')>"

    def to_dict(self):
        return {
            "id": self.id,
            "taskId": self.task_id,
            "instagram_username": self.instagram_username, # Reverted in dict
            "email": self.email,
            "linkedin_profile": self.linkedin_profile, # Reverted in dict
            "originalFilename": self.original_filename,
            "status": self.status,
            "cloudinary_url": self.cloudinary_url,
            "metadata": self.video_metadata,
            "message": self.message,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
            "shotstackRenderId": self.shotstackRenderId,
            "shotstackUrl": self.shotstackUrl,
            "posterUrl": self.posterUrl
        }

def create_tables():
    Base.metadata.create_all(engine)
    print("Database tables created or already exist.")

create_tables()

# ----------- API ENDPOINTS -----------

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

        # Reverted to old keys
        instagram_username = request.form.get('instagram_username')
        email = request.form.get('email')
        linkedin_profile = request.form.get('linkedin_profile')

        # Reverted cleaning logic to use instagram_username
        cleaned_username = "".join(c for c in (instagram_username or '').strip() if c.isalnum() or c in ('_', '-')).strip()
        if not cleaned_username:
            print("[UPLOAD] Instagram username is empty or invalid after cleaning.")
            return jsonify({"error": "Instagram username is required and must be valid."}), 400

        original_filename_base = os.path.splitext(filename)[0]
        # Reverted task_id generation to use hashlib.md5
        unique_hash = hashlib.md5(f"{cleaned_username}/{filename}/{datetime.now().timestamp()}".encode()).hexdigest()
        task_id = f"{cleaned_username}/{original_filename_base}_{unique_hash}" # New, unique task_id

        print(f"[{task_id}] Received upload request for file: '{filename}'")
        print(f"[{task_id}] User data: Instagram='{instagram_username}', Email='{email}', LinkedIn='{linkedin_profile}'")

        # Use the Cloudinary service to upload
        upload_result = cloudinary_service.upload_video(file, cleaned_username, original_filename_base, unique_hash)

        if upload_result and upload_result.get('secure_url'):
            cloudinary_url = upload_result['secure_url']
            print(f"[{task_id}] Cloudinary URL: {cloudinary_url}")

            # Check for essential metadata after upload
            new_upload_duration = upload_result.get('duration', 0)
            new_upload_width = upload_result.get('width', 0)
            new_upload_height = upload_result.get('height', 0)
            new_upload_bytes = upload_result.get('bytes', 0)

            if new_upload_duration <= 0 or new_upload_width <= 0 or new_upload_height <= 0 or new_upload_bytes <= 0:
                print(f"[{task_id}] CRITICAL WARNING: Video uploaded, but essential metadata (duration/resolution/size) is still 0 or missing from Cloudinary response. Full metadata: {upload_result}")
                session.rollback()
                return jsonify({
                    'error': 'Video uploaded but could not retrieve complete and valid metadata from Cloudinary. Please try again or check video file.',
                    'taskId': task_id,
                    'cloudinary_url': cloudinary_url,
                    'metadata': upload_result
                }), 500

            new_task = Task(
                task_id=task_id,
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
            session.commit()
            print(f"[{task_id}] New task successfully created and committed to DB.")
            return jsonify({
                'message': 'Video uploaded and task created.',
                'taskId': new_task.task_id,
                'cloudinary_url': cloudinary_url,
                'metadata': new_task.video_metadata,
                'originalFilename': new_task.original_filename,
                'status': new_task.status
            }), 200
        else:
            print(f"[{task_id}] Cloudinary upload failed: secure_url missing in response.")
            return jsonify({'error': 'Cloudinary upload failed'}), 500

    except SQLAlchemyError as e:
        session.rollback()
        print(f"[{task_id if 'task_id' in locals() else 'N/A'}] Database error during upload: {e}")
        return jsonify({'error': 'Database error', 'details': str(e)}), 500
    except Exception as e:
        session.rollback()
        print(f"[{task_id if 'task_id' in locals() else 'N/A'}] An unexpected error occurred during upload: {e}")
        return jsonify({'error': 'An unexpected error occurred', 'details': str(e)}), 500
    finally:
        session.close()

@app.route('/process_videos', methods=['POST'])
def process_videos():
    session = Session()
    try:
        data = request.json
        task_ids = data.get('task_ids', [])
        connect_videos = data.get('connect_videos', False)

        print(f"[PROCESS_VIDEOS] Received request. Task IDs: {task_ids}, Connect Videos: {connect_videos}")

        if not task_ids:
            print("[PROCESS_VIDEOS] No task IDs provided.")
            return jsonify({"error": "No task IDs provided"}), 400

        valid_tasks = []
        for tid in task_ids:
            task = session.query(Task).filter_by(task_id=tid).first()
            if task and task.cloudinary_url and task.video_metadata and task.status == 'completed':
                valid_tasks.append(task)
            else:
                print(f"[PROCESS_VIDEOS] Skipping task {tid}: not found, missing Cloudinary URL/metadata, or status not 'completed'.")

        if not valid_tasks:
            print("[PROCESS_VIDEOS] No valid tasks found for provided IDs or Cloudinary URLs/metadata missing or not completed.")
            return jsonify({"error": "No valid tasks found for processing (missing or invalid data). Please ensure videos are uploaded and have full metadata."}), 404

        # Extract user info from the first valid task
        # Reverted to old keys
        instagram_username = valid_tasks[0].instagram_username
        email = valid_tasks[0].email
        linkedin_profile = valid_tasks[0].linkedin_profile

        render_id = None
        message = ""
        concatenated_task_id = None

        if connect_videos and len(valid_tasks) >= 2:
            print(f"[PROCESS_VIDEOS] Initiating concatenation for {len(valid_tasks)} videos.")

            cloudinary_video_urls = [t.cloudinary_url for t in valid_tasks]
            all_tasks_metadata = [t.video_metadata for t in valid_tasks]

            # Create a unique filename for the combined video
            combined_filename_base = "_".join([os.path.splitext(t.original_filename)[0] for t in valid_tasks[:3]])
            # Reverted to hashlib for combined filename hash
            combined_filename = f"Combined_{combined_filename_base}_{hashlib.md5(str(time.time()).encode()).hexdigest()[:8]}.mp4"

            render_id, message, poster_url = shotstack_service.initiate_shotstack_render(
                cloudinary_video_url_or_urls=cloudinary_video_urls,
                video_metadata=all_tasks_metadata,
                original_filename=combined_filename,
                instagram_username=instagram_username,
                email=email,
                linkedin_profile=linkedin_profile,
                connect_videos=True
            )

            if render_id:
                # Generate a unique task_id for the concatenated video
                # Reverted to hashlib for concatenated_task_id
                concatenated_task_id = f"concatenated_video_{hashlib.md5(render_id.encode()).hexdigest()}"
                new_concatenated_task = Task(
                    task_id=concatenated_task_id,
                    instagram_username=instagram_username,
                    email=email,
                    linkedin_profile=linkedin_profile,
                    original_filename=combined_filename,
                    status='concatenated_pending',
                    timestamp=datetime.now(),
                    cloudinary_url=None,
                    video_metadata={
                        "combined_from_tasks": [t.task_id for t in valid_tasks],
                        "total_duration": sum(m.get('duration', 0) for m in all_tasks_metadata if m)
                    },
                    message=f"Concatenated video render initiated with ID: {render_id}",
                    shotstackRenderId=render_id,
                    shotstackUrl=None,
                    posterUrl=poster_url
                )
                session.add(new_concatenated_task)
                session.commit()
                print(f"[PROCESS_VIDEOS] Shotstack render initiated for connected videos. New Task ID: {concatenated_task_id}, Render ID: {render_id}")
            else:
                session.rollback()
                print(f"[PROCESS_VIDEOS] Shotstack API did not return a render ID for connected videos. Unexpected. Message: {message}")
                return jsonify({"error": f"Failed to get Shotstack render ID for concatenated video. (Service issue): {message}"}), 500

        else: # Scenario: individual video processing (even if only one is selected)
            # This logic branch is intended for individual video re-processing.
            # If front-end sends 1 video and connect_videos = False, it will be "re-processing"
            # If front-end sends >1 video and connect_videos = False, it will currently trigger this.
            # As per the new architecture, individual processing should be handled by separate requests
            # or a more explicit flow. This placeholder indicates it's not fully implemented for
            # multiple individual renders in a single /process_videos call.
            print("[PROCESS_VIDEOS] Individual video processing not yet implemented in this combined endpoint logic. Please use the appropriate flow for single video processing if needed.")
            return jsonify({
                "message": "Individual video processing not yet implemented.",
                "initiated_tasks": []
            }), 200

        # Unified return for successful concatenation
        if connect_videos and concatenated_task_id:
            return jsonify({
                "message": message,
                "shotstackRenderId": render_id,
                "concatenated_task_id": concatenated_task_id
            }), 200
        elif connect_videos and not concatenated_task_id:
            print("[PROCESS_VIDEOS] Logic error: connect_videos is True but concatenated_task_id is None.")
            return jsonify({"error": "Failed to initiate concatenation due to an internal logic error."}), 500


    except SQLAlchemyError as e:
        session.rollback()
        print(f"[PROCESS_VIDEOS] Database error: {e}")
        return jsonify({"error": "Database error", "details": str(e)}), 500
    except requests.exceptions.RequestException as err:
        session.rollback()
        print(f"[PROCESS_VIDEOS] Network/API Error during Shotstack initiation: {err}")
        return jsonify({"error": f"Error communicating with Shotstack API: {err}", "details": str(err)}), 500
    except Exception as e:
        session.rollback()
        print(f"[PROCESS_VIDEOS] An unexpected error occurred during video processing: {e}")
        return jsonify({"error": "An unexpected server error occurred.", "details": str(e)}), 500
    finally:
        session.close()

@app.route('/task-status/<path:task_id>', methods=['GET'])
def get_task_status(task_id):
    session = Session()
    try:
        print(f"\n[STATUS] Received status request for task_id: '{task_id}'")
        task_info = session.query(Task).filter_by(task_id=task_id).first()

        if not task_info:
            print(f"[STATUS] Task with task_id '{task_id}' NOT FOUND in DB.")
            return jsonify({"message": "Task not found."}), 404

        # Check if the task is related to Shotstack rendering and not yet completed
        if task_info.shotstackRenderId and \
           task_info.status not in ['completed', 'failed', 'concatenated_completed', 'concatenated_failed']:
            print(f"[STATUS] Task {task_info.task_id} has Shotstack render ID. Checking Shotstack API...")
            try:
                # Use the function from shotstack_service to get render status
                status_info = shotstack_service.get_shotstack_render_status(task_info.shotstackRenderId)

                shotstack_status = status_info['status']
                shotstack_url = status_info['url']
                shotstack_poster_url = status_info.get('poster') # Get poster URL
                shotstack_error_message = status_info['error_message']

                print(f"[STATUS] Shotstack render status for {task_info.shotstackRenderId}: {shotstack_status}")

                if shotstack_status == 'done' and shotstack_url:
                    # Update status in our DB based on task type
                    if task_id.startswith('concatenated_video_'):
                        task_info.status = 'concatenated_completed'
                        task_info.message = "Concatenated video rendered successfully."
                    else:
                        task_info.status = 'completed'
                        task_info.message = "Shotstack video rendered successfully."
                    task_info.shotstackUrl = shotstack_url
                    task_info.posterUrl = shotstack_poster_url # Save poster URL
                    session.commit()
                    print(f"[STATUS] Shotstack render completed for {task_id}. URL: {shotstack_url}, Poster: {shotstack_poster_url}")
                elif shotstack_status in ['failed', 'error', 'failed_due_to_timeout']:
                    if task_id.startswith('concatenated_video_'):
                        task_info.status = 'concatenated_failed'
                        task_info.message = f"Concatenated video rendering failed: {shotstack_error_message or 'Unknown Shotstack error'}"
                    else:
                        task_info.status = 'failed'
                        task_info.message = f"Shotstack rendering failed: {shotstack_error_message or 'Unknown Shotstack error'}"
                    session.commit()
                    print(f"[STATUS] Shotstack render failed for {task_id}. Error: {task_info.message}")
                else:
                    # Rendering still in progress, only update message
                    task_info.message = f"Shotstack render in progress: {shotstack_status}"
                    # Don't update posterUrl if it's already set and status is pending,
                    # as a new request might not provide it immediately.
                    if not task_info.posterUrl and shotstack_poster_url:
                        task_info.posterUrl = shotstack_poster_url
                        session.commit() # Commit if posterUrl was updated
                    print(f"[STATUS] Shotstack render still in progress for {task_id}. Status: {shotstack_status}")

                response_data = task_info.to_dict()
                response_data['status'] = task_info.status
                return jsonify(response_data), 200

            except requests.exceptions.RequestException as e:
                print(f"[STATUS] Error querying Shotstack API for {task_info.shotstackRenderId}: {e}")
                task_info.message = f"Error checking Shotstack status: {e}"
                # Do NOT rollback here, we only tried to update the message, not data
                return jsonify(task_info.to_dict()), 200
            except Exception as e:
                print(f"[STATUS] Unexpected error during Shotstack status check for {task_info.shotstackRenderId}: {e}")
                task_info.message = f"Unexpected error during Shotstack status check: {e}"
                return jsonify(task_info.to_dict()), 200

        print(f"[STATUS] Task found in DB: {task_info.task_id}, current_status: {task_info.status}")
        return jsonify(task_info.to_dict()), 200
    except SQLAlchemyError as e:
        session.rollback()
        print(f"[STATUS] Database error fetching task status: {e}")
        return jsonify({"error": "Database error", "details": str(e)}), 500
    except Exception as e:
        session.rollback()
        print(f"[STATUS] An unexpected error occurred in get_task_status: {e}")
        return jsonify({"error": "An unexpected error occurred", "details": str(e)}), 500
    finally:
        session.close()

@app.route('/heavy-tasks/pending', methods=['GET'])
def get_heavy_tasks():
    print("[HEAVY_TASKS] Request for heavy tasks received.")
    return jsonify({"message": "No heavy tasks pending for local worker yet."}), 200

if __name__ == '__main__':
    from waitress import serve
    port = int(os.environ.get('PORT', 8080))
    serve(app, host='0.0.0.0', port=port)
