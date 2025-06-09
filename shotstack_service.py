# script/shotstack_service.py
import os
import requests
import json

def create_shotstack_payload(cloudinary_video_url, video_metadata, original_filename, instagram_username, email, linkedin_profile):
    """
    Создает JSON-payload для запроса к Shotstack API.
    Теперь с ИСПРАВЛЕННОЙ структурой для 'layers'.
    """
    duration = video_metadata.get('duration', 5.0)
    width = video_metadata.get('width', 0)
    height = video_metadata.get('height', 0)

    # Используем cleaned_username для заголовка
    cleaned_username = "".join(c for c in (instagram_username or '').strip() if c.isalnum() or c in ('_', '-')).strip()
    title_text = f"@{cleaned_username}" if cleaned_username else "Video Analysis"

    # Определяем разрешение и aspectRatio на основе оригинальных метаданных
    output_resolution = "sd" # Default
    aspect_ratio = "16:9" # Default

    if width and height:
        if width >= 1920 or height >= 1920: # Consider anything above 1080p as HD
             output_resolution = "hd"
        elif width >= 1280 or height >= 720: # Consider SD for 720p
             output_resolution = "sd"

        if width > height:
            aspect_ratio = "16:9" # Landscape
        elif height > width:
            aspect_ratio = "9:16" # Portrait
        else:
            aspect_ratio = "1:1" # Square


    payload = {
        "timeline": {
            "tracks": [
                {
                    "clips": [
                        {
                            "asset": {
                                "type": "url",
                                "src": cloudinary_video_url
                            },
                            "length": duration,
                            "start": 0
                        }
                    ],
                    # --- ИСПРАВЛЕНИЕ: 'layers' ТЕПЕРЬ НА УРОВНЕ 'TRACK' ---
                    "layers": [
                        {
                            "asset": {
                                "type": "title",
                                "text": title_text,
                                "style": "minimal",
                                "color": "#FFFFFF",
                                "size": "large"
                            },
                            "start": 0,
                            "length": duration, # Длительность текста равна длительности видео
                            "position": "bottom",
                            "offset": {
                                "y": "-0.2" # Немного выше нижнего края
                            }
                        }
                    ]
                    # --- КОНЕЦ ИСПРАВЛЕНИЯ ---
                }
            ],
            "background": "#000000" # Черный фон
        },
        "output": {
            "format": "mp4",
            "resolution": output_resolution,
            "aspectRatio": aspect_ratio
        }
    }

    return payload

def initiate_shotstack_render(cloudinary_video_url, video_metadata, original_filename, instagram_username, email, linkedin_profile):
    shotstack_api_key = os.environ.get('SHOTSTACK_API_KEY')
    shotstack_render_url = "https://api.shotstack.io/stage/render"

    if not shotstack_api_key:
        raise ValueError("SHOTSTACK_API_KEY environment variable is not set.")

    headers = {
        "Content-Type": "application/json",
        "x-api-key": shotstack_api_key
    }

    payload = create_shotstack_payload(
        cloudinary_video_url,
        video_metadata,
        original_filename,
        instagram_username,
        email,
        linkedin_profile
    )

    print(f"[ShotstackService] Sending request to Shotstack API for {original_filename}...")
    print(f"[ShotstackService] Shotstack JSON payload: {json.dumps(payload, indent=2)}")

    try:
        response = requests.post(shotstack_render_url, json=payload, headers=headers, timeout=30)
        response.raise_for_status()

        result = response.json()
        render_id = result.get('response', {}).get('id')

        if render_id:
            return render_id, "Render successfully queued."
        else:
            print(f"[ShotstackService] ERROR: Shotstack API did not return render ID. Response: {result}")
            raise Exception("Shotstack API did not return render ID.")

    except requests.exceptions.HTTPError as e:
        error_message = f"HTTP Error from Shotstack: {e.response.status_code} {e.response.reason}. Details: {e.response.text}"
        print(f"[ShotstackService] ERROR: {error_message}")
        raise requests.exceptions.RequestException(error_message) from e
    except requests.exceptions.ConnectionError as e:
        error_message = f"Connection Error to Shotstack: {e}"
        print(f"[ShotstackService] ERROR: {error_message}")
        raise requests.exceptions.RequestException(error_message) from e
    except requests.exceptions.Timeout as e:
        error_message = f"Timeout connecting to Shotstack: {e}"
        print(f"[ShotstackService] ERROR: {error_message}")
        raise requests.exceptions.RequestException(error_message) from e
    except Exception as e:
        error_message = f"An unexpected error occurred during Shotstack API call: {e}"
        print(f"[ShotstackService] ERROR: {error_message}")
        raise Exception(error_message) from e

def get_shotstack_render_status(render_id):
    shotstack_api_key = os.environ.get('SHOTSTACK_API_KEY')
    shotstack_status_url = f"https://api.shotstack.io/stage/render/{render_id}"

    if not shotstack_api_key:
        raise ValueError("SHOTSTACK_API_KEY environment variable is not set.")

    headers = {
        "x-api-key": shotstack_api_key
    }

    print(f"[ShotstackService] Checking status for Render ID: {render_id}...")

    try:
        response = requests.get(shotstack_status_url, headers=headers, timeout=15)
        response.raise_for_status()

        result = response.json()
        status = result.get('response', {}).get('status')
        url = result.get('response', {}).get('url')
        error_message = result.get('response', {}).get('message')

        return {
            "status": status,
            "url": url,
            "error_message": error_message
        }

    except requests.exceptions.HTTPError as e:
        error_message = f"HTTP Error from Shotstack status API: {e.response.status_code} {e.response.reason}. Details: {e.response.text}"
        print(f"[ShotstackService] ERROR: {error_message}")
        raise requests.exceptions.RequestException(error_message) from e
    except requests.exceptions.ConnectionError as e:
        error_message = f"Connection Error to Shotstack status API: {e}"
        print(f"[ShotstackService] ERROR: {error_message}")
        raise requests.exceptions.RequestException(error_message) from e
    except requests.exceptions.Timeout as e:
        error_message = f"Timeout connecting to Shotstack status API: {e}"
        print(f"[ShotstackService] ERROR: {error_message}")
        raise requests.exceptions.RequestException(error_message) from e
    except Exception as e:
        error_message = f"An unexpected error occurred during Shotstack status API call: {e}"
        print(f"[ShotstackService] ERROR: {error_message}")
        raise Exception(error_message) from e
