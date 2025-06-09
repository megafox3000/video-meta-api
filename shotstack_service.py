# script/shotstack_service.py
import os
import requests
import json

def create_shotstack_payload(cloudinary_video_url, video_metadata, original_filename, instagram_username, email, linkedin_profile):
    """
    Создает МИНИМАЛЬНЫЙ JSON-payload для запроса к Shotstack API для пошагового дебага.
    """
    # Для минимального теста просто используем базовый текст и короткую длительность.
    # Мы даже не будем использовать Cloudinary URL на этом этапе, чтобы исключить его как источник проблем.

    title_text = "Debug Test" # Простой текст
    duration = 2 # Две секунды

    payload = {
        "timeline": {
            "tracks": [
                {
                    "clips": [
                        {
                            "asset": {
                                "type": "title", # Используем asset type "title"
                                "text": title_text,
                                "style": "minimal",
                                "color": "#FFFFFF",
                                "size": "medium"
                            },
                            "start": 0,
                            "length": duration
                        }
                    ]
                }
            ],
            "background": "#000000" # Черный фон
        },
        "output": {
            "format": "mp4",
            "resolution": "sd", # Самое низкое разрешение
            "aspectRatio": "16:9"
        }
    }

    return payload

def initiate_shotstack_render(cloudinary_video_url, video_metadata, original_filename, instagram_username, email, linkedin_profile):
    """
    Отправляет запрос на рендеринг видео в Shotstack API.
    Возвращает renderId в случае успеха.
    """
    shotstack_api_key = os.environ.get('SHOTSTACK_API_KEY')
    shotstack_render_url = "https://api.shotstack.io/stage/render"

    if not shotstack_api_key:
        raise ValueError("SHOTSTACK_API_KEY environment variable is not set.")

    headers = {
        "Content-Type": "application/json",
        "x-api-key": shotstack_api_key
    }

    # Использование минимального payload.
    # Передаем заглушки вместо реальных данных, так как они не используются в минимальном payload.
    payload = create_shotstack_payload(
        "dummy_url", {}, "dummy_filename", "dummy_user", "dummy@example.com", "dummy_linkedin"
    )

    print(f"[ShotstackService] Sending MINIMAL request to Shotstack API for debug...")
    print(f"[ShotstackService] Shotstack JSON payload: {json.dumps(payload, indent=2)}")

    try:
        response = requests.post(shotstack_render_url, json=payload, headers=headers, timeout=30)
        response.raise_for_status() # Вызовет исключение для 4xx/5xx ошибок

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
    """
    Получает статус рендеринга видео из Shotstack API.
    Возвращает словарь со статусом и URL видео (если завершено).
    """
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
        response.raise_for_status() # Вызовет исключение для 4xx/5xx ошибок

        result = response.json()
        status = result.get('response', {}).get('status')
        url = result.get('response', {}).get('url') # URL финального видео
        error_message = result.get('response', {}).get('message') # Сообщение об ошибке, если есть

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
