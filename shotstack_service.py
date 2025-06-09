import os
import requests
import json

# Настройки Shotstack API
SHOTSTACK_API_KEY = os.environ.get('SHOTSTACK_API_KEY')
SHOTSTACK_RENDER_URL = "https://api.shotstack.io/stage/render" # Правильный базовый URL для инициирования рендеринга
SHOTSTACK_OUTPUT_URL = "https://api.shotstack.io/stage/output" # Правильный базовый URL для проверки статуса

if not SHOTSTACK_API_KEY:
    print("[ShotstackService] ERROR: SHOTSTACK_API_KEY environment variable is not set!")
    raise RuntimeError("SHOTSTACK_API_KEY environment variable is not set!")
else:
    # ВРЕМЕННОЕ ЛОГИРОВАНИЕ ДЛЯ ОТЛАДКИ - УДАЛИТЕ ПОСЛЕ ТЕСТОВ
    print(f"[ShotstackService] SHOTSTACK_API_KEY loaded successfully. Length: {len(SHOTSTACK_API_KEY)}, Starts with: {SHOTSTACK_API_KEY[:5]}*****")
# КОНЕЦ ВРЕМЕННОГО ЛОГИРОВАНИЯ


def _get_shotstack_headers():
    """Возвращает стандартные заголовки для запросов Shotstack."""
    return {
        "Content-Type": "application/json",
        "x-api-key": SHOTSTACK_API_KEY
    }

def initiate_shotstack_render(cloudinary_video_url: str, video_metadata: dict,
                              original_filename: str, instagram_username: str = None,
                              email: str = None, linkedin_profile: str = None) -> (str, str):
    # Создаем основной видеоклип
    video_clip = {
        "asset": {
            "type": "url",
            "src": cloudinary_video_url
        },
        "length": video_metadata.get('duration', 5),
        "start": 0
    }

    # Если есть имя пользователя Instagram, создаем текстовый слой
    if instagram_username:
        text_layer = {
            "asset": {
                "type": "title",
                "text": f"@{instagram_username}",
                "style": "minimal",
                "color": "#FFFFFF",
                "size": "large"
            },
            "start": 0,
            "length": video_metadata.get('duration', 5),
            "position": "bottom",
            "offset": { "y": "-0.2" }
        }
        # Добавляем текстовый слой в список слоев видеоклипа
        if "layers" not in video_clip:
            video_clip["layers"] = []
        video_clip["layers"].append(text_layer)


    shotstack_json = {
        "timeline": {
            "tracks": [
                {
                    "clips": [
                        video_clip # Теперь наш video_clip может содержать layers
                    ]
                }
            ],
            "background": "#000000"
        },
        "output": {
            "format": "mp4",
            "resolution": "sd",
            "aspectRatio": "9:16" if video_metadata.get('height', 0) > video_metadata.get('width', 0) else "16:9"
        }
        # Callback секция остается закомментированной, если вы не используете ее
    }


    print(f"[ShotstackService] Sending request to Shotstack API for {original_filename}...")
    print(f"[ShotstackService] Shotstack JSON payload: {json.dumps(shotstack_json, indent=2)}")

    response = requests.post(SHOTSTACK_RENDER_URL,
                             json=shotstack_json,
                             headers=_get_shotstack_headers())
    response.raise_for_status()

    shotstack_result = response.json()
    render_id = shotstack_result.get('response', {}).get('id')

    if render_id:
        return render_id, "Shotstack render initiated successfully."
    else:
        raise Exception(f"Failed to get Shotstack render ID. Response: {shotstack_result}")

def get_shotstack_render_status(render_id: str) -> dict:
    """
    Получает текущий статус рендеринга от Shotstack API.
    """
    if not render_id:
        raise ValueError("Render ID is required to check Shotstack render status.")

    print(f"[ShotstackService] Checking Shotstack render status for ID: {render_id}...")
    response = requests.get(f"{SHOTSTACK_OUTPUT_URL}/{render_id}",
                            headers=_get_shotstack_headers())
    response.raise_for_status()

    shotstack_data = response.json()
    status = shotstack_data.get('response', {}).get('status')
    url = shotstack_data.get('response', {}).get('url')
    error_message = shotstack_data.get('response', {}).get('error')

    return {
        "status": status,
        "url": url,
        "error_message": error_message
    }

# ---
# Пример использования (только для локального тестирования или демонстрации,
# не используется в основной логике Flask app)
# ---
if __name__ == '__main__':
    # Установите эти переменные окружения для локального тестирования
    # os.environ['SHOTSTACK_API_KEY'] = 'YOUR_SHOTSTACK_API_KEY' # РАСКОММЕНТИРУЙТЕ И ВСТАВЬТЕ СВОЙ КЛЮЧ
    # os.environ['DATABASE_URL'] = '...' # Если нужно для других тестов

    print("--- Тестирование shotstack_service.py напрямую ---")

    # Пример вызова initiate_shotstack_render
    # Замените на реальный URL вашего видео из Cloudinary
    test_cloudinary_url = "https://res.cloudinary.com/dcqzpaik8/video/upload/v1749116542/hife_video_analysis/1/AKHO6881.mp4"
    test_video_metadata = {
        "duration": 5.7,
        "width": 720,
        "height": 1280
    }
    test_original_filename = "test_video.mp4"
    test_instagram_username = "my_insta_handle"

    try:
        render_id, message = initiate_shotstack_render(
            cloudinary_video_url=test_cloudinary_url,
            video_metadata=test_video_metadata,
            original_filename=test_original_filename,
            instagram_username=test_instagram_username
        )
        print(f"Инициирован рендеринг Shotstack. Render ID: {render_id}, Message: {message}")

        # Пример проверки статуса (подождите несколько секунд)
        import time
        print("Ожидание 10 секунд перед проверкой статуса...")
        time.sleep(10)

        status_info = get_shotstack_render_status(render_id)
        print(f"Текущий статус рендеринга: {status_info['status']}")
        if status_info['url']:
            print(f"Готовое видео Shotstack URL: {status_info['url']}")
        if status_info['error_message']:
            print(f"Ошибка Shotstack: {status_info['error_message']}")

    except requests.exceptions.RequestException as e:
        print(f"Ошибка HTTP-запроса: {e}")
        if e.response is not None:
            print(f"Ответ сервера: {e.response.text}")
    except Exception as e:
        print(f"Произошла ошибка: {e}")
