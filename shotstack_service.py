# shotstack_service.py

import os
import requests
import json # Убедитесь, что json импортирован

# Настройки Shotstack API
SHOTSTACK_API_KEY = os.environ.get('SHOTSTACK_API_KEY')
SHOTSTACK_RENDER_URL = "https://api.shotstack.io/stage/render" # Правильный базовый URL для инициирования рендеринга
SHOTSTACK_OUTPUT_URL = "https://api.shotstack.io/stage/output" # Правильный базовый URL для проверки статуса

if not SHOTSTACK_API_KEY:
    raise RuntimeError("SHOTSTACK_API_KEY environment variable is not set!")

def _get_shotstack_headers():
    """Возвращает стандартные заголовки для запросов Shotstack."""
    return {
        "Content-Type": "application/json",
        "x-api-key": SHOTSTACK_API_KEY
    }

def initiate_shotstack_render(cloudinary_video_url: str, video_metadata: dict,
                              original_filename: str, instagram_username: str = None,
                              email: str = None, linkedin_profile: str = None) -> (str, str):
    # Здесь вам нужно будет построить JSON-тело запроса для Shotstack.
    # Этот блок кода должен быть заполнен логикой, которая формирует `shotstack_json`
    # на основе входных параметров.
    # Пример структуры (вам нужно будет адаптировать ее под свои нужды):
    shotstack_json = {
        "timeline": {
            "tracks": [
                {
                    "clips": [
                        {
                            "asset": {
                                "type": "video",
                                "src": cloudinary_video_url
                                # Здесь можно добавить другие параметры видео, если они нужны,
                                # например, обрезка, громкость и т.д.
                            },
                            "length": video_metadata.get('duration', 5), # Длительность видео
                            "start": 0
                        }
                    ]
                }
            ],
            "background": "#000000" # Черный фон по умолчанию
            # Можно добавить другие треки для текста, аудио, изображений и т.д.
        },
        "output": {
            "format": "mp4",
            "resolution": "sd", # Можно изменить на "hd", "full-hd" и т.д.
            "aspectRatio": "9:16" if video_metadata.get('height', 0) > video_metadata.get('width', 0) else "16:9"
        },
        "callback": {
            "url": "YOUR_CALLBACK_URL_HERE", # Опционально: URL для уведомлений о завершении рендеринга
            "data": {
                "taskId": original_filename # Пример передачи данных обратно
            }
        }
    }

    # Если вы хотите добавить текст, например, имя пользователя Instagram:
    if instagram_username:
        text_clip = {
            "asset": {
                "type": "title",
                "text": f"@{instagram_username}",
                "style": "minimal", # Стиль текста, можно настроить
                "color": "#FFFFFF", # Белый цвет
                "size": "large"
            },
            "start": 0,
            "length": video_metadata.get('duration', 5),
            "position": "bottom", # Позиция текста
            "offset": { "y": "-0.2" } # Немного поднять от низа
        }
        # Добавляем новый трек для текста или в существующий трек с видео
        # Для простоты, добавим его в новый трек
        shotstack_json["timeline"]["tracks"].append({
            "clips": [text_clip]
        })


    print(f"[ShotstackService] Sending request to Shotstack API for {original_filename}...")

    # --- ЛОГИРОВАНИЕ JSON-ТЕЛА ЗАПРОСА ---
    # Мы логируем полную JSON-структуру, которая будет отправлена в Shotstack.
    # Это поможет нам отладить ошибку 400 Bad Request.
    print(f"[ShotstackService] Shotstack JSON payload: {json.dumps(shotstack_json, indent=2)}")

    # --- ИЗМЕНЕНИЕ: УДАЛЕН ЛИШНИЙ '/render' ---
    response = requests.post(SHOTSTACK_RENDER_URL,
                             json=shotstack_json,
                             headers=_get_shotstack_headers())
    response.raise_for_status() # Вызывает исключение для 4xx/5xx ошибок

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
    os.environ['SHOTSTACK_API_KEY'] = 'YOUR_SHOTSTACK_API_KEY'
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
