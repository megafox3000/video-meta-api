# shotstack_service.py
import os
import requests
import json # Для более чистого форматирования JSON

# Настройки Shotstack API
SHOTSTACK_API_KEY = os.environ.get('SHOTSTACK_API_KEY')
SHOTSTACK_RENDER_URL = "https://api.shotstack.io/stage/render"
SHOTSTACK_OUTPUT_URL = "https://api.shotstack.io/stage/output"

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
    """
    Инициирует рендеринг видео в Shotstack.

    :param cloudinary_video_url: URL видео на Cloudinary для использования в Shotstack.
    :param video_metadata: Метаданные видео из Cloudinary (для длительности и т.д.).
    :param original_filename: Оригинальное имя файла для использования в тексте.
    :param instagram_username: Имя пользователя Instagram.
    :param email: Email пользователя.
    :param linkedin_profile: Профиль LinkedIn пользователя.
    :return: (render_id, message)
    :raises requests.exceptions.RequestException: В случае ошибки HTTP или сетевой ошибки.
    :raises Exception: В случае других непредвиденных ошибок.
    """
    if not cloudinary_video_url:
        raise ValueError("Cloudinary video URL is required to initiate Shotstack render.")

    duration = video_metadata.get('duration', 10) # Длительность видео, по умолчанию 10 секунд
    filename_display = os.path.splitext(original_filename)[0] # Имя файла без расширения

    # Текст для отображения в видео
    user_identifier = instagram_username or email or linkedin_profile or "User"
    title_text = f"Video by {user_identifier}"
    filename_text = f"Original: {filename_display}"

    # Определение оптимального разрешения
    width = video_metadata.get('width', 1920)
    height = video_metadata.get('height', 1080)
    resolution = "sd" # Default
    if width >= 1920 or height >= 1080:
        resolution = "fhd"
    elif width >= 1280 or height >= 720:
        resolution = "hd"

    # Создание JSON-запроса для Shotstack API
    shotstack_json = {
        "timeline": {
            "tracks": [
                {
                    "clips": [
                        {
                            "asset": {
                                "type": "video",
                                "src": cloudinary_video_url,
                                "volume": 1
                            },
                            "start": 0,
                            "length": duration
                        },
                        {
                            "asset": {
                                "type": "title",
                                "text": title_text,
                                "style": "minimal"
                            },
                            "start": 0,
                            "length": min(5, duration), # Показывать заголовок до 5 секунд или меньше, если видео короче
                            "offset": { "x": "0", "y": "0.4" }
                        },
                        {
                            "asset": {
                                "type": "title",
                                "text": filename_text,
                                "style": "minimal"
                            },
                            "start": 0,
                            "length": min(5, duration), # Показывать заголовок до 5 секунд или меньше, если видео короче
                            "offset": { "x": "0", "y": "0.3" } # Немного ниже первого текста
                        }
                    ]
                }
            ]
        },
        "output": {
            "format": "mp4",
            "resolution": resolution,
            "aspectRatio": "16:9" # Или "9:16" для вертикального видео, "1:1" для квадрата, etc.
                                 # Вы можете добавить логику для определения aspectRatio на основе video_metadata
        }
    }

    print(f"[ShotstackService] Sending request to Shotstack API for {original_filename}...")
    # print(f"Shotstack JSON: {json.dumps(shotstack_json, indent=2)}") # Для отладки

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

    :param render_id: ID рендера, полученный от Shotstack.
    :return: Словарь с информацией о статусе (status, url, error_message).
             Пример: {'status': 'done', 'url': '...', 'error_message': None}
             Пример: {'status': 'rendering', 'url': None, 'error_message': None}
             Пример: {'status': 'failed', 'url': None, 'error_message': 'Some error'}
    :raises requests.exceptions.RequestException: В случае ошибки HTTP или сетевой ошибки.
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

if __name__ == '__main__':
    # Пример использования (для тестирования)
    # Запустите: python shotstack_service.py
    # Установите переменную окружения SHOTSTACK_API_KEY перед запуском
    print("Running Shotstack service test...")
    # Необходимо указать реальный Cloudinary URL и метаданные для теста
    test_cloudinary_url = "https://res.cloudinary.com/YOUR_CLOUD_NAME/video/upload/v123456789/your_video_public_id.mp4"
    test_metadata = {"duration": 5, "width": 1280, "height": 720}
    test_filename = "MyAwesomeVideo.mp4"

    try:
        # 1. Инициируем рендеринг
        print("\n--- Initiating Shotstack Render ---")
        render_id, message = initiate_shotstack_render(
            cloudinary_video_url=test_cloudinary_url,
            video_metadata=test_metadata,
            original_filename=test_filename,
            instagram_username="test_user"
        )
        print(f"Render Initiated! Render ID: {render_id}, Message: {message}")

        # 2. Опрашиваем статус (в реальном приложении это будет в цикле или по планировщику)
        print("\n--- Checking Shotstack Render Status (Waiting 5s) ---")
        time.sleep(5) # Ждем немного перед первой проверкой
        status_info = get_shotstack_render_status(render_id)
        print(f"Current Status: {status_info['status']}, URL: {status_info['url']}, Error: {status_info['error_message']}")

        # Можете добавить цикл для ожидания статуса 'done'
        # while status_info['status'] not in ['done', 'failed', 'error']:
        #     print("Still rendering... waiting 5s.")
        #     time.sleep(5)
        #     status_info = get_shotstack_render_status(render_id)
        #     print(f"Current Status: {status_info['status']}, URL: {status_info['url']}, Error: {status_info['error_message']}")

        # if status_info['status'] == 'done':
        #     print(f"\nRender Completed! Final URL: {status_info['url']}")
        # else:
        #     print(f"\nRender Failed! Status: {status_info['status']}, Error: {status_info['error_message']}")

    except ValueError as e:
        print(f"Configuration/Input Error: {e}")
    except requests.exceptions.RequestException as e:
        print(f"Network/API Error: {e}")
        if e.response:
            print(f"Response content: {e.response.text}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
