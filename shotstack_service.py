import os
import requests
import json
import random

# Определяем список доступных переходов Shotstack
AVAILABLE_TRANSITIONS = [
    "fade",
    "slideLeft",
    "slideRight",
    "wipeLeft",
    "wipeRight",
    "dissolve"
]

def create_shotstack_payload(cloudinary_video_url_or_urls, video_metadata_list, original_filename, instagram_username, email, linkedin_profile, connect_videos=False):
    """
    Создает JSON-payload для запроса к Shotstack API.
    Поддерживает объединение нескольких видео путем добавления нескольких клипов в одну дорожку,
    добавляет текстовые наложения, случайные переходы и использует кастомный шрифт.

    :param cloudinary_video_url_or_urls: Список URL Cloudinary видео (если connect_videos=True)
                                         Или одиночный URL (если connect_videos=False)
    :param video_metadata_list: Список метаданных для КАЖДОГО видео (когда connect_videos=True),
                                 или метаданные одного видео (когда connect_videos=False).
                                 Должен содержать 'duration', 'width', 'height' для каждого.
    :param original_filename: Имя файла (для логирования/названия)
    :param instagram_username: Имя пользователя Instagram для наложения
    :param email: Email пользователя.
    :param linkedin_profile: Профиль LinkedIn пользователя.
    :param connect_videos: Флаг, указывающий, нужно ли объединять видео.
    """
    if not isinstance(video_metadata_list, list):
        processed_metadata_list = [video_metadata_list]
    else:
        processed_metadata_list = video_metadata_list

    total_duration = sum(m.get('duration', 0) for m in processed_metadata_list if m)
    # Гарантируем минимальную длительность, если видео слишком короткое или пустое
    if total_duration < 0.1: 
        total_duration = 0.1 

    first_video_metadata = processed_metadata_list[0] if processed_metadata_list else {}
    width = first_video_metadata.get('width', 1920)
    height = first_video_metadata.get('height', 1080)

    cleaned_username = "".join(c for c in (instagram_username or '').strip() if c.isalnum() or c in ('_', '-')).strip()
    username_display_text = f"@{cleaned_username}" if cleaned_username else "Video Analysis"

    output_resolution = "sd"
    aspect_ratio = "16:9"

    if width >= 1920 or height >= 1080:
        output_resolution = "hd"
    elif width >= 1280 or height >= 720:
        output_resolution = "sd"

    if width > height:
        aspect_ratio = "9:16" # Vertical orientation for mobile
    elif height > width:
        aspect_ratio = "9:16" # Vertical orientation for mobile
    else:
        aspect_ratio = "1:1" # Square

    video_clips = []
    current_start_time = 0.0

    if connect_videos and isinstance(cloudinary_video_url_or_urls, list):
        for i, url in enumerate(cloudinary_video_url_or_urls):
            clip_metadata = processed_metadata_list[i] if i < len(processed_metadata_list) else {}
            clip_duration = clip_metadata.get('duration', 5.0)
            
            clip_definition = {
                "asset": {
                    "type": "video",
                    "src": url
                },
                "start": current_start_time,
                "length": clip_duration
            }
            
            if i > 0:
                random_in_transition = random.choice(AVAILABLE_TRANSITIONS)
                clip_definition["transition"] = {"in": random_in_transition}
                print(f"[ShotstackService] Added 'in' transition: '{random_in_transition}' for clip {i+1}.")
            
            video_clips.append(clip_definition)
            current_start_time += clip_duration
    else:
        single_video_duration = processed_metadata_list[0].get('duration', 5.0) if processed_metadata_list else 5.0
        video_clips.append({
            "asset": {
                "type": "video",
                "src": cloudinary_video_url_or_urls
            },
            "length": single_video_duration,
            "start": 0
        })
        total_duration = single_video_duration

    payload = {
        "timeline": {
            "fonts": [
                {
                    "src": "https://shotstack-assets.s3.amazonaws.com/fonts/Cousine.ttf" 
                }
            ],
            "tracks": [
                {   # ДОРОЖКА 1: ДЛЯ ТЕКСТА - INDEX 0 (как вы просили)
                    "clips": [] 
                },
                {   # ДОРОЖКА 2: ДЛЯ ВИДЕОКЛИПОВ - INDEX 1 (как вы просили)
                    "clips": video_clips 
                }
            ],
            "background": "#000000"
        },
        "output": {
            "format": "mp4",
            "resolution": output_resolution,
            "aspectRatio": aspect_ratio,
            "poster": {
                "capture": 1
            }
        }
    }

    # Добавляем текстовые наложения на дорожку 0
    if connect_videos:
        # Текст для объединенного видео (вверху)
        payload["timeline"]["tracks"][0]["clips"].append({ 
            "asset": {
                "type": "text",
                "text": "COMBINED VIDEO", 
                "font": {
                    "family": "Cousine", 
                    "color": "#FFFFFF",
                    "size": 70
                },
                "alignment": { "horizontal": "center", "vertical": "top" },
                "width": 1280,
                "height": 150,
                "effect": "zoomIn"
            },
            "start": 0,
            "length": "end", 
            "position": "top",
            "offset": { "y": "0.1" }
        })

    # Добавляем имя пользователя (или общий текст) внизу
    payload["timeline"]["tracks"][0]["clips"].append({ 
        "asset": {
            "type": "text",
            "text": username_display_text,
            "font": {
                "family": "Cousine", 
                "color": "#FFFFFF",
                "size": 40
            },
            "alignment": { "horizontal": "center", "vertical": "bottom" },
            "width": 960,
            "height": 100
        },
        "start": 0,
        "length": "end", 
        "position": "bottom",
        "offset": { "y": "-0.1" }
    })

    return payload

def initiate_shotstack_render(cloudinary_video_url_or_urls, video_metadata, original_filename, instagram_username, email, linkedin_profile, connect_videos=False):
    """
    Отправляет запрос на рендеринг видео в Shotstack API.
    Теперь принимает один URL или список URL для объединения.
    """
    shotstack_api_key = os.environ.get('SHOTSTACK_API_KEY')
    shotstack_render_url = "https://api.shotstack.io/stage/render" # Используем stage для тестирования

    if not shotstack_api_key:
        print("[ShotstackService] ОШИБКА: Переменная окружения SHOTSTACK_API_KEY не установлена.")
        raise ValueError("SHOTSTACK_API_KEY environment variable is not set.")

    headers = {
        "Content-Type": "application/json",
        "x-api-key": shotstack_api_key
    }

    if connect_videos and not isinstance(video_metadata, list):
        print("[ShotstackService] ПРЕДУПРЕЖДЕНИЕ: connect_videos равно True, но video_metadata не является списком. Это может привести к некорректному рендерингу.")
        video_metadata_for_payload = [video_metadata] if video_metadata else []
    elif not connect_videos and isinstance(video_metadata, list):
        print("[ShotstackService] ПРЕДУПРЕЖДЕНИЕ: connect_videos равно False, но video_metadata является списком. Используем первый элемент.")
        video_metadata_for_payload = video_metadata[0] if video_metadata else {}
    else:
        video_metadata_for_payload = video_metadata


    payload = create_shotstack_payload(
        cloudinary_video_url_or_urls,
        video_metadata_for_payload,
        original_filename,
        instagram_username,
        email,
        linkedin_profile,
        connect_videos
    )

    print(f"[ShotstackService] Отправка запроса в Shotstack API для {original_filename} (Объединение видео: {connect_videos})...")
    print(f"[ShotstackService] JSON-payload для Shotstack: {json.dumps(payload, indent=2, ensure_ascii=False)}")

    try:
        response = requests.post(shotstack_render_url, json=payload, headers=headers, timeout=30)
        response.raise_for_status()

        result = response.json()
        render_id = result.get('response', {}).get('id')

        if render_id:
            return render_id, "Рендеринг успешно поставлен в очередь."
        else:
            print(f"[ShotstackService] ОШИБКА: Shotstack API не вернул ID рендеринга. Ответ: {json.dumps(result, indent=2, ensure_ascii=False)}")
            raise RuntimeError("Shotstack API не вернул ID рендеринга после успешного запроса.")

    except requests.exceptions.HTTPError as e:
        error_message = f"HTTP-ошибка от Shotstack: {e.response.status_code} {e.response.reason}. Подробности: {e.response.text}"
        print(f"[ShotstackService] ОШИБКА: {error_message}")
        raise requests.exceptions.RequestException(error_message) from e
    except requests.exceptions.ConnectionError as e:
        error_message = f"Ошибка подключения к Shotstack: {e}"
        print(f"[ShotstackService] ОШИБКА: {error_message}")
        raise requests.exceptions.RequestException(error_message) from e
    except requests.exceptions.Timeout as e:
        error_message = f"Тайм-аут при подключении к Shotstack: {e}"
        print(f"[ShotstackService] ОШИБКА: {e}")
        raise requests.exceptions.RequestException(error_message) from e
    except Exception as e:
        error_message = f"Произошла непредвиденная ошибка при вызове Shotstack API: {e}"
        print(f"[ShotstackService] ОШИБКА: {error_message}")
        raise Exception(error_message) from e


def get_shotstack_render_status(render_id):
    shotstack_api_key = os.environ.get('SHOTSTACK_API_KEY')
    shotstack_status_url = f"https://api.shotstack.io/stage/render/{render_id}" # Используем stage для тестирования

    if not shotstack_api_key:
        print("[ShotstackService] ОШИБКА: Переменная окружения SHOTSTACK_API_KEY не установлена.")
        raise ValueError("SHOTSTACK_API_KEY environment variable is not set.")

    headers = {
        "x-api-key": shotstack_api_key
    }

    print(f"[ShotstackService] Проверка статуса для ID рендеринга: {render_id}...")

    try:
        response = requests.get(shotstack_status_url, headers=headers, timeout=15)
        response.raise_for_status()

        result = response.json()
        status = result.get('response', {}).get('status')
        url = result.get('response', {}).get('url')
        poster_url = result.get('response', {}).get('poster') 
        error_message = result.get('response', {}).get('message')

        return {
            "status": status,
            "url": url,
            "poster": poster_url, 
            "error_message": error_message
        }

    except requests.exceptions.HTTPError as e:
        error_message = f"HTTP-ошибка от Shotstack API статуса: {e.response.status_code} {e.response.reason}. Подробности: {e.response.text}"
        print(f"[ShotstackService] ОШИБКА: {error_message}")
        raise requests.exceptions.RequestException(error_message) from e
    except requests.exceptions.ConnectionError as e:
        error_message = f"Ошибка подключения к Shotstack: {e}"
        print(f"[ShotstackService] ОШИБКА: {error_message}")
        raise requests.exceptions.RequestException(error_message) from e
    except requests.exceptions.Timeout as e:
        error_message = f"Тайм-аут при подключении к Shotstack: {e}"
        print(f"[ShotstackService] ОШИБКА: {e}")
        raise requests.exceptions.RequestException(error_message) from e
    except Exception as e:
        error_message = f"Произошла непредвиденная ошибка при вызове Shotstack API статуса: {e}"
        print(f"[ShotstackService] ОШИБКА: {error_message}")
        raise Exception(error_message) from e
