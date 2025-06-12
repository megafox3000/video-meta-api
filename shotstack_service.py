import os
import requests
import json

def create_shotstack_payload(cloudinary_video_url_or_urls, video_metadata_list, original_filename, instagram_username, email, linkedin_profile, connect_videos=False):
    """
    Создает JSON-payload для запроса к Shotstack API.
    Поддерживает объединение нескольких видео путем добавления нескольких клипов в одну дорожку.

    :param cloudinary_video_url_or_urls: Список URL Cloudinary видео (если connect_videos=True)
                                         Или одиночный URL (если connect_videos=False)
    :param video_metadata_list: Список метаданных для КАЖДОГО видео (когда connect_videos=True),
                                 или метаданные одного видео (когда connect_videos=False).
                                 Должен содержать 'duration', 'width', 'height' для каждого.
    :param original_filename: Имя файла (для логирования/названия)
    :param instagram_username: Имя пользователя Instagram для наложения
    :param email: Email пользователя
    :param linkedin_profile: Профиль LinkedIn пользователя
    :param connect_videos: Флаг, указывающий, нужно ли объединять видео.
    """
    # Если connect_videos=True, ожидаем список метаданных.
    # Если connect_videos=False, ожидаем один словарь метаданных.
    # Чтобы упростить логику, всегда работаем со списком, преобразуя одиночные метаданные в список из одного элемента.
    if not isinstance(video_metadata_list, list):
        # Если переданы одиночные метаданные, преобразуем их в список из одного элемента
        processed_metadata_list = [video_metadata_list]
    else:
        processed_metadata_list = video_metadata_list

    # Для определения общей длительности и разрешения/соотношения сторон для выходного видео
    # Если объединяем, берем суммарную длительность и параметры первого видео как базовые
    # Если не объединяем, берем параметры единственного видео
    total_duration = sum(m.get('duration', 0) for m in processed_metadata_list if m)
    
    # Берем ширину/высоту из первого видео в списке для определения разрешения выходного файла
    # Это важно, так как Shotstack использует их для Canvas.
    first_video_metadata = processed_metadata_list[0] if processed_metadata_list else {}
    width = first_video_metadata.get('width', 1920) # По умолчанию 1080p, если не найдено
    height = first_video_metadata.get('height', 1080) # По умолчанию 1080p, если не найдено

    # Используем cleaned_username для заголовка
    cleaned_username = "".join(c for c in (instagram_username or '').strip() if c.isalnum() or c in ('_', '-')).strip()
    title_text = f"@{cleaned_username}" if cleaned_username else "Video Analysis"

    output_resolution = "sd" # По умолчанию
    aspect_ratio = "16:9" # По умолчанию

    if width >= 1920 or height >= 1080: # Проверяем на HD (1080p и выше)
        output_resolution = "hd"
    elif width >= 1280 or height >= 720: # Проверяем на SD (720p)
        output_resolution = "sd"
    # Для очень маленьких видео Shotstack масштабирует их.

    if width > height:
        aspect_ratio = "16:9" # Горизонтальная ориентация
    elif height > width:
        aspect_ratio = "9:16" # Вертикальная ориентация
    else:
        aspect_ratio = "1:1" # Квадрат

    # --- Создание клипов для видео ---
    video_clips = []
    current_start_time = 0.0

    if connect_videos and isinstance(cloudinary_video_url_or_urls, list):
        # Если объединяем, то cloudinary_video_url_or_urls - это список URL
        # и video_metadata_list должен быть списком метаданных, соответствующих этим URL.
        # Необходимо использовать длительность КАЖДОГО видео для правильного позиционирования клипов.
        for i, url in enumerate(cloudinary_video_url_or_urls):
            clip_metadata = processed_metadata_list[i] if i < len(processed_metadata_list) else {}
            clip_duration = clip_metadata.get('duration', 5.0) # Использовать длительность каждого клипа
            
            video_clips.append({
                "asset": {
                    "type": "video",
                    "src": url
                },
                "start": current_start_time,
                "length": clip_duration # Явно указываем длительность каждого клипа
            })
            current_start_time += clip_duration # Обновляем время начала для следующего клипа
    else:
        # Если не объединяем или передан одиночный URL
        # Здесь processed_metadata_list содержит один элемент
        single_video_duration = processed_metadata_list[0].get('duration', 5.0) if processed_metadata_list else 5.0
        video_clips.append({
            "asset": {
                "type": "video",
                "src": cloudinary_video_url_or_urls # Здесь ожидается один URL
            },
            "length": single_video_duration, # Используем длительность для одиночного видео
            "start": 0
        })
        total_duration = single_video_duration # Обновляем общую длительность для одного видео

    payload = {
        "timeline": {
            "tracks": [
                {   # ДОРОЖКА 1: ДЛЯ ВИДЕОКЛИПОВ (одного или нескольких)
                    "clips": video_clips # Здесь будут все видеоклипы
                },
                {   # ДОРОЖКА 2: ДЛЯ ТЕКСТОВОГО НАЛОЖЕНИЯ
                    "clips": [
                        {
                            "asset": {
                                "type": "title",
                                "text": title_text,
                                "style": "minimal",
                                "color": "#FFFFFF",
                                "size": "large"
                            },
                            "start": 0,
                            "length": total_duration, # Длительность текста равна ОБЩЕЙ длительности видео
                            "position": "bottom",
                            "offset": {
                                "y": "-0.2"
                            }
                        }
                    ]
                }
            ],
            "background": "#000000" # Черный фон
        },
        "output": {
            "format": "mp4",
            "resolution": output_resolution,
            "aspectRatio": aspect_ratio,
            "poster": {
                "format": "jpg",
                "quality": 75,
                "capture": 1 # <--- ИЗМЕНЕНИЕ ЗДЕСЬ: "1" вместо "00:00:01.000"
            }
        }
    }

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

    # Если connect_videos=True, video_metadata должен быть списком метаданных.
    # Если connect_videos=False, video_metadata должен быть одним словарем метаданных.
    # Это изменение в сигнатуре функции initiate_shotstack_render, чтобы app.py мог передавать
    # СПИСОК метаданных для каждого видео при объединении.
    if connect_videos and not isinstance(video_metadata, list):
        print("[ShotstackService] ПРЕДУПРЕЖДЕНИЕ: connect_videos равно True, но video_metadata не является списком. Это может привести к некорректному рендерингу.")
        # Попытаемся преобразовать для дальнейшей работы, если это возможно.
        video_metadata_for_payload = [video_metadata] if video_metadata else []
    elif not connect_videos and isinstance(video_metadata, list):
        print("[ShotstackService] ПРЕДУПРЕЖДЕНИЕ: connect_videos равно False, но video_metadata является списком. Используем первый элемент.")
        video_metadata_for_payload = video_metadata[0] if video_metadata else {}
    else:
        video_metadata_for_payload = video_metadata


    payload = create_shotstack_payload(
        cloudinary_video_url_or_urls, # Это может быть список или одиночный URL
        video_metadata_for_payload, # Передаем обработанные метаданные
        original_filename,
        instagram_username,
        email,
        linkedin_profile,
        connect_videos # Передаем флаг
    )

    print(f"[ShotstackService] Отправка запроса в Shotstack API для {original_filename} (Объединение видео: {connect_videos})...")
    print(f"[ShotstackService] JSON-payload для Shotstack: {json.dumps(payload, indent=2)}")

    try:
        response = requests.post(shotstack_render_url, json=payload, headers=headers, timeout=30)
        response.raise_for_status()

        result = response.json()
        render_id = result.get('response', {}).get('id')

        if render_id:
            return render_id, "Рендеринг успешно поставлен в очередь."
        else:
            print(f"[ShotstackService] ОШИБКА: Shotstack API не вернул ID рендеринга. Ответ: {json.dumps(result, indent=2)}")
            # Более специфичное исключение, если нет ID
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
        print(f"[ShotstackService] ОШИБКА: {error_message}")
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
        error_message = f"Ошибка подключения к Shotstack API статуса: {e}"
        print(f"[ShotstackService] ОШИБКА: {error_message}")
        raise requests.exceptions.RequestException(error_message) from e
    except requests.exceptions.Timeout as e:
        error_message = f"Тайм-аут при подключении к Shotstack API статуса: {e}"
        print(f"[ShotstackService] ОШИБКА: {error_message}")
        raise requests.exceptions.RequestException(error_message) from e
    except Exception as e:
        error_message = f"Произошла непредвиденная ошибка при вызове Shotstack API статуса: {e}"
        print(f"[ShotstackService] ОШИБКА: {error_message}")
        raise Exception(error_message) from e
