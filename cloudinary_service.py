# cloudinary_service.py
import cloudinary
import cloudinary.uploader
import hashlib
from datetime import datetime
import logging
import os
from cloudinary.exceptions import NotFound

logger = logging.getLogger(__name__)

def upload_video_to_cloudinary(file_stream, original_filename, instagram_username):
    """
    Загружает видеофайл в Cloudinary.
    Конфигурация Cloudinary (cloud_name, api_key, api_secret) должна быть
    установлена до вызова этой функции (например, в app.py).

    Args:
        file_stream: Объект файла (например, request.files['video']).
        original_filename (str): Оригинальное имя файла.
        instagram_username (str): Имя пользователя Instagram для организации папок.

    Returns:
        dict: Результат загрузки от Cloudinary (словарь),
              включающий 'secure_url', 'duration', 'width', 'height', 'bytes' и другие метаданные.

    Raises:
        Exception: Если загрузка в Cloudinary не удалась или отсутствует secure_url.
    """
    # Очищаем имя пользователя Instagram для использования в путях и тегах Cloudinary
    cleaned_username = "".join(c for c in (instagram_username or '').strip() if c.isalnum() or c in ('_', '-')).strip()
    if not cleaned_username:
        cleaned_username = "anonymous" # Запасной вариант, если имя пользователя пустое

    original_filename_base = os.path.splitext(original_filename)[0]
    # Используем уникальный хэш для создания уникального public_id
    unique_hash = hashlib.md5(f"{cleaned_username}/{original_filename}/{datetime.now().timestamp()}".encode()).hexdigest()
    
    # Public ID для Cloudinary будет включать имя пользователя и часть уникального хэша
    # Это помогает предотвратить коллизии имен и организовать ресурсы.
    public_id = f"hife_video_analysis/{cleaned_username}/{original_filename_base}_{unique_hash[:8]}"

    logger.info(f"[CloudinaryService] Загрузка видео '{original_filename}' в Cloudinary (public_id: {public_id})...")
    try:
        upload_result = cloudinary.uploader.upload(
            file_stream,
            resource_type="video",
            folder=f"hife_video_analysis/{cleaned_username}", # Папка для организации в Cloudinary
            public_id=public_id,
            unique_filename=False, # public_id уже уникален благодаря хэшу
            overwrite=True, # Перезаписать, если ресурс с таким public_id уже существует (крайне маловероятно)
            quality="auto", # Автоматическая оптимизация качества
            format="mp4",   # Конвертация в MP4
            tags=["hife_analysis", cleaned_username] # Добавление тегов для лучшей организации
        )
        logger.info(f"[CloudinaryService] Ответ Cloudinary: {upload_result.keys()}")

        if upload_result and upload_result.get('secure_url'):
            # Проверка, что основные метаданные доступны и корректны
            if upload_result.get('duration', 0) <= 0 or \
               upload_result.get('width', 0) <= 0 or \
               upload_result.get('height', 0) <= 0 or \
               upload_result.get('bytes', 0) <= 0:
                logger.warning(
                    f"[CloudinaryService] ПРЕДУПРЕЖДЕНИЕ: Видео загружено, но основные метаданные "
                    f"(duration/resolution/size) отсутствуют или равны 0. Полные метаданные: {upload_result}"
                )
                # Возвращаем результат даже с неполными метаданными, пусть app.py решает, что делать дальше.
            return upload_result
        else:
            # Если secure_url отсутствует, это серьезная проблема
            raise Exception(f"Загрузка в Cloudinary не удалась: secure_url отсутствует в ответе. Ответ: {upload_result}")

    except Exception as e:
        logger.error(f"[CloudinaryService] ОШИБКА при загрузке в Cloudinary: {e}", exc_info=True)
        raise # Перебрасываем исключение для обработки в app.py

def check_video_existence(public_id):
    """
    Проверяет, существует ли ресурс в Cloudinary.
    Возвращает True, если ресурс найден, и False, если нет.
    """
    if not public_id:
        return False
    try:
        # Метод .resource() вернет данные, если ресурс есть,
        # или выбросит исключение NotFound, если его нет.
        cloudinary.api.resource(public_id, resource_type="video")
        logger.info(f"[CloudinaryService] Проверка: ресурс '{public_id}' существует.")
        return True
    except NotFound:
        # Это ожидаемое исключение, если файла нет.
        logger.warning(f"[CloudinaryService] Проверка: ресурс '{public_id}' НЕ НАЙДЕН в Cloudinary.")
        return False
    except Exception as e:
        # Любые другие ошибки (проблемы с API, соединением) логируем.
        logger.error(f"[CloudinaryService] Ошибка при проверке ресурса '{public_id}': {e}")
        # В этом случае лучше считать, что ресурс есть, чтобы случайно не удалить его.
        return True


