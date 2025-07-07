import os
from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, JSON, or_
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

# Конфигурация базы данных
DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    logger.error("DATABASE_URL environment variable is not set!")
    raise RuntimeError("DATABASE_URL environment variable is not set!")

connect_args = {}
# Добавляем опцию SSL для PostgreSQL, если она еще не указана в URL
if DATABASE_URL.startswith("postgresql://") or DATABASE_URL.startswith("postgres://"):
    if "sslmode=" not in DATABASE_URL:
        connect_args["sslmode"] = "require"

engine = create_engine(DATABASE_URL, connect_args=connect_args)

Base = declarative_base()
Session = sessionmaker(bind=engine)

# Определение модели задачи (Task Model)
class Task(Base):
    __tablename__ = 'tasks'

    id = Column(Integer, primary_key=True)
    task_id = Column(String, unique=True, nullable=False)
    instagram_username = Column(String)
    email = Column(String)
    linkedin_profile = Column(String)
    original_filename = Column(String)
    status = Column(String)
    cloudinary_url = Column(String)
    video_metadata = Column(JSON)
    message = Column(Text)
    timestamp = Column(DateTime, default=datetime.now)
    shotstackRenderId = Column(String)
    shotstackUrl = Column(String)
    posterUrl = Column(String)
    cloudinary_public_id = Column(String) # Добавляем это поле для хранения public_id

    def __repr__(self):
        return f"<Task(task_id='{self.task_id}', status='{self.status}')>"

    def to_dict(self):
        return {
            "id": self.id,
            "taskId": self.task_id,
            "instagram_username": self.instagram_username,
            "email": self.email,
            "linkedin_profile": self.linkedin_profile,
            "originalFilename": self.original_filename,
            "status": self.status,
            "cloudinary_url": self.cloudinary_url,
            "metadata": self.video_metadata,
            "message": self.message,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
            "shotstackRenderId": self.shotstackRenderId,
            "shotstackUrl": self.shotstackUrl,
            "posterUrl": self.posterUrl,
            "cloudinary_public_id": self.cloudinary_public_id # Включаем в словарь
        }

def create_tables():
    """Создает таблицы в базе данных, если они еще не существуют."""
    try:
        Base.metadata.create_all(engine)
        logger.info("Database tables created or already exist.")
    except SQLAlchemyError as e:
        logger.error(f"Error creating database tables: {e}", exc_info=True)
        raise

# Helper functions for database operations

def get_session():
    """Возвращает новую сессию базы данных."""
    return Session()

def add_task(task_data):
    """
    Добавляет новую задачу в базу данных.
    Args:
        task_data (dict): Словарь с данными для создания Task.
    Returns:
        Task: Созданный объект Task.
    Raises:
        SQLAlchemyError: В случае ошибки базы данных.
    """
    session = get_session()
    try:
        new_task = Task(**task_data)
        session.add(new_task)
        session.commit()
        logger.info(f"Task '{new_task.task_id}' added to DB.")
        return new_task
    except SQLAlchemyError as e:
        session.rollback()
        logger.error(f"Error adding task to DB: {e}", exc_info=True)
        raise
    finally:
        session.close()

def get_task_by_id(task_id):
    """
    Получает задачу по ее task_id.
    Returns:
        Task or None: Объект Task, если найден, иначе None.
    Raises:
        SQLAlchemyError: В случае ошибки базы данных.
    """
    session = get_session()
    try:
        task = session.query(Task).filter_by(task_id=task_id).first()
        return task
    except SQLAlchemyError as e:
        logger.error(f"Error fetching task '{task_id}' from DB: {e}", exc_info=True)
        raise
    finally:
        session.close()

def update_task(task, updates):
    """
    Обновляет существующую задачу в базе данных.
    Args:
        task (Task): Объект Task для обновления.
        updates (dict): Словарь с полями и их новыми значениями.
    Returns:
        Task: Обновленный объект Task.
    Raises:
        SQLAlchemyError: В случае ошибки базы данных.
    """
    session = get_session()
    try:
        # Присоединяем переданный объект к текущей сессии, если он отсоединен
        session.add(task)
        for key, value in updates.items():
            setattr(task, key, value)
        session.commit()
        logger.info(f"Task '{task.task_id}' updated in DB.")
        return task
    except SQLAlchemyError as e:
        session.rollback()
        logger.error(f"Error updating task '{task.task_id}': {e}", exc_info=True)
        raise
    finally:
        session.close()

def delete_task(task_id):
    """
    Удаляет задачу из базы данных по ее task_id.
    Args:
        task_id (str): Уникальный идентификатор задачи.
    Returns:
        bool: True, если задача успешно удалена, False в противном случае.
    Raises:
        SQLAlchemyError: В случае ошибки базы данных.
    """
    session = get_session()
    try:
        task = session.query(Task).filter_by(task_id=task_id).first()
        if task:
            session.delete(task)
            session.commit()
            logger.info(f"Task '{task_id}' successfully deleted from DB.")
            return True
        else:
            logger.warning(f"Task '{task_id}' not found in DB for deletion.")
            return False
    except SQLAlchemyError as e:
        session.rollback()
        logger.error(f"Error deleting task '{task_id}' from DB: {e}", exc_info=True)
        raise
    finally:
        session.close()

def get_user_videos(instagram_username=None, email=None, linkedin_profile=None):
    """
    Получает список видео для пользователя по одному из идентификаторов (логика ИЛИ).
    Returns:
        list[Task]: Список объектов Task.
    Raises:
        SQLAlchemyError: В случае ошибки базы данных.
    """
    session = get_session()
    try:
        query = session.query(Task)
        
        # Строим динамический запрос с OR условиями
        conditions = []
        if instagram_username:
            conditions.append(Task.instagram_username == instagram_username)
        if email:
            conditions.append(Task.email == email)
        if linkedin_profile:
            conditions.append(Task.linkedin_profile == linkedin_profile)

        if conditions:
            # Объединяем условия с помощью OR, чтобы найти совпадения по любому из полей
            query = query.filter(or_(*conditions))
        else:
            # Если идентификаторы не предоставлены, возвращаем пустой список
            return [] 

        # Фильтруем только те видео, которые могут быть интересны для отображения на фронтенде
        # Включены также статусы 'failed' и 'cloudinary_metadata_incomplete'
        query = query.filter(Task.status.in_([
            'completed', 'processing', 'uploaded', 'shotstack_pending',
            'concatenated_pending', 'concatenated_completed', 'failed', 'cloudinary_metadata_incomplete'
        ]))
        
        tasks = query.order_by(Task.timestamp.desc()).all()
        return tasks
    except SQLAlchemyError as e:
        logger.error(f"Error fetching user videos: {e}", exc_info=True)
        raise
    finally:
        session.close()

# Создание таблиц при импорте модуля
create_tables()

