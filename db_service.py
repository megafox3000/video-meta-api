import os
from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, JSON, or_, Index
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime
import logging
from contextlib import contextmanager

logger = logging.getLogger(__name__)

# --- Конфигурация базы данных ---
DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL environment variable is not set!")

# Настройка SSL для облачных баз данных PostgreSQL
engine_args = {}
if DATABASE_URL.startswith("postgresql") and "sslmode=" not in DATABASE_URL:
    engine_args['connect_args'] = {'sslmode': 'require'}

engine = create_engine(DATABASE_URL, **engine_args)
Base = declarative_base()
Session = sessionmaker(bind=engine)


# --- Модель данных ---
class Task(Base):
    __tablename__ = 'tasks'

    id = Column(Integer, primary_key=True)
    task_id = Column(String, unique=True, nullable=False, index=True)
    
    # ИЗМЕНЕНИЕ: Добавлено поле для public_id из Cloudinary с индексацией
    cloudinary_public_id = Column(String, unique=True, index=True)
    
    instagram_username = Column(String, index=True)
    email = Column(String, index=True)
    linkedin_profile = Column(String)
    original_filename = Column(String)
    status = Column(String)
    cloudinary_url = Column(String)
    video_metadata = Column(JSON)
    message = Column(Text)
    timestamp = Column(DateTime, default=datetime.utcnow)
    shotstackRenderId = Column(String)
    shotstackUrl = Column(String)
    posterUrl = Column(String)

    def __repr__(self):
        return f"<Task(id={self.id}, task_id='{self.task_id}', status='{self.status}')>"

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


# --- ИСПРАВЛЕНИЕ: Контекстный менеджер для управления сессиями ---
@contextmanager
def session_scope():
    """Обеспечивает транзакционный скоуп для серий операций с БД."""
    session = Session()
    try:
        yield session
        session.commit()
    except SQLAlchemyError as e:
        logger.error(f"Session rollback due to error: {e}", exc_info=True)
        session.rollback()
        raise
    finally:
        session.close()


# --- Функции для работы с БД (переписаны с session_scope) ---

def add_task(task_data):
    """Добавляет новую задачу в базу данных."""
    with session_scope() as session:
        new_task = Task(**task_data)
        session.add(new_task)
        session.flush() # Применяем изменения, чтобы получить объект с ID
        logger.info(f"Task '{new_task.task_id}' added to DB.")
        return new_task

def get_task_by_id(task_id_str):
    """Получает задачу по ее строковому task_id."""
    with session_scope() as session:
        return session.query(Task).filter_by(task_id=task_id_str).first()

def get_task_by_public_id(public_id):
    """НОВАЯ ФУНКЦИЯ: Получает задачу по ее Cloudinary public_id."""
    with session_scope() as session:
        return session.query(Task).filter_by(cloudinary_public_id=public_id).first()

def update_task_by_id(task_id_str, updates):
    """Обновляет существующую задачу по ее строковому task_id."""
    with session_scope() as session:
        task = session.query(Task).filter_by(task_id=task_id_str).first()
        if task:
            for key, value in updates.items():
                setattr(task, key, value)
            logger.info(f"Task '{task.task_id}' updated in DB.")
            return task
        return None

def delete_task_by_id(task_primary_key):
    """НОВАЯ ФУНКЦИЯ: Удаляет задачу по ее первичному ключу (Integer id)."""
    with session_scope() as session:
        task = session.query(Task).filter_by(id=task_primary_key).first()
        if task:
            logger.warning(f"Deleting task ID {task.id} ('{task.task_id}') from DB.")
            session.delete(task)
            return True
        return False

def get_user_videos(instagram_username=None, email=None, linkedin_profile=None):
    """Получает список видео для пользователя."""
    with session_scope() as session:
        conditions = []
        if instagram_username:
            conditions.append(Task.instagram_username == instagram_username)
        if email:
            conditions.append(Task.email == email)
        if linkedin_profile:
            conditions.append(Task.linkedin_profile == linkedin_profile)

        if not conditions:
            return []

        tasks = session.query(Task).filter(or_(*conditions)).order_by(Task.timestamp.desc()).all()
        return tasks

def create_tables():
    """Создает таблицы в базе данных, если они еще не существуют."""
    try:
        Base.metadata.create_all(engine)
        logger.info("Database tables checked/created successfully.")
    except Exception as e:
        logger.error(f"Error creating database tables: {e}", exc_info=True)
        raise

# Создание таблиц при первом импорте модуля
create_tables()
