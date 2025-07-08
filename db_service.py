# db_service.py
import os
from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, JSON, or_, Index
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime
import logging
from contextlib import contextmanager

logger = logging.getLogger(__name__)

# --- Конфигурация ---
DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set!")

engine_args = {}
if DATABASE_URL.startswith("postgresql") and "sslmode=" not in DATABASE_URL:
    engine_args['connect_args'] = {'sslmode': 'require'}

engine = create_engine(DATABASE_URL, **engine_args)
Base = declarative_base()
Session = sessionmaker(bind=engine)

# --- Вспомогательная функция ---
def to_camel_case(snake_str):
    """Конвертирует строку из snake_case в camelCase."""
    components = snake_str.split('_')
    return components[0] + ''.join(x.title() for x in components[1:])

# --- Модель данных ---
class Task(Base):
    __tablename__ = 'tasks'
    id = Column(Integer, primary_key=True)
    task_id = Column(String, unique=True, nullable=False, index=True)
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
    shotstackRenderId = Column(String) # Это уже camelCase, конвертер его не тронет
    shotstackUrl = Column(String)      # И это тоже
    posterUrl = Column(String)         # И это

    def to_dict(self):
        """Автоматически создает словарь и конвертирует ключи в camelCase."""
        snake_case_dict = {c.name: getattr(self, c.name) for c in self.__table__.columns}
        
        if isinstance(snake_case_dict.get('timestamp'), datetime):
            snake_case_dict['timestamp'] = snake_case_dict['timestamp'].isoformat()
            
        return {to_camel_case(key): value for key, value in snake_case_dict.items()}

# --- Контекстный менеджер сессий ---
@contextmanager
def session_scope():
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

# --- Функции для работы с БД ---

def get_user_videos(instagram_username=None, email=None, linkedin_profile=None):
    """Получает список видео для пользователя в виде словарей."""
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
        # ИСПРАВЛЕНИЕ: Возвращаем список словарей
        return [task.to_dict() for task in tasks]

# ... (Остальные ваши функции, например, add_task, update_task, теперь тоже должны возвращать task.to_dict() или ничего) ...

def get_task_by_public_id(public_id):
    """Находит задачу по public_id."""
    with session_scope() as session:
        task = session.query(Task).filter_by(cloudinary_public_id=public_id).first()
        return task # Возвращаем объект, так как он используется только внутри бэкенда

def delete_task_by_id(task_primary_key):
    """Удаляет задачу по ее первичному ключу (Integer id)."""
    with session_scope() as session:
        task = session.query(Task).get(task_primary_key)
        if task:
            session.delete(task)
            return True
        return False
        
def create_tables():
    Base.metadata.create_all(engine)

create_tables()
