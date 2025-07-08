# db_service.py
"""
Service layer for all database operations.
This module defines the database schema using SQLAlchemy ORM, manages database sessions,
and provides helper functions for all CRUD (Create, Read, Update, Delete) operations.
"""

import os
from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, JSON, or_, Index
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime, timezone
import logging
from contextlib import contextmanager

logger = logging.getLogger(__name__)

# --- Database Configuration ---
DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL environment variable is not set!")

# Add SSL option for cloud-based PostgreSQL databases like on Render
engine_args = {}
if DATABASE_URL.startswith("postgresql") and "sslmode=" not in DATABASE_URL:
    engine_args['connect_args'] = {'sslmode': 'require'}

engine = create_engine(DATABASE_URL, **engine_args)
Base = declarative_base()
Session = sessionmaker(bind=engine)


# --- Helper Function ---
def to_camel_case(snake_str):
    """Converts a snake_case string to camelCase."""
    if not isinstance(snake_str, str):
        return snake_str
    components = snake_str.split('_')
    # Return the first component lowercase and all subsequent components capitalized.
    return components[0] + ''.join(x.title() for x in components[1:])


# --- Data Model (Schema) ---
class Task(Base):
    """SQLAlchemy model representing a video processing task."""
    __tablename__ = 'tasks'

    # Columns
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
    timestamp = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    shotstackRenderId = Column(String)
    shotstackUrl = Column(String)
    posterUrl = Column(String)

    def __repr__(self):
        """String representation of the Task object for debugging."""
        return f"<Task(id={self.id}, task_id='{self.task_id}', status='{self.status}')>"

    def to_dict(self):
        """
        Automatically creates a dictionary from the model's fields
        and converts its keys to camelCase for API responses.
        """
        # 1. Automatically get a dict with snake_case keys from all table columns
        snake_case_dict = {c.name: getattr(self, c.name) for c in self.__table__.columns}
        
        # 2. Convert datetime object to ISO 8601 string format if it exists
        if isinstance(snake_case_dict.get('timestamp'), datetime):
            snake_case_dict['timestamp'] = snake_case_dict['timestamp'].isoformat()
            
        # 3. Create a new dictionary, converting each key to camelCase
        return {to_camel_case(key): value for key, value in snake_case_dict.items()}


# --- Session Management ---
@contextmanager
def session_scope():
    """
    Provide a transactional scope around a series of operations.
    This handles session creation, commit, rollback, and closing.
    """
    session = Session()
    logger.debug("Database session opened.")
    try:
        yield session
        session.commit()
        logger.debug("Database session committed.")
    except SQLAlchemyError as e:
        logger.error(f"Session rollback due to error: {e}", exc_info=True)
        session.rollback()
        raise
    finally:
        session.close()
        logger.debug("Database session closed.")


# --- Database Service Functions ---

def add_task(task_data):
    """
    Adds a new task to the database.

    Args:
        task_data (dict): A dictionary containing data for the new Task.
    
    Returns:
        dict: The newly created task object, converted to a camelCase dictionary.
    """
    with session_scope() as session:
        new_task = Task(**task_data)
        session.add(new_task)
        session.flush()  # Flush to get the new object with its ID before commit
        logger.info(f"Task '{new_task.task_id}' added to DB.")
        # CHANGED: Always return a dictionary to prevent DetachedInstanceError
        return new_task.to_dict()

def get_task_by_id(task_id_str):
    """
    Retrieves a single task as a dictionary by its string-based task_id.

    Args:
        task_id_str (str): The unique task identifier string.
    
    Returns:
        dict or None: A camelCase dictionary of the Task if found, otherwise None.
    """
    with session_scope() as session:
        task = session.query(Task).filter_by(task_id=task_id_str).first()
        # CHANGED: Return a dictionary or None to prevent DetachedInstanceError
        return task.to_dict() if task else None

def get_task_by_public_id(public_id):
    """
    Retrieves a single task object by its Cloudinary public_id.
    NOTE: This returns the raw object because it's used internally by other backend services.

    Args:
        public_id (str): The Cloudinary public_id.

    Returns:
        Task or None: The SQLAlchemy Task object if found, otherwise None.
    """
    with session_scope() as session:
        return session.query(Task).filter_by(cloudinary_public_id=public_id).first()

def update_task_by_id(task_id_str, updates):
    """
    Updates an existing task identified by its string-based task_id.

    Args:
        task_id_str (str): The unique task identifier string.
        updates (dict): A dictionary of fields to update.

    Returns:
        dict or None: The updated task as a camelCase dictionary, or None if not found.
    """
    with session_scope() as session:
        task = session.query(Task).filter_by(task_id=task_id_str).first()
        if task:
            for key, value in updates.items():
                setattr(task, key, value)
            logger.info(f"Task '{task.task_id}' updated in DB.")
            session.flush()
            # CHANGED: Return the updated dictionary
            return task.to_dict()
        return None

def delete_task_by_id(task_primary_key):
    """
    Deletes a task by its integer primary key.

    Args:
        task_primary_key (int): The primary key (id) of the task to delete.

    Returns:
        bool: True if deletion was successful, False otherwise.
    """
    with session_scope() as session:
        task = session.query(Task).get(task_primary_key)
        if task:
            logger.warning(f"Deleting task ID {task.id} ('{task.task_id}') from DB.")
            session.delete(task)
            return True
        return False

def get_user_videos(instagram_username=None, email=None, linkedin_profile=None):
    """
    Retrieves a list of videos for a user by one of the identifiers (OR logic).

    Args:
        instagram_username (str, optional): User's Instagram username.
        email (str, optional): User's email.
        linkedin_profile (str, optional): User's LinkedIn profile URL.

    Returns:
        list[dict]: A list of task dictionaries, with keys converted to camelCase.
    """
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
        # CHANGED: Return a list of dictionaries to prevent DetachedInstanceError
        return [task.to_dict() for task in tasks]

def create_tables():
    """
    Creates all database tables defined in the Base metadata if they don't already exist.
    This function should be called once at application startup from app.py.
    """
    try:
        Base.metadata.create_all(engine)
        logger.info("Database tables checked/created successfully.")
    except Exception as e:
        logger.error(f"Error creating database tables: {e}", exc_info=True)
        raise

# REMOVED: The automatic call to create_tables() has been removed.
# This function should now be called explicitly from your main app file (e.g., app.py).
