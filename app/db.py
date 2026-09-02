from sqlalchemy import create_engine, text
from sqlalchemy.orm import DeclarativeBase, sessionmaker
from contextlib import contextmanager
import time
import logging

from app.config import get_settings

settings = get_settings()
logger = logging.getLogger(__name__)


def create_engine_with_retry(url: str, max_retries: int = 10, retry_delay: int = 3):
    """Create SQLAlchemy engine with connection retry logic.
    
    This handles the race condition where the app starts before the database
    is fully ready (common in Docker/Railway deployments).
    """
    for attempt in range(max_retries):
        try:
            engine = create_engine(
                url,
                pool_pre_ping=True,
                future=True,
            )
            # Test the connection
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info(f"Database connection established: {url.split('@')[-1]}")
            return engine
        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning(f"Database connection attempt {attempt + 1}/{max_retries} failed: {e}")
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error(f"Failed to connect to database after {max_retries} attempts")
                raise


engine = create_engine_with_retry(settings.database_url)
SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False, future=True)


class Base(DeclarativeBase):
    """Declarative base for all ORM models."""


@contextmanager
def get_session():
    """Context manager for database sessions."""
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
