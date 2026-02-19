"""
Database connection and session management for Rankings feature.
Uses SQLite by default for local development, PostgreSQL if DATABASE_URL is set.
"""
import os
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Use SQLite by default for easy local development
_default_db_path = Path(__file__).parent / "pgrb_rankings.db"
DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    f"sqlite:///{_default_db_path}"
)

# SQLite needs check_same_thread=False for FastAPI
connect_args = {"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {}
engine = create_engine(DATABASE_URL, echo=False, connect_args=connect_args)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


def get_db():
    """Dependency for FastAPI endpoints to get a database session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db():
    """Create all tables if they don't exist."""
    from ranking_models import FundCategory, Fund, FundScore  # noqa
    Base.metadata.create_all(bind=engine)
