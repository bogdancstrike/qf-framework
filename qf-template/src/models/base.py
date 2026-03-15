"""
SQLAlchemy declarative base and example model.

All ORM models should inherit from Base.  The ExampleRecord model is a
minimal working example — replace or extend it for your domain.

Creating / updating the schema
-------------------------------
Use Alembic for production migrations.  For a quick dev reset:

    from instances import get_engine
    from models.base import Base
    Base.metadata.create_all(get_engine())   # create all tables
    Base.metadata.drop_all(get_engine())     # drop all tables
"""

from datetime import datetime

from sqlalchemy import Column, DateTime, Integer, String, Text
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """Shared declarative base for all models in this application."""
    pass


class ExampleRecord(Base):
    """Minimal example model — replace with your domain entities.

    Demonstrates:
    - Auto-increment primary key
    - String and Text columns
    - Automatic created_at / updated_at timestamps
    """
    __tablename__ = "example_records"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False, index=True)
    payload = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False
    )

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "name": self.name,
            "payload": self.payload,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }

    def __repr__(self) -> str:
        return f"<ExampleRecord id={self.id} name={self.name!r}>"
