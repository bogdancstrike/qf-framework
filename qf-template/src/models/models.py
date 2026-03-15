"""
init_db — called by the ETL runtime at startup.

Use this hook to run schema migrations, create tables, or warm up
connection pools before Kafka consumers start processing messages.

Example (auto-create all SQLAlchemy tables):
    from instances import get_engine
    from models.base import Base

    def init_db():
        Base.metadata.create_all(get_engine())
"""


def init_db():
    """Initialise the database schema.

    Replace the pass with your migration or table-creation logic.
    Called once at ETL startup before any workers begin polling.
    """
    pass
