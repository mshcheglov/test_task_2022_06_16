from databases import Database
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base

DATABASE_URL = "sqlite:///./db.sqlite"
Base = declarative_base()
database = Database(DATABASE_URL)
engine = create_engine(
    DATABASE_URL, connect_args={"check_same_thread": False}  # check_same_thread=False is needed only for SQLite
)
