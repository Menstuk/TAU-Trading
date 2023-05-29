import os
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session

# use predefined environment variable to connect to db
SQLALCHAMY_DATABASE_URL = os.getenv('DB_CONNECTION_STRING')

engine = create_engine(SQLALCHAMY_DATABASE_URL)

db_session = scoped_session(sessionmaker(autocommit=False,
                                         autoflush=False,
                                         bind=engine))
Base = declarative_base()

def get_db() -> scoped_session:
    return db_session

