import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

# Force load only .env_target
load_dotenv(dotenv_path=".env_target", override=True)

def get_target_engine():
    PG_HOST = os.getenv("PG_HOST")
    PG_PORT = os.getenv("PG_PORT")
    PG_DB = os.getenv("PG_DB")
    PG_USER = os.getenv("PG_USER")
    PG_PASSWORD = quote_plus(os.getenv("PG_PASSWORD"))  # Encode special chars

    # 🔍 Debug print to confirm correct env vars are loaded
    print("🔍 Target ENV:", PG_HOST, PG_PORT, PG_DB, PG_USER)

    # Build SQLAlchemy engine
    engine = create_engine(
        f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    )

    # Ensure schema "test" exists
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS test;"))

    print("✅ Connected to target DB and schema 'test' is ready.")
    return engine

