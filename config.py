import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    # SQLAlchemy (for app database - stores user data, history, etc.)
    SQLALCHEMY_DATABASE_URI = os.getenv(
        'APP_DATABASE_URL',
        f"mysql+pymysql://{os.getenv('MYSQL_USER', 'root')}:{os.getenv('MYSQL_PASSWORD', 'Jp^6')}@"
        f"{os.getenv('MYSQL_HOST', 'localhost')}:{os.getenv('MYSQL_PORT', '3306')}/"
        f"{os.getenv('MYSQL_DATABASE', 'classicmodels')}"
    )
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    # LLM Configuration
    LLM_MODEL_PATH = os.getenv("LLM_MODEL_PATH", "./models/llm/sqlcoder-7b-q5_k_m.gguf")
    LLM_CONTEXT_SIZE = int(os.getenv('LLM_CONTEXT_SIZE', 16384))
    LLM_GPU_LAYERS = int(os.getenv('LLM_GPU_LAYERS', 28))  # Adjust for 128MB VRAM
    LLM_TEMPERATURE = float(os.getenv('LLM_TEMPERATURE', 0.1))
    LLM_MAX_TOKENS = int(os.getenv('LLM_MAX_TOKENS', 1000))

    # RAG Configuration
    EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "sentence-transformers/all-MiniLM-L6-v2")
    FAISS_INDEX_PATH = os.getenv("FAISS_INDEX_PATH", "./data/faiss_index")
    RAG_TOP_K = int(os.getenv("RAG_TOP_K", 3))

    # Query Constraints
    MAX_QUERY_ROWS = int(os.getenv("MAX_QUERY_ROWS", 1000))
    QUERY_TIMEOUT = int(os.getenv("QUERY_TIMEOUT", 30))
    MAX_JOINS = int(os.getenv("MAX_JOINS", 3))
    MAX_SUBQUERY_DEPTH = int(os.getenv("MAX_SUBQUERY_DEPTH", 3))

    # Schema Cache
    SCHEMA_CACHE_DIR = os.getenv("SCHEMA_CACHE_DIR", "./data/schema_cache")
    SCHEMA_CACHE_TTL = int(os.getenv("SCHEMA_CACHE_TTL", 3600))  # 1 hour
