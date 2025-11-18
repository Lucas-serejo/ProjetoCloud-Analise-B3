import os
from pathlib import Path
from dotenv import load_dotenv

# Procura o .env na pasta atual OU na raiz do projeto
env_candidates = [
    Path(__file__).parent / '.env',
    Path(__file__).parent.parent / '.env',
    Path.cwd() / '.env'
]
for env_path in env_candidates:
    if env_path.exists():
        load_dotenv(env_path)
        break

class Config:
    # Diretórios
    BASE_DIR = Path(__file__).parent.resolve()
    DATA_DIR = Path(os.getenv("PATH_TO_SAVE", "./dados_b3")).resolve()
    
    # Azure Storage
    AZURE_STORAGE_CONNECTION = os.getenv(
        "AZURE_STORAGE_CONNECTION_STRING")
    CONTAINER_NAME = os.getenv("AZURE_BLOB_CONTAINER", "dados-pregao")
    UPLOAD_TO_BLOB = os.getenv("UPLOAD_TO_BLOB", "true").lower() == "true"
    
    # PostgreSQL
    POSTGRES_HOST = os.getenv("POSTGRES_HOST")
    POSTGRES_PORT = os.getenv("POSTGRES_PORT")
    POSTGRES_DB = os.getenv("POSTGRES_DB")
    POSTGRES_USER = os.getenv("POSTGRES_USER")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
    POSTGRES_SSL_MODE = os.getenv("POSTGRES_SSL_MODE")
    POSTGRES_CONNECTION_STRING = os.getenv("POSTGRES_CONNECTION_STRING")
    
    # Exportação opcional
    EXPORT_JSON = os.getenv("EXPORT_JSON", "false").lower() == "true"

    # Processamento multi-dia
    MULTI_DAY_PROCESSING = os.getenv("MULTI_DAY_PROCESSING", "false")
    MULTI_DAY_LIMIT = int(os.getenv("MULTI_DAY_LIMIT", "5"))  # Número de dias úteis para processar