import os
from pathlib import Path

class Config:
    # Diretórios
    BASE_DIR = Path(__file__).parent.parent.parent.resolve()
    DATA_DIR = Path(os.getenv("PATH_TO_SAVE", "./dados_b3")).resolve()
    
    # Azure Storage
    AZURE_STORAGE_CONNECTION = os.getenv(
        "AZURE_STORAGE_CONNECTION_STRING",
        "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://azurite:10000/devstoreaccount1;"
    )
    CONTAINER_NAME = os.getenv("AZURE_BLOB_CONTAINER", "dados-pregao")
    
    # PostgreSQL
    POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
    POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
    POSTGRES_DB = os.getenv("POSTGRES_DB", "b3_data")
    POSTGRES_USER = os.getenv("POSTGRES_USER", "user")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password")
    
    # Configurações de exportação
    EXPORT_JSON = os.getenv("EXPORT_JSON", "false").lower() == "true"

    # Novas configurações para processamento de múltiplos dias
    MULTI_DAY_PROCESSING = os.getenv("MULTI_DAY_PROCESSING", "false").lower() == "true"
    MULTI_DAY_LIMIT = int(os.getenv("MULTI_DAY_LIMIT", "5"))  # Número de dias úteis para processar