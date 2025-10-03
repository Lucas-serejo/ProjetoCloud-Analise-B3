from azure.storage.blob import BlobServiceClient, PublicAccess
from azure.core.exceptions import ResourceExistsError
from pathlib import Path
from etl.common.config import Config

def get_blob_service_client():
    """Retorna um cliente do Azure Blob Storage."""
    return BlobServiceClient.from_connection_string(Config.AZURE_STORAGE_CONNECTION)

def get_container_client(container_name=None):
    """Obtém ou cria um container no Azure Blob Storage."""
    container = container_name or Config.CONTAINER_NAME
    service = get_blob_service_client()
    container_client = service.get_container_client(container)
    try:
        container_client.create_container(public_access=PublicAccess.Container)
        print(f"[INFO] Container '{container}' criado")
    except ResourceExistsError:
        pass
    return container_client

def upload_blob(container_client, blob_name, local_path):
    """Faz upload de um arquivo para o Blob Storage."""
    path = Path(local_path)
    try:
        with open(path, "rb") as data:
            container_client.upload_blob(name=blob_name, data=data, overwrite=True)
        print(f"[OK] Arquivo '{blob_name}' enviado para o blob storage")
        return True
    except Exception as e:
        print(f"[ERROR] Falha ao enviar arquivo '{path}': {e}")
        return False

def download_blob_to_string(container_client, blob_name):
    """Baixa um blob e retorna seu conteúdo como string."""
    try:
        blob_client = container_client.get_blob_client(blob_name)
        download = blob_client.download_blob()
        content = download.readall()
        return content
    except Exception as e:
        print(f"[ERROR] Falha ao baixar blob '{blob_name}': {e}")
        return None

def list_blobs(container_client, name_starts_with=None):
    """Lista blobs em um container com prefixo opcional."""
    try:
        blobs = list(container_client.list_blobs(name_starts_with=name_starts_with))
        return blobs
    except Exception as e:
        print(f"[ERROR] Falha ao listar blobs: {e}")
        return []