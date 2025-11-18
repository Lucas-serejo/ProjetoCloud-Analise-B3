from azure.storage.blob import BlobServiceClient, PublicAccess, ContentSettings
from azure.core.exceptions import ResourceExistsError
from pathlib import Path
from config import Config

def get_blob_service_client():
    # Cliente de serviço de Blob a partir da connection string
    return BlobServiceClient.from_connection_string(Config.AZURE_STORAGE_CONNECTION)

def get_container_client(container_name=None):
    # Retorna o container; cria se não existir
    container = container_name or Config.CONTAINER_NAME
    service = get_blob_service_client()
    container_client = service.get_container_client(container)
    try:
        container_client.create_container(public_access=PublicAccess.Container)
        print(f"[INFO] Container '{container}' criado")
    except ResourceExistsError:
        pass
    return container_client

def upload_blob(container_client, blob_name, local_path, *, max_concurrency: int = 8, content_type: str | None = None, skip_if_exists: bool = False):
    # Upload de arquivo local com paralelismo
    path = Path(local_path)
    try:
        blob_client = container_client.get_blob_client(blob_name)

        if skip_if_exists:
            try:
                if blob_client.exists():
                    print(f"[SKIP] Blob já existe: '{blob_name}'")
                    return True
            except Exception:
                # Se falhar ao checar existência, segue com upload
                pass

        settings = ContentSettings(content_type=content_type) if content_type else None

        with open(path, "rb") as data:
            blob_client.upload_blob(
                data=data,
                overwrite=True,
                max_concurrency=max_concurrency,
                content_settings=settings,
            )
        print(f"[OK] Arquivo '{blob_name}' enviado para o blob storage")
        return True
    except Exception as e:
        print(f"[ERROR] Falha ao enviar '{path}': {e}")
        return False

def blob_exists(container_client, blob_name: str) -> bool:
    try:
        return container_client.get_blob_client(blob_name).exists()
    except Exception:
        return False

def download_blob_to_string(container_client, blob_name):
    # Baixa conteúdo do blob como bytes
    try:
        blob_client = container_client.get_blob_client(blob_name)
        download = blob_client.download_blob()
        content = download.readall()
        return content
    except Exception as e:
        print(f"[ERROR] Falha ao baixar blob '{blob_name}': {e}")
        return None

def list_blobs(container_client, name_starts_with=None):
    # Lista blobs com prefixo opcional
    try:
        blobs = list(container_client.list_blobs(name_starts_with=name_starts_with))
        return blobs
    except Exception as e:
        print(f"[ERROR] Falha ao listar blobs: {e}")
        return []