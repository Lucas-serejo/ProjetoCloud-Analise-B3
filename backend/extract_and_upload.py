import os
from datetime import datetime, timedelta
from pathlib import Path
from helpers import yymmdd
import requests
import zipfile
from azure.storage.blob import BlobServiceClient, PublicAccess
from azure.core.exceptions import ResourceExistsError

# Configurações
PATH_TO_SAVE = Path(os.getenv("PATH_TO_SAVE", "./dados_b3")).resolve()
AZURE_BLOB_CONNECTION = os.getenv(
    "AZURE_STORAGE_CONNECTION_STRING",
    "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://azurite:10000/devstoreaccount1;"
) 
CONTAINER = os.getenv("AZURE_BLOB_CONTAINER", "dados-pregao")

def build_url_download(date_to_download: str) -> str:
    return f"https://www.b3.com.br/pesquisapregao/download?filelist=PR{date_to_download}.zip"

def try_http_download(url: str):
    session = requests.Session()
    try:
        print(f"[INFO] Tentando {url}")
        resp = session.get(url, timeout=30)
        if resp.ok and resp.content and len(resp.content) > 200 and resp.content[:2] == b"PK":
            return resp.content, os.path.basename(url)
    except requests.RequestException:
        print(f"[ERROR] Falha ao acessar {url}")
    return None

def get_container_client():
    service = BlobServiceClient.from_connection_string(AZURE_BLOB_CONNECTION)
    container_client = service.get_container_client(CONTAINER)
    try:
        container_client.create_container(public_access=PublicAccess.Container)
        print(f"[INFO] Container '{CONTAINER}' criado")
    except ResourceExistsError:
        pass
    return container_client

def save_file_to_blob(container_client, file_name: str, local_path_file: Path):
    try:
        with open(local_path_file, "rb") as data:
            container_client.upload_blob(name=file_name, data=data, overwrite=True)
        print(f"[OK] Arquivo '{file_name}' enviado para o blob storage")
    except Exception as e:
        print(f"[ERROR] Falha ao enviar arquivo '{local_path_file}': {e}")
        raise

def upload_directory_to_blob(container_client, directory_path: Path, blob_prefix: str = ""):
    files_uploaded = 0
    for root, _, files in os.walk(directory_path):
        for file in files:
            local_path = Path(root) / file
            relative_path = local_path.relative_to(directory_path)
            blob_name = f"{blob_prefix}{relative_path.as_posix()}"
            save_file_to_blob(container_client, blob_name, local_path)
            files_uploaded += 1
    return files_uploaded

def run():
    dt = yymmdd(datetime.now())
    print(f"[INFO] Iniciando processo de extração para data: {dt}")

    # tenta dia atual, se falhar tenta dia anterior
    zip_result = try_http_download(build_url_download(dt))
    if not zip_result:
        print("[WARNING] Não foi possível baixar arquivo do dia atual, tentando data anterior...")
        dt = yymmdd(datetime.now() - timedelta(days=1))
        zip_result = try_http_download(build_url_download(dt))

    if not zip_result:
        raise RuntimeError("Não foi possível baixar o arquivo de cotações para hoje ou ontem")

    zip_bytes, zip_name = zip_result
    print(f"[OK] Baixado arquivo de cotações: {zip_name}")

    # salva zip em PATH_TO_SAVE
    PATH_TO_SAVE.mkdir(parents=True, exist_ok=True)
    zip_path = PATH_TO_SAVE / f"pregao_{dt}.zip"
    zip_path.write_bytes(zip_bytes)
    print(f"[OK] Zip salvo em {zip_path}")

    # extrair primeira camada dentro de PATH_TO_SAVE
    extract_dir_1 = PATH_TO_SAVE / f"pregao_{dt}"
    extract_dir_1.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(extract_dir_1)
    print(f"[OK] Primeira extração concluída em {extract_dir_1}")

    # localizar inner zip (PR{dt}.zip) e extrair dentro de PATH_TO_SAVE/PR{dt}
    inner_zip_path = extract_dir_1 / f"PR{dt}.zip"
    if not inner_zip_path.exists():
        raise FileNotFoundError(f"Inner zip não encontrado: {inner_zip_path}")

    extract_dir_2 = PATH_TO_SAVE / f"PR{dt}"
    extract_dir_2.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(inner_zip_path, "r") as zf:
        zf.extractall(extract_dir_2)
    print(f"[OK] Segunda extração concluída em {extract_dir_2}")

    # garantir que exista pelo menos um XML antes de enviar
    xml_files = list(extract_dir_2.glob("*.xml"))
    if not xml_files:
        raise RuntimeError(f"Nenhum arquivo XML encontrado em {extract_dir_2}")

    # upload: cria client uma vez e envia
    container = get_container_client()
    # opcional: enviar zip cru também
    # save_file_to_blob(container, f"raw/pregao_{dt}.zip", zip_path)

    uploaded = upload_directory_to_blob(container, extract_dir_2, blob_prefix=f"xml/{dt}/")
    print(f"[OK] {uploaded} arquivos XML enviados para o blob storage")

    print(f"[SUCCESS] Processo completo! Data processada: {dt}")

if __name__ == "__main__":
    run()