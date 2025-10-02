import os
from datetime import datetime
from helpers import yymmdd
import requests
import zipfile
from azure.storage.blob import BlobServiceClient

# Configurações
PATH_TO_SAVE = "./dados_b3"
AZURE_BLOB_CONNECTION = os.getenv("AZURE_STORAGE_CONNECTION_STRING", 
    "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://localhost:10000/devstoreaccount1;")
CONTAINER = "dados-pregao"

def build_url_download(date_to_download):
    return f"https://www.b3.com.br/pesquisapregao/download?filelist=PR{date_to_download}.zip"

def try_http_download(url):
    session = requests.Session()
    try:
        print(f"[INFO] Tentando {url}")
        resp = session.get(url, timeout=30)
        if (resp.ok) and resp.content and len(resp.content) > 200:
            if (resp.content[:2] == b"PK"):
                return resp.content, os.path.basename(url)
    except requests.RequestException:
        print(f"[ERROR] Falha ao acessar a {url}")
        pass

def save_file_to_blob(file_name, local_path_file):
    """Salva arquivo local no Azure Blob Storage"""
    try:
        service = BlobServiceClient.from_connection_string(AZURE_BLOB_CONNECTION)
        container = service.get_container_client(CONTAINER)
        
        # Criar container se não existir
        try:
            service.create_container(CONTAINER)
            print(f"[INFO] Container '{CONTAINER}' criado")
        except Exception:
            pass  # Container já existe
        
        # Upload do arquivo
        with open(local_path_file, "rb") as data:
            container.upload_blob(name=file_name, data=data, overwrite=True)
            print(f"[OK] Arquivo '{file_name}' enviado para o blob storage")
            
    except Exception as e:
        print(f"[ERROR] Falha ao enviar arquivo para blob: {e}")

def upload_directory_to_blob(directory_path, blob_prefix=""):
    """Faz upload de todos os arquivos de um diretório para o blob"""
    try:
        for root, dirs, files in os.walk(directory_path):
            for file in files:
                local_path = os.path.join(root, file)
                # Cria um nome do blob baseado na estrutura de diretórios
                relative_path = os.path.relpath(local_path, directory_path)
                blob_name = f"{blob_prefix}{relative_path}".replace("\\", "/")
                
                save_file_to_blob(blob_name, local_path)
                
    except Exception as e:
        print(f"[ERROR] Falha ao enviar diretório para blob: {e}")

def run():
    dt = yymmdd(datetime.now())
    url_to_download = build_url_download(dt)

    print(f"[INFO] Iniciando processo de extração para data: {dt}")

    # 1) Download do Zip
    zip_result = try_http_download(url_to_download)
    
    if not zip_result:
        print("[WARNING] Não foi possível baixar arquivo do dia atual, tentando data anterior...")
        # Tenta dia anterior (para testes quando o arquivo do dia atual não existe)
        from datetime import timedelta
        dt_anterior = yymmdd(datetime.now() - timedelta(days=1))
        url_to_download = build_url_download(dt_anterior)
        zip_result = try_http_download(url_to_download)
        dt = dt_anterior
        
    if not zip_result:
        raise RuntimeError("Não foi possível baixar o arquivo de cotações")
    
    zip_bytes, zip_name = zip_result
    print(f"[OK] Baixado arquivo de cotações: {zip_name}")

    # 2) Salvar o Zip localmente
    os.makedirs(PATH_TO_SAVE, exist_ok=True)
    zip_path = f"{PATH_TO_SAVE}/pregao_{dt}.zip"
    with open(zip_path, "wb") as f:
        f.write(zip_bytes)
    print(f"[OK] Zip salvo em {zip_path}")

    # 3) Upload do arquivo ZIP para o blob
    # save_file_to_blob(f"raw/pregao_{dt}.zip", zip_path)

    # 4) Extrair os arquivos do zip
    # Extrair primeira camada
    extract_dir_1 = f"pregao_{dt}"
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(extract_dir_1)
    print(f"[OK] Primeira extração concluída em {extract_dir_1}")

    # Extrair segunda camada
    extract_dir_2 = f"PR{dt}"
    inner_zip_path = f"{extract_dir_1}/PR{dt}.zip"
    if os.path.exists(inner_zip_path):
        with zipfile.ZipFile(inner_zip_path, "r") as zf:
            zf.extractall(extract_dir_2)
        print(f"[OK] Segunda extração concluída em {extract_dir_2}")
        
        # 5) Upload dos arquivos XML extraídos para o blob
        upload_directory_to_blob(extract_dir_2, f"xml/{dt}/")
        print(f"[OK] Arquivos XML enviados para o blob storage")
    else:
        print(f"[WARNING] Arquivo interno {inner_zip_path} não encontrado")

    print(f"[SUCCESS] Processo completo! Data processada: {dt}")

if __name__ == "__main__":
    run()