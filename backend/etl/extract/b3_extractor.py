import os
import requests
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from etl.common.helpers import yymmdd
from etl.common.config import Config
from etl.common.storage import get_container_client, upload_blob

# Gera datas em ordem decrescente, apenas dias úteis (seg-sex)
def iter_uteis_ate(max_days: int = 10, base: datetime | None = None):
    if base is None:
        base = datetime.now()
    for i in range(max_days):
        dt = base - timedelta(days=i)
        if dt.weekday() < 5:
            yield dt

class B3Extractor:
    def __init__(self):
        self.data_dir = Config.DATA_DIR
    
    def build_url(self, date_str):
        return f"https://www.b3.com.br/pesquisapregao/download?filelist=SPRE{date_str}.zip"
    
    def download_zip(self, date_str=None):
        if not date_str:
            date_str = yymmdd(datetime.now())
        
        url = self.build_url(date_str)
        session = requests.Session()
        # Define um User-Agent para evitar bloqueios eventuais
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0 Safari/537.36"
        })
        
        try:
            print(f"[INFO] Tentando {url}")
            resp = session.get(url, timeout=30)
            # Confirma que é um ZIP válido: começa com assinatura PK
            if resp.ok and resp.content and len(resp.content) > 200 and resp.content[:2] == b"PK":
                return resp.content, date_str
        except requests.RequestException:
            print(f"[ERROR] Falha ao acessar {url}")
        
        return None, None
    
    def extract_files(self, zip_bytes, date_str):
        # Extrai os arquivos do ZIP da B3.
        # Garantir que o diretório existe
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # Salvar o ZIP
        zip_path = self.data_dir / f"pregao_{date_str}.zip"
        zip_path.write_bytes(zip_bytes)
        print(f"[OK] Zip salvo em {zip_path}")
        
        # Extrair primeira camada
        extract_dir_1 = self.data_dir / f"pregao_{date_str}"
        extract_dir_1.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(extract_dir_1)
        print(f"[OK] Primeira extração concluída em {extract_dir_1}")
        
        # Extrair segunda camada
        inner_zip_path = extract_dir_1 / f"SPRE{date_str}.zip"
        if not inner_zip_path.exists():
            raise FileNotFoundError(f"Inner zip não encontrado: {inner_zip_path}")

        extract_dir_2 = self.data_dir / f"SPRE{date_str}"
        extract_dir_2.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(inner_zip_path, "r") as zf:
            zf.extractall(extract_dir_2)
        print(f"[OK] Segunda extração concluída em {extract_dir_2}")
        
        # Verificar se existem XMLs
        xml_files = list(extract_dir_2.glob("*.xml"))
        if not xml_files:
            raise RuntimeError(f"Nenhum arquivo XML encontrado em {extract_dir_2}")
            
        return {
            "date": date_str,
            "zip_path": zip_path,
            "xml_dir": extract_dir_2,
            "xml_files": xml_files
        }
    
    def execute(self):
        # Executa o processo de extração completo.
        # Percorre últimos dias úteis até encontrar um arquivo disponível
        zip_bytes = None
        date_str = None
        for dt in iter_uteis_ate(max_days=10):
            ds = yymmdd(dt)
            content, ok_date = self.download_zip(ds)
            if content:
                zip_bytes = content
                date_str = ok_date
                break
            else:
                print(f"[WARNING] Arquivo indisponível para {ds}, tentando dia útil anterior...")
        
        if not zip_bytes:
            raise RuntimeError("Não foi possível baixar o arquivo de cotações nos últimos dias úteis verificados")
        
        print(f"[OK] Baixado arquivo de cotações para {date_str}")
        
        # Extrai os arquivos
        result = self.extract_files(zip_bytes, date_str)
        print(f"[SUCCESS] Extração concluída! {len(result['xml_files'])} arquivos XML extraídos.")
        
        return result

    def run(self):
        extractor = B3Extractor()
        result = extractor.execute()
        
        if os.getenv("UPLOAD_TO_BLOB", "true").lower() == "true":
            from etl.common.storage import get_container_client, upload_blob
            
            print("[INFO] Iniciando upload para o Blob Storage...")
            container = get_container_client()
            
            # Upload dos XMLs
            xml_dir = result["xml_dir"]
            date_str = result["date"]
            uploaded = 0
            
            for xml_file in result["xml_files"]:
                relative_path = xml_file.relative_to(xml_dir)
                blob_name = f"xml/{date_str}/{relative_path}"
                if upload_blob(container, blob_name, xml_file):
                    uploaded += 1
            
            print(f"[OK] {uploaded} arquivos XML enviados para o blob storage")
        
        return result

if __name__ == "__main__":
    extractor = B3Extractor()
    extractor.run() 