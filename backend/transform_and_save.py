import os
import time
import psycopg2
from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient
import xml.etree.ElementTree as ET
from helpers import yymmdd

# --- Configurações ---
# Blob Storage
AZURE_BLOB_CONNECTION = os.getenv(
    "AZURE_STORAGE_CONNECTION_STRING",
    "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://azurite:10000/devstoreaccount1;"
)
CONTAINER = os.getenv("AZURE_BLOB_CONTAINER", "dados-pregao")

# PostgreSQL
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "b3_data")
POSTGRES_USER = os.getenv("POSTGRES_USER", "user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password")

def get_blob_client():
    """Retorna cliente do Blob Storage"""
    return BlobServiceClient.from_connection_string(AZURE_BLOB_CONNECTION)

def get_db_connection(retries=5, delay=5):
    """Tenta conectar ao PostgreSQL com múltiplas tentativas."""
    for i in range(retries):
        try:
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                dbname=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD
            )
            print("[INFO] Conexão com o PostgreSQL estabelecida com sucesso.")
            return conn
        except psycopg2.OperationalError as e:
            print(f"[WARNING] Tentativa {i+1}/{retries}: Falha ao conectar ao PostgreSQL: {e}")
            if i < retries - 1:
                print(f"Aguardando {delay} segundos para tentar novamente...")
                time.sleep(delay)
            else:
                print("[ERROR] Não foi possível conectar ao PostgreSQL após várias tentativas.")
                raise



def list_xml_blobs(blob_prefix):
    """Lista todos os arquivos XML no blob storage com o prefixo especificado"""
    try:
        service = get_blob_client()
        container = service.get_container_client(CONTAINER)
        
        xml_blobs = [blob.name for blob in container.list_blobs(name_starts_with=blob_prefix) if blob.name.endswith('.xml')]
        print(f"[INFO] Encontrados {len(xml_blobs)} arquivos XML com prefixo '{blob_prefix}'")
        return xml_blobs
        
    except Exception as e:
        print(f"[ERROR] Falha ao listar blobs: {e}")
        return []

def download_blob_content(blob_name):
    """Baixa conteúdo de um blob"""
    try:
        service = get_blob_client()
        container = service.get_container_client(CONTAINER)
        
        blob_data = container.download_blob(blob_name)
        return blob_data.readall()
        
    except Exception as e:
        print(f"[ERROR] Falha ao baixar blob {blob_name}: {e}")
        return None

def parse_b3_xml(xml_content):
    """Extrai dados de cotações dos arquivos XML da B3."""
    try:
        root = ET.fromstring(xml_content)
        namespace = {'': 'urn:bvmf.052.01.xsd'} # Namespace comum em arquivos da B3
        
        cotacoes = []
        
        # Encontra a data do pregão no cabeçalho do arquivo
        tradg_dt_element = root.find('.//BizGrp/Hdr/TradgDt/Dt', namespace)
        if tradg_dt_element is None:
            print("[WARNING] Não foi possível encontrar a data do pregão no XML.")
            return []
        
        data_pregao_str = tradg_dt_element.text
        data_pregao = datetime.strptime(data_pregao_str, '%Y-%m-%d').date()

        # Itera sobre os instrumentos (ações)
        for instmt in root.findall('.//BizGrp/Document/PricRpt/Trad/Instmt', namespace):
            try:
                ativo = instmt.find('TckrSymb', namespace).text
                
                cotacao = {
                    'ativo': ativo,
                    'data_pregao': data_pregao,
                    'abertura': float(instmt.find('OpnPx', namespace).text),
                    'fechamento': float(instmt.find('ClsPx', namespace).text),
                    'maximo': float(instmt.find('HghstPx', namespace).text),
                    'minimo': float(instmt.find('LwstPx', namespace).text),
                    'volume': int(float(instmt.find('FinInstrmQty', namespace).text))
                }
                cotacoes.append(cotacao)
            except (AttributeError, ValueError) as e:
                # Pula o instrumento se algum campo essencial estiver faltando ou for inválido
                # print(f"[WARNING] Pulando instrumento por dados ausentes ou inválidos: {e}")
                continue
                
        return cotacoes
        
    except Exception as e:
        print(f"[ERROR] Falha ao processar XML: {e}")
        return []

def save_to_postgres(conn, cotacoes):
    """Salva uma lista de cotações no PostgreSQL usando INSERT ... ON CONFLICT."""
    if not cotacoes:
        return 0
        
    saved_count = 0
    
    with conn.cursor() as cur:
        for cotacao in cotacoes:
            try:
                cur.execute("""
                    INSERT INTO cotacoes (ativo, data_pregao, abertura, fechamento, maximo, minimo, volume)
                    VALUES (%(ativo)s, %(data_pregao)s, %(abertura)s, %(fechamento)s, %(maximo)s, %(minimo)s, %(volume)s)
                    ON CONFLICT (ativo, data_pregao) DO UPDATE SET
                        abertura = EXCLUDED.abertura,
                        fechamento = EXCLUDED.fechamento,
                        maximo = EXCLUDED.maximo,
                        minimo = EXCLUDED.minimo,
                        volume = EXCLUDED.volume,
                        timestamp_processamento = NOW();
                """, cotacao)
                saved_count += 1
            except Exception as e:
                print(f"[ERROR] Falha ao salvar cotação para {cotacao.get('ativo')}: {e}")
                conn.rollback() # Desfaz a transação em caso de erro
                raise
    
    conn.commit()
    return saved_count

def run():
    """Orquestra o processo de transformação e carga."""
    print("[INFO] Iniciando processo de transformação e carga...")
    
    # 1) Configurar a tabela no PostgreSQL
    try:
        # A configuração do banco de dados agora é gerenciada pelo Alembic.
        pass
    except Exception:
        print("[FATAL] Saindo devido a falha na configuração do banco de dados.")
        return

    # 2) Tentar encontrar arquivos XML para a data de hoje ou de ontem
    dt_hoje = yymmdd(datetime.now())
    dt_ontem = yymmdd(datetime.now() - timedelta(days=1))
    
    xml_prefix_hoje = f"xml/{dt_hoje}/"
    xml_prefix_ontem = f"xml/{dt_ontem}/"
    
    xml_blobs = list_xml_blobs(xml_prefix_hoje)
    if not xml_blobs:
        print(f"[INFO] Nenhum arquivo XML encontrado para hoje. Tentando para ontem ({dt_ontem})...")
        xml_blobs = list_xml_blobs(xml_prefix_ontem)

    if not xml_blobs:
        print("[ERROR] Nenhum arquivo XML encontrado para hoje ou ontem. Saindo.")
        return
    
    total_cotacoes_salvas = 0
    
    # 3) Processar cada arquivo XML
    try:
        with get_db_connection() as conn:
            for xml_blob in xml_blobs:
                print(f"[INFO] Processando {xml_blob}...")
                
                xml_content = download_blob_content(xml_blob)
                if not xml_content:
                    continue
                    
                cotacoes = parse_b3_xml(xml_content)
                
                if cotacoes:
                    saved = save_to_postgres(conn, cotacoes)
                    total_cotacoes_salvas += saved
                    print(f"[OK] {saved} cotações salvas/atualizadas de {xml_blob}")
                else:
                    print(f"[WARNING] Nenhuma cotação válida extraída de {xml_blob}")
    
    except Exception as e:
        print(f"[FATAL] Erro durante o processamento dos blobs: {e}")
        return

    print(f"\n[SUCCESS] Processo completo! Total de cotações salvas/atualizadas: {total_cotacoes_salvas}")

if __name__ == "__main__":
    run()