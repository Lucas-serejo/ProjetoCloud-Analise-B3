import logging
import azure.functions as func
import io
import zipfile
from datetime import datetime, timedelta

# Importações da lógica ETL
from b3_extractor import B3Extractor
from storage import get_container_client
from helpers import yymmdd
from xml_parse import B3XMLParser
from postgres_loader import PostgresLoader
from config import Config

# Configura logging
logging.basicConfig(level=logging.INFO)

# Inicializa a Function App
app = func.FunctionApp()

# Timer Trigger: baixa ZIP do dia útil e envia XMLs ao Blob
@app.timer_trigger(schedule="0 0 1 * * 2-6", arg_name="mytimer", run_on_startup=True,
                   use_monitor=True)
def ExtractorTimer(mytimer: func.TimerRequest) -> None:
    """Baixa ZIP da B3 e envia XMLs ao Blob."""
    if mytimer.past_due:
        logging.warning('Timer está executando com atraso.')

    logging.info('=== INICIANDO FUNÇÃO ExtractorTimer ===')

    try:
        extractor = B3Extractor()
        
        zip_bytes = None
        date_str = None

        logging.info('Procurando arquivo nos últimos dias úteis...')
        # Use a flag para decidir quantos dias tentar
        if Config.MULTI_DAY_PROCESSING:
            dias = Config.MULTI_DAY_LIMIT
        else:
            dias = 1

        for dt in iter_uteis_ate(max_days=dias):
            ds = yymmdd(dt)
            logging.info(f"Tentando baixar para data: {ds}")
            content, ok_date = extractor.download_zip(ds)
            if content:
                zip_bytes = content
                date_str = ok_date
                logging.info(f"✅ Arquivo baixado com sucesso para: {date_str}")
                break
            else:
                logging.info(f"Arquivo não disponível para {ds}")
        
        if not zip_bytes:
            logging.error("❌ Nenhum arquivo encontrado nos últimos 5 dias úteis.")
            return

        # Extrai XMLs e envia ao Blob
        container_client = get_container_client()
        
        logging.info('Extraindo arquivos do ZIP...')
        with zipfile.ZipFile(io.BytesIO(zip_bytes), "r") as zf1:
            # Primeira camada
            inner_zip_name = zf1.namelist()[0]
            inner_zip_bytes = zf1.read(inner_zip_name)
            
            # Segunda camada (XMLs)
            with zipfile.ZipFile(io.BytesIO(inner_zip_bytes), "r") as zf2:
                xml_files = [f for f in zf2.namelist() if f.endswith('.xml')]
                logging.info(f"Encontrados {len(xml_files)} arquivos XML")
                
                uploaded = 0
                for xml_file_name in xml_files:
                    xml_content = zf2.read(xml_file_name)
                    blob_name = f"xml/{date_str}/{xml_file_name}"
                    
                    try:
                        blob_client = container_client.get_blob_client(blob_name)
                        blob_client.upload_blob(xml_content, overwrite=True)
                        uploaded += 1
                        logging.info(f"✅ Upload: {blob_name}")
                    except Exception as e:
                        logging.error(f"❌ Falha no upload {blob_name}: {e}")
        
        logging.info(f'=== EXTRAÇÃO CONCLUÍDA: {uploaded}/{len(xml_files)} arquivos enviados ===')

    except Exception as e:
        logging.error(f"❌ ERRO FATAL na ExtractorTimer: {e}")
        import traceback
        logging.error(traceback.format_exc())


# Gera datas úteis (seg-sex) em ordem decrescente
def iter_uteis_ate(max_days: int = 10, base: datetime = None):
    """Retorna até max_days datas úteis a partir de base."""
    if base is None:
        base = datetime.now()
    for i in range(max_days):
        dt = base - timedelta(days=i)
        if dt.weekday() < 5:  # 0=seg, 4=sex
            yield dt

# Blob Trigger: processa XML adicionado ao Blob e carrega no Postgres
@app.blob_trigger(arg_name="myblob",
                  path="dados-pregao/xml/{date}/{name}.xml",  # <-- CORRIGIDO: removido "dados-pregao/"
                  connection="AzureWebJobsStorage")
def LoaderBlobTrigger(myblob: func.InputStream):
    """Extrai cotações do XML e carrega no PostgreSQL."""
    logging.info(f'=== INICIANDO PROCESSAMENTO DO BLOB: {myblob.name} ===')
    
    try:
        # Lê conteúdo do XML
        xml_content = myblob.read()
        if not xml_content:
            logging.warning(f"⚠️ Blob vazio: {myblob.name}")
            return

        logging.info(f"Tamanho do arquivo: {len(xml_content)} bytes")

        # Parse de XML para cotações
        parser = B3XMLParser()
        cotacoes = parser.parse_xml(xml_content)
        
        if not cotacoes:
            logging.warning(f"⚠️ Nenhuma cotação válida encontrada em {myblob.name}")
            return
            
        logging.info(f"✅ Extraídas {len(cotacoes)} cotações válidas")

        # Carrega no PostgreSQL
        loader = PostgresLoader()
        total_loaded = loader.execute(cotacoes)
        
        logging.info(f'=== CARGA CONCLUÍDA: {total_loaded} registros processados ===')

    except Exception as e:
        logging.error(f"❌ ERRO FATAL no processamento de {myblob.name}: {e}")
        import traceback
        logging.error(traceback.format_exc())