import logging
import azure.functions as func
import io
import zipfile
import datetime

# --- Importações da nossa lógica de negócio (da pasta 'etl' que copiámos) ---
from etl.extract.b3_extractor import B3Extractor
from etl.common.storage import get_container_client
from etl.common.helpers import yymmdd
from etl.transform.xml_parse import B3XMLParser
from etl.load.postgres_loader import PostgresLoader
# ---

# Inicializa a aplicação de funções (como o seu ficheiro já tinha)
app = func.FunctionApp()

#================================================================================
# FUNÇÃO 1: Extração (Timer Trigger)
#================================================================================
@app.timer_trigger(schedule="0 0 21 * * 1-5", arg_name="mytimer", run_on_startup=False) 
    # NOTA: "0 0 21 * * 1-5" = 21:00 UTC, de Segunda a Sexta.
    # Para testar, pode usar: "0 */5 * * * *" (a cada 5 minutos)
def ExtractorTimer(mytimer: func.TimerRequest) -> None:
    
    if mytimer.past_due:
        logging.info('A função ExtractorTimer está a ser executada depois do esperado.')

    logging.info('Função ExtractorTimer iniciada.')

    try:
        extractor = B3Extractor()
        
        # 1. Tentar baixar os dados do dia útil mais recente
        zip_bytes = None
        date_str = None
        for dt in extractor.iter_uteis_ate(max_days=5):
            ds = yymmdd(dt)
            content, ok_date = extractor.download_zip(ds)
            if content:
                zip_bytes = content
                date_str = ok_date
                logging.info(f"Sucesso ao baixar o ZIP para a data: {date_str}")
                break
        
        if not zip_bytes:
            logging.warning("Nenhum ficheiro ZIP encontrado nos últimos 5 dias úteis.")
            return

        # 2. Extrair XMLs do ZIP e fazer o upload para o Blob Storage
        container_client = get_container_client()
        
        with zipfile.ZipFile(io.BytesIO(zip_bytes), "r") as zf1:
            inner_zip_name = zf1.namelist()[0] # Ex: SPRE{date_str}.zip
            inner_zip_bytes = zf1.read(inner_zip_name)
            
            with zipfile.ZipFile(io.BytesIO(inner_zip_bytes), "r") as zf2:
                xml_files = [f for f in zf2.namelist() if f.endswith('.xml')]
                logging.info(f"Encontrados {len(xml_files)} ficheiros XML dentro do ZIP.")
                
                for xml_file_name in xml_files:
                    xml_content = zf2.read(xml_file_name)
                    
                    # Definir o nome do blob (ex: xml/251112/SPRE251112_001.xml)
                    blob_name = f"xml/{date_str}/{xml_file_name}"
                    
                    try:
                        blob_client = container_client.get_blob_client(blob_name)
                        blob_client.upload_blob(xml_content, overwrite=True)
                        logging.info(f"Upload do ficheiro {blob_name} concluído.")
                    except Exception as e:
                        logging.error(f"Falha no upload do {blob_name}: {e}")
        
        logging.info('Função ExtractorTimer concluída com sucesso.')

    except Exception as e:
        logging.error(f"Erro fatal na ExtractorTimer: {e}")
        import traceback
        traceback.print_exc()


#================================================================================
# FUNÇÃO 2: Carga (Blob Trigger)
#================================================================================
@app.blob_trigger(arg_name="myblob", 
                  path="dados-pregao/xml/{name}.xml",
                  connection="AzureWebJobsStorage") 
def LoaderBlobTrigger(myblob: func.InputStream):
    
    logging.info(f"Função LoaderBlobTrigger processando o blob: {myblob.name}")
    
    try:
        # O myblob.read() dá-nos o conteúdo do ficheiro
        xml_content = myblob.read()
        if not xml_content:
            logging.warning(f"Blob {myblob.name} está vazio.")
            return

        # 1. Fazer o Parse do XML
        parser = B3XMLParser()
        cotacoes = parser.parse_xml(xml_content)
        
        if not cotacoes:
            logging.warning(f"Nenhuma cotação válida encontrada em {myblob.name}")
            return
            
        logging.info(f"Extraídas {len(cotacoes)} cotações de {myblob.name}")

        # 2. Carregar no Banco de Dados
        loader = PostgresLoader()
        total_loaded = loader.execute(cotacoes)
        
        logging.info(f"Carga concluída! {total_loaded} registos processados.")

    except Exception as e:
        logging.error(f"Erro fatal no processamento do {myblob.name}: {e}")
        import traceback
        traceback.print_exc()