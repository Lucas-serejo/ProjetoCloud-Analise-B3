#!/usr/bin/env python3
"""
Script para executar o pipeline ETL completo
"""
from etl.extract.b3_extractor import B3Extractor
from etl.transform.xml_parse import B3XMLParser
from etl.load.postgres_loader import PostgresLoader
from etl.common.config import Config
from etl.common.helpers import yymmdd
from datetime import datetime, timedelta
import time
import os

TODAY = datetime.now()

def run_pipeline_for_date(date_str):
    """Executa o pipeline ETL completo para uma data específica"""
    print(f"\n=== INICIANDO PIPELINE ETL B3 PARA {date_str} ===\n", flush=True)
    start_time = time.time()
    
    # Fase 1: Extração
    print(f"--- FASE 1: EXTRAÇÃO ({date_str}) ---", flush=True)
    extractor = B3Extractor()
    try:
        zip_bytes, ok_date = extractor.download_zip(date_str)
        if not zip_bytes:
            print(f"[WARNING] Nenhum arquivo disponível para {date_str}", flush=True)
            return 0
            
        extraction_result = extractor.extract_files(zip_bytes, ok_date)
        if os.getenv("UPLOAD_TO_BLOB", "true").lower() == "true":
            extractor.upload_to_blob(extraction_result)
            
        print(f"Extração concluída para {date_str}! {len(extraction_result['xml_files'])} arquivos XML extraídos.", flush=True)
        
        # Fase 2: Transformação
        print(f"\n--- FASE 2: TRANSFORMAÇÃO ({date_str}) ---", flush=True)
        parser = B3XMLParser()
        xml_files = parser.list_xml_files(date_str)
        cotacoes = parser._process_date(date_str, xml_files)
        print(f"Transformação concluída para {date_str}! {len(cotacoes)} cotações extraídas.", flush=True)
        
        # Fase 3: Carga
        print(f"\n--- FASE 3: CARGA ({date_str}) ---", flush=True)
        loader = PostgresLoader()
        total_loaded = loader.execute(cotacoes)
        print(f"Carga concluída para {date_str}! {total_loaded} registros processados.", flush=True)
        
        elapsed = time.time() - start_time
        print(f"\n=== PIPELINE ETL B3 PARA {date_str} CONCLUÍDO EM {elapsed:.2f}s! ===\n", flush=True)
        return total_loaded
    except Exception as e:
        print(f"[ERROR] Falha no processamento para {date_str}: {str(e)}", flush=True)
        import traceback
        traceback.print_exc()
        return 0

def run_pipeline():
    """Executa o pipeline ETL para um ou mais dias"""
    print("=== INICIANDO PIPELINE ETL B3 ===", flush=True)
    start_time = time.time()
    total_records = 0
    days_processed = 0
    
    # Se modo multi_day, processa cada dia individualmente
    if Config.MULTI_DAY_PROCESSING:
        dates_to_process = []
        for i in range(0, Config.MULTI_DAY_LIMIT):
            dt = datetime.now() - timedelta(days=i)
            if dt > TODAY:
                continue
            if dt.weekday() >= 5:
                continue
            dates_to_process.append(yymmdd(dt))
            
        print(f"[INFO] Modo multi-dia ativado. Processando {len(dates_to_process)} dias: {', '.join(dates_to_process)}", flush=True)
        
        for date_str in dates_to_process:
            records = run_pipeline_for_date(date_str)
            if records > 0:
                total_records += records
                days_processed += 1
    
    # Modo padrão - processa apenas um dia
    else:
        # Tenta processar dias recentes até encontrar um disponível
        for i in range(10):
            dt = datetime.now() - timedelta(days=i)
            if dt > TODAY:
                continue
            if dt.weekday() >= 5:
                continue
            
            date_str = yymmdd(dt)
            records = run_pipeline_for_date(date_str)
            if records > 0:
                total_records = records
                days_processed = 1
                break
    
    elapsed = time.time() - start_time
    print(f"\n=== PIPELINE ETL B3 CONCLUÍDO! ===", flush=True)
    print(f"Total de dias processados: {days_processed}", flush=True)
    print(f"Total de registros: {total_records}", flush=True)
    print(f"Tempo total de execução: {elapsed:.2f}s", flush=True)

if __name__ == "__main__":
    run_pipeline()