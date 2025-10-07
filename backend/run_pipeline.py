#!/usr/bin/env python3
"""
Script para executar o pipeline ETL completo
"""
from etl.extract.b3_extractor import B3Extractor
from etl.transform.xml_parse import B3XMLParser
from etl.load.postgres_loader import PostgresLoader
from etl.common.config import Config

def run_pipeline():
    print("=== INICIANDO PIPELINE ETL B3 ===")
    
    # Fase 1: Extração
    print("\n--- FASE 1: EXTRAÇÃO ---")
    extractor = B3Extractor()
    extraction_result = extractor.run(
        multi_day=Config.MULTI_DAY_PROCESSING,
        days_limit=Config.MULTI_DAY_LIMIT
    )
    print(f"Extração concluída! {len(extraction_result['xml_files'])} arquivos XML extraídos.")
    
    # Fase 2: Transformação
    print("\n--- FASE 2: TRANSFORMAÇÃO ---")
    parser = B3XMLParser()
    cotacoes = parser.execute(
        multi_day=Config.MULTI_DAY_PROCESSING,
        days_limit=Config.MULTI_DAY_LIMIT
    )
    print(f"Transformação concluída! {len(cotacoes)} cotações extraídas.")
    
    # Fase 3: Carga
    print("\n--- FASE 3: CARGA ---")
    loader = PostgresLoader()
    total_loaded = loader.execute(cotacoes)
    print(f"Carga concluída! {total_loaded} registros processados.")
    
    print("\n=== PIPELINE ETL B3 CONCLUÍDO COM SUCESSO! ===")

if __name__ == "__main__":
    run_pipeline()