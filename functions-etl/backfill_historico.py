"""
Script de Backfill - Carrega dados históricos dos últimos 30 dias da B3

Execução:
    python backfill_historico.py

Configuração:
    Certifique-se de ter as variáveis de ambiente configuradas:
    - POSTGRES_HOST, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB
    - AZURE_STORAGE_CONNECTION_STRING
    - AZURE_BLOB_CONTAINER
"""

import os
import sys
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from dotenv import load_dotenv

# Carregar variáveis de ambiente
load_dotenv()

# Importar módulos ETL
from etl.extract.b3_extractor import B3Extractor
from etl.transform.xml_parse import B3XMLParser
from etl.load.postgres_loader import PostgresLoader
from etl.common.storage import get_container_client, upload_blob
from etl.common.helpers import yymmdd

def is_dia_util(data: datetime) -> bool:
    """Verifica se é dia útil (segunda a sexta)"""
    return data.weekday() < 5

def backfill_historico(dias_atras: int = 30, max_workers: int = 4):
    """
    Processa dados históricos dos últimos N dias
    
    Args:
        dias_atras: Número de dias para retroceder (padrão: 30)
    """
    print(f"🚀 Iniciando backfill de {dias_atras} dias...")
    print(f"📅 Data de referência: {datetime.now().strftime('%Y-%m-%d')}\n")
    
    # Inicializar componentes de infraestrutura
    container_client = get_container_client()
    
    # Limpar banco de dados antes de começar
    print("🗑️  Limpando banco de dados...")
    try:
        _loader = PostgresLoader()
        _loader.truncate_table()
        _loader.disconnect()
        print("✅ Banco de dados esvaziado com sucesso!\n")
    except Exception as e:
        print(f"❌ ERRO ao limpar banco: {str(e)}")
        print("⚠️  Continuando mesmo assim...\n")
    
    # Calcular intervalo de datas
    data_fim = datetime.now() - timedelta(days=1)  # Ontem
    data_inicio = data_fim - timedelta(days=dias_atras)
    
    print(f"📊 Período: {data_inicio.strftime('%d/%m/%Y')} até {data_fim.strftime('%d/%m/%Y')}\n")
    
    # Montar lista de dias úteis no período
    datas = []
    data_atual = data_inicio
    while data_atual <= data_fim:
        if is_dia_util(data_atual):
            datas.append(data_atual)
        data_atual += timedelta(days=1)

    print(f"🧵 Rodando em paralelo com até {max_workers} workers...\n")

    total_dias = len(datas)
    dias_processados = 0
    dias_pulados = 0
    total_cotacoes = 0
    erros = []

    def processar_dia(data_ref: datetime):
        data_str = data_ref.strftime('%Y-%m-%d')
        dia_semana = data_ref.strftime('%A')
        print(f"\n{'='*70}")
        print(f"📅 Processando: {data_str} ({dia_semana})")
        print(f"{'='*70}")

        extractor_local = B3Extractor()
        parser_local = B3XMLParser()
        loader_local = PostgresLoader()
        try:
            # 1. EXTRACT - download
            print(f"🌐 [1/4] Extraindo dados da B3...")
            date_str_download = yymmdd(data_ref)
            zip_bytes, ok_date = extractor_local.download_zip(date_str_download)
            if not zip_bytes:
                print(f"⚠️  Arquivo não encontrado na B3 para {data_str}")
                return {"ok": False, "cotacoes": 0, "msg": f"{data_str}: Arquivo não disponível"}

            # Extrair
            result = extractor_local.extract_files(zip_bytes, date_str_download)
            xml_files = result['xml_files']
            print(f"✅ {len(xml_files)} arquivo(s) XML extraído(s)")

            # 2. UPLOAD - Blob com paralelismo interno
            print(f"☁️  [2/4] Enviando para Azure Blob Storage...")
            uploaded = 0
            for xml_file in xml_files:
                relative_path = xml_file.relative_to(result['xml_dir'])
                blob_name = f"xml/{date_str_download}/{relative_path}"
                if upload_blob(container_client, blob_name, xml_file, max_concurrency=8, content_type='application/xml', skip_if_exists=True):
                    uploaded += 1
            print(f"✅ {uploaded} arquivo(s) enviado(s) para o blob")

            # 3. TRANSFORM - parse
            print(f"🔄 [3/4] Processando XML...")
            all_cotacoes = []
            for xml_file in xml_files:
                with open(xml_file, 'r', encoding='utf-8') as f:
                    xml_content = f.read()
                cotacoes = parser_local.parse_xml(xml_content)
                all_cotacoes.extend(cotacoes)
            num_cotacoes = len(all_cotacoes)
            print(f"✅ Extraídas {num_cotacoes:,} cotações válidas")

            # 4. LOAD - banco
            print(f"💾 [4/4] Carregando no PostgreSQL...")
            loader_local.load_cotacoes(all_cotacoes)
            print(f"✅ {num_cotacoes:,} cotações inseridas/atualizadas no banco")

            # Limpeza
            import shutil
            if result['zip_path'].exists():
                result['zip_path'].unlink()
            if result['xml_dir'].exists():
                shutil.rmtree(result['xml_dir'])

            return {"ok": True, "cotacoes": num_cotacoes}
        except Exception as e:
            return {"ok": False, "cotacoes": 0, "msg": f"{data_str}: {str(e)}"}
        finally:
            loader_local.disconnect()

    # Executar em paralelo
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {executor.submit(processar_dia, dt): dt for dt in datas}
        for future in as_completed(future_map):
            res = future.result()
            if res.get("ok"):
                dias_processados += 1
                total_cotacoes += res.get("cotacoes", 0)
            else:
                dias_pulados += 1
                erros.append(res.get("msg", "Erro desconhecido"))
    
    # Resumo final
    print(f"\n{'='*70}")
    print(f"📊 RESUMO DO BACKFILL")
    print(f"{'='*70}")
    print(f"📅 Total de dias no período: {total_dias}")
    print(f"✅ Dias processados com sucesso: {dias_processados}")
    print(f"⏭️  Dias pulados (fim de semana/feriado): {dias_pulados}")
    print(f"📈 Total de cotações carregadas: {total_cotacoes:,}")
    
    if total_cotacoes > 0 and dias_processados > 0:
        media = total_cotacoes / dias_processados
        print(f"📊 Média de cotações por dia: {media:.0f}")
    
    if erros:
        print(f"\n⚠️  ERROS ENCONTRADOS ({len(erros)}):")
        for erro in erros:
            print(f"   - {erro}")
    else:
        print(f"\n🎉 Backfill concluído sem erros!")
    
    print(f"\n{'='*70}\n")
    
    # Nada a fechar aqui (cada worker fechou sua conexão)

if __name__ == "__main__":
    # Verificar variáveis de ambiente
    required_vars = [
        'POSTGRES_HOST', 'POSTGRES_USER', 'POSTGRES_PASSWORD', 'POSTGRES_DB',
        'AZURE_STORAGE_CONNECTION_STRING', 'AZURE_BLOB_CONTAINER'
    ]
    
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print("❌ ERRO: Variáveis de ambiente faltando:")
        for var in missing_vars:
            print(f"   - {var}")
        print("\n💡 Configure as variáveis no arquivo .env ou no ambiente do sistema")
        sys.exit(1)
    
    # Executar backfill
    try:
        backfill_historico(dias_atras=30)
    except KeyboardInterrupt:
        print("\n\n⚠️  Backfill interrompido pelo usuário")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ ERRO FATAL: {str(e)}")
        sys.exit(1)
