#!/usr/bin/env python3
# Script para testar o pipeline ETL completo

import os
import psycopg2
from datetime import datetime
from azure.storage.blob import BlobServiceClient

# Azurite
AZURE_BLOB_CONNECTION = os.getenv(
    "AZURE_STORAGE_CONNECTION_STRING", 
    "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://azurite:10000/devstoreaccount1;"
)
CONTAINER = os.getenv("AZURE_BLOB_CONTAINER", "dados-pregao")

# PostgreSQL
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "b3_data")
POSTGRES_USER = os.getenv("POSTGRES_USER", "user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password")


def check_azurite():
    # Verifica se Azurite está disponível
    try:
        print("\n=== TESTE DE CONEXÃO COM AZURITE ===")
        client = BlobServiceClient.from_connection_string(AZURE_BLOB_CONNECTION)
        containers = client.list_containers()
        print("✅ Azurite está rodando! Containers disponíveis:")
        for container in containers:
            print(f"  • {container.name}")
        return True
    except Exception as e:
        print(f"❌ Erro ao conectar ao Azurite: {str(e)}")
        return False


def list_blobs():
    # Lista blobs no container especificado
    try:
        print("\n=== BLOBS DISPONÍVEIS EM DADOS-PREGAO ===")
        client = BlobServiceClient.from_connection_string(AZURE_BLOB_CONNECTION)
        container_client = client.get_container_client(CONTAINER)
        
        # Lista blobs e organiza por prefixo para facilitar a visualização
        blobs_by_prefix = {}
        for blob in container_client.list_blobs():
            prefix = blob.name.split('/')[0] if '/' in blob.name else '(raiz)'
            if prefix not in blobs_by_prefix:
                blobs_by_prefix[prefix] = []
            blobs_by_prefix[prefix].append(blob.name)
        
        if not blobs_by_prefix:
            print("❗ Nenhum blob encontrado no container")
            return False
            
        # Imprime blobs agrupados por prefixo
        for prefix, blobs in blobs_by_prefix.items():
            print(f"\nPrefixo '{prefix}':")
            # Mostra só os primeiros 5 de cada grupo
            for i, blob in enumerate(sorted(blobs)[:5]):  
                print(f"  • {blob}")
            if len(blobs) > 5:
                print(f"  ... e mais {len(blobs) - 5} arquivo(s)")
                
        return True
    except Exception as e:
        print(f"❌ Erro ao listar blobs: {str(e)}")
        return False


def check_postgres():
    # Verifica se PostgreSQL está disponível
    try:
        print("\n=== TESTE DE CONEXÃO COM POSTGRES ===")
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        conn.close()
        print("✅ PostgreSQL está conectado e funcionando!")
        return True
    except Exception as e:
        print(f"❌ Erro ao conectar ao PostgreSQL: {str(e)}")
        return False


def list_postgres_data():
    # Lista os dados na tabela cotacoes
    try:
        print("\n=== DADOS NO POSTGRESQL ===")
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        cur = conn.cursor()
        
        # Verifica se a tabela existe
        cur.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'cotacoes')")
        if not cur.fetchone()[0]:
            print("❗ A tabela 'cotacoes' ainda não foi criada")
            conn.close()
            return False
        
        # Conta registros
        cur.execute("SELECT COUNT(*) FROM cotacoes")
        count = cur.fetchone()[0]
        print(f"Total de registros na tabela cotacoes: {count}")
        
        if count == 0:
            print("❗ A tabela 'cotacoes' está vazia - pipeline ainda não completou")
            conn.close()
            return False
        
        # Mostra os últimos registros
        print("\nÚltimos 3 registros:")
        cur.execute("""
            SELECT ativo, data_pregao, abertura, fechamento, volume 
            FROM cotacoes 
            ORDER BY data_pregao DESC, ativo 
            LIMIT 3
        """)
        for row in cur.fetchall():
            ativo, data, abertura, fechamento, volume = row
            print(f"  • {ativo} ({data}): Abertura={abertura}, Fechamento={fechamento}, Volume={volume}")
        
        conn.close()
        return True
    except Exception as e:
        print(f"❌ Erro ao listar dados do PostgreSQL: {str(e)}")
        return False


def main():
    # Teste do pipeline completo
    print("🔍 VERIFICAÇÃO DO PIPELINE B3 → BLOB → POSTGRES")
    print(f"Data/Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Testes
    azurite_ok = check_azurite()
    if azurite_ok:
        list_blobs()
    
    postgres_ok = check_postgres()
    if postgres_ok:
        list_postgres_data()
    
    # Resumo
    print("\n=== RESUMO DOS TESTES ===")
    print(f"Azurite/Blob Storage: {'✅ OK' if azurite_ok else '❌ FALHA'}")
    print(f"PostgreSQL: {'✅ OK' if postgres_ok else '❌ FALHA'}")
    
    if azurite_ok and postgres_ok:
        print("\n🎉 Ambiente básico está pronto! Se os dados ainda não apareceram,")
        print("verifique logs dos serviços 'extractor' e 'transformer'.")
    else:
        print("\n❌ Algum componente não está funcionando corretamente.")
        print("Verifique os logs dos contêineres para mais detalhes.")


if __name__ == "__main__":
    main()