"""
Script de inicialização da API
Roda migrações automaticamente antes de iniciar o servidor
"""
import subprocess
import sys
import os
from dotenv import load_dotenv

# Carrega variáveis de ambiente
load_dotenv()

def run_migrations():
    """Executa migrações do Alembic"""
    print("🔄 Executando migrações do banco de dados...")
    try:
        result = subprocess.run(
            ["alembic", "upgrade", "head"],
            capture_output=True,
            text=True,
            check=True
        )
        print("✅ Migrações aplicadas com sucesso!")
        if result.stdout:
            print(result.stdout)
        return True
    except subprocess.CalledProcessError as e:
        print("❌ Erro ao executar migrações:")
        print(e.stderr)
        return False
    except FileNotFoundError:
        print("⚠️ Alembic não encontrado. Pulando migrações...")
        return True

def start_server():
    """Inicia o servidor Uvicorn"""
    print("🚀 Iniciando servidor...")
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8000"))
    
    subprocess.run([
        "uvicorn",
        "app.main:app",
        "--host", host,
        "--port", str(port),
        "--reload"
    ])

if __name__ == "__main__":
    # Roda migrações
    if not run_migrations():
        print("⚠️ Continuando mesmo com erros nas migrações...")
    
    # Inicia servidor
    start_server()
