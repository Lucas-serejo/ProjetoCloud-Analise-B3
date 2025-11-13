import os
import psycopg2
from fastapi import FastAPI, HTTPException
from etl.common.config import Config  # Reutilizamos sua configuração

app = FastAPI()

def get_db_connection():
    # Conecta ao DB usando as variáveis de ambiente que o Azure vai fornecer
    try:
        conn = psycopg2.connect(
            host=Config.POSTGRES_HOST,
            port=Config.POSTGRES_PORT,
            dbname=Config.POSTGRES_DB,
            user=Config.POSTGRES_USER,
            password=Config.POSTGRES_PASSWORD
        )
        return conn
    except psycopg2.OperationalError as e:
        print(f"Erro ao conectar ao DB: {e}")
        # Se a API não pode conectar ao banco, ela retorna um erro 500
        raise HTTPException(status_code=500, detail="Não foi possível conectar ao banco de dados")

@app.get("/")
async def root():
    return {"message": "API de Cotações B3"}

@app.get("/api/cotacoes/{ticker}")
async def get_cotacao_por_ticker(ticker: str):
    """
    Busca as últimas 10 cotações para um ativo específico.
    """
    # Esta query busca as 10 cotações mais recentes para o ticker
    query = """
        SELECT ativo, data_pregao, abertura, fechamento, maximo, minimo, volume 
        FROM cotacoes 
        WHERE ativo = %s 
        ORDER BY data_pregao DESC 
        LIMIT 10
    """

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # Executa a query de forma segura
            cur.execute(query, (ticker.upper(),))
            rows = cur.fetchall()

            # Transforma as tuplas do banco em uma lista de dicionários
            results = [
                {
                    "ativo": r[0], 
                    "data_pregao": r[1], 
                    "abertura": r[2], 
                    "fechamento": r[3], 
                    "maximo": r[4], 
                    "minimo": r[5], 
                    "volume": r[6]
                }
                for r in rows
            ]

            if not results:
                raise HTTPException(status_code=404, detail="Ativo não encontrado")

            return {"ticker": ticker.upper(), "dados": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")
    finally:
        conn.close()