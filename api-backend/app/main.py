import os
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional
from datetime import date
from dotenv import load_dotenv

from app.database import get_db
from app.models import Cotacao

# Carrega variáveis de ambiente
load_dotenv()

# Inicializa FastAPI
app = FastAPI(
    title="B3 Cotações API",
    version="1.0.0",
    description="API para consulta de cotações da B3"
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/api/cotacoes/datas")
def listar_datas_disponiveis():
    """Retorna as datas disponíveis (distintas) com cotações, em ordem crescente."""
    try:
        query = """
            SELECT data_pregao::date AS data, COUNT(*) AS total
            FROM cotacoes
            GROUP BY data_pregao
            ORDER BY data_pregao ASC
        """

        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                rows = cur.fetchall()

                if not rows:
                    raise HTTPException(status_code=404, detail="Nenhuma data encontrada")

                datas = [
                    {"data": r[0], "total": r[1]}
                    for r in rows
                ]

                return {
                    "total_dias": len(datas),
                    "datas": datas
                }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")


@app.get("/api/cotacoes/{codigo_ativo}")
def buscar_historico_ativo(
    codigo_ativo: str,
    limite: int = Query(10, ge=1, le=100, description="Quantidade de registros (máx: 100)")
):
    """
    Retorna histórico de cotações de um ativo específico.
    
    - **codigo_ativo**: Código do ativo na B3 (ex: PETR4, VALE3, ITUB4, BBAS3)
    - **limite**: Quantidade de registros mais recentes (padrão: 10, máximo: 100)
    
    Exemplo: /api/cotacoes/PETR4?limite=20
    """
    try:
        query = """
            SELECT ativo, data_pregao, abertura, fechamento, maximo, minimo, volume
            FROM cotacoes
            WHERE ativo = %s
            ORDER BY data_pregao DESC
            LIMIT %s
        """
        
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (codigo_ativo.upper(), limite))
                rows = cur.fetchall()
                
                if not rows:
                    raise HTTPException(
                        status_code=404,
                        detail=f"Nenhuma cotação encontrada para {codigo_ativo.upper()}"
                    )
                
                cotacoes = [
                    {
                        "ativo": r[0],
                        "data_pregao": r[1],
                        "abertura": float(r[2]),
                        "fechamento": float(r[3]),
                        "maximo": float(r[4]),
                        "minimo": float(r[5]),
                        "volume": r[6]
                    }
                    for r in rows
                ]
                
                return {
                    "ativo": codigo_ativo.upper(),
                    "total": len(cotacoes),
                    "dados": cotacoes
                }
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")


@app.get("/api/cotacoes/{codigo_ativo}/latest")
def cotacao_mais_recente(codigo_ativo: str):
    """
    Retorna a cotação mais recente de um ativo específico.
    
    - **codigo_ativo**: Código do ativo na B3 (ex: PETR4, VALE3, ITUB4, BBAS3)
    
    Exemplo: /api/cotacoes/PETR4/latest
    """
    try:
        query = """
            SELECT ativo, data_pregao, abertura, fechamento, maximo, minimo, volume
            FROM cotacoes
            WHERE ativo = %s
            ORDER BY data_pregao DESC
            LIMIT 1
        """
        
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (codigo_ativo.upper(),))
                row = cur.fetchone()
                
                if not row:
                    raise HTTPException(
                        status_code=404,
                        detail=f"Ativo {codigo_ativo.upper()} não encontrado"
                    )
                
                return {
                    "ativo": row[0],
                    "data_pregao": row[1],
                    "abertura": float(row[2]),
                    "fechamento": float(row[3]),
                    "maximo": float(row[4]),
                    "minimo": float(row[5]),
                    "volume": row[6]
                }
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")


@app.get("/api/ativos")
def listar_ativos():
    """Lista todos os ativos disponíveis no banco"""
    try:
        query = "SELECT DISTINCT ativo FROM cotacoes ORDER BY ativo"
        
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                ativos = [row[0] for row in cur.fetchall()]
                
                return {
                    "total": len(ativos),
                    "ativos": ativos
                }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")


@app.get("/api/cotacoes")
def listar_cotacoes_sem_parametros():
    """
    Retorna TODAS as cotações da base (sem parâmetros).
    Atenção: pode ser pesado conforme a base cresce.
    """
    try:
        query = """
            SELECT ativo, data_pregao, abertura, fechamento, maximo, minimo, volume
            FROM cotacoes
            ORDER BY data_pregao DESC, ativo
        """

        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                rows = cur.fetchall()

                if not rows:
                    raise HTTPException(status_code=404, detail="Nenhuma cotação encontrada")

                cotacoes = [
                    {
                        "ativo": r[0],
                        "data_pregao": r[1],
                        "abertura": float(r[2]),
                        "fechamento": float(r[3]),
                        "maximo": float(r[4]),
                        "minimo": float(r[5]),
                        "volume": r[6]
                    }
                    for r in rows
                ]

                return {
                    "total": len(cotacoes),
                    "dados": cotacoes
                }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")


@app.get("/api/cotacoes/data/{data}")
def listar_cotacoes_por_data(data: date):
    """
    Retorna cotações de uma data específica (sem query params).
    """
    try:
        query = """
            SELECT ativo, data_pregao, abertura, fechamento, maximo, minimo, volume
            FROM cotacoes
            WHERE data_pregao = %s
            ORDER BY ativo
        """

        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (data,))
                rows = cur.fetchall()

                if not rows:
                    raise HTTPException(status_code=404, detail=f"Nenhuma cotação encontrada para a data {data}")

                cotacoes = [
                    {
                        "ativo": r[0],
                        "data_pregao": r[1],
                        "abertura": float(r[2]),
                        "fechamento": float(r[3]),
                        "maximo": float(r[4]),
                        "minimo": float(r[5]),
                        "volume": r[6]
                    }
                    for r in rows
                ]

                return {
                    "total": len(cotacoes),
                    "data": str(data),
                    "dados": cotacoes
                }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")


@app.get("/api/ativos/intervalo")
def listar_ativos_por_intervalo(
    inicio: date = Query(..., description="Data inicial (YYYY-MM-DD)"),
    fim: date = Query(..., description="Data final (YYYY-MM-DD)")
):
    """
    Lista os ativos que possuem cotações no intervalo de datas informado (inclusive),
    com a quantidade de registros por ativo. Ordenado por quantidade decrescente.
    """
    try:
        if fim < inicio:
            raise HTTPException(status_code=400, detail="A data final deve ser maior ou igual à inicial")

        query = """
            SELECT ativo, COUNT(*) AS total
            FROM cotacoes
            WHERE data_pregao BETWEEN %s AND %s
            GROUP BY ativo
            ORDER BY total DESC, ativo ASC
        """

        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (inicio, fim))
                rows = cur.fetchall()

                if not rows:
                    raise HTTPException(status_code=404, detail="Nenhum ativo encontrado no intervalo informado")

                ativos = [
                    {"ativo": r[0], "total": r[1]} for r in rows
                ]

                return {
                    "inicio": str(inicio),
                    "fim": str(fim),
                    "total_ativos": len(ativos),
                    "total_registros": int(sum(r[1] for r in rows)),
                    "ativos": ativos
                }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")
