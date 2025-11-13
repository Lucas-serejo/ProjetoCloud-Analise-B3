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


@app.get("/")
def root():
    """Página inicial"""
    return {
        "message": "B3 Cotações API",
        "docs": "/docs",
        "version": "1.0.0"
    }


@app.get("/health")
def health_check():
    """Verifica status da API e banco"""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        return {"status": "unhealthy", "database": str(e)}


@app.get("/api/cotacoes/{ticker}")
def buscar_cotacoes(
    ticker: str,
    limit: int = Query(10, ge=1, le=365, description="Quantidade de registros"),
    data_inicio: Optional[date] = Query(None, description="Data inicial (YYYY-MM-DD)"),
    data_fim: Optional[date] = Query(None, description="Data final (YYYY-MM-DD)")
):
    """
    Busca histórico de cotações de um ativo
    
    - **ticker**: Código do ativo (ex: PETR4, VALE3)
    - **limit**: Máximo de registros (padrão: 10)
    - **data_inicio**: Filtro data inicial (opcional)
    - **data_fim**: Filtro data final (opcional)
    """
    try:
        # Monta query com filtros opcionais
        query = """
            SELECT ativo, data_pregao, abertura, fechamento, maximo, minimo, volume
            FROM cotacoes
            WHERE ativo = %s
        """
        params = [ticker.upper()]
        
        if data_inicio:
            query += " AND data_pregao >= %s"
            params.append(data_inicio)
        
        if data_fim:
            query += " AND data_pregao <= %s"
            params.append(data_fim)
        
        query += " ORDER BY data_pregao DESC LIMIT %s"
        params.append(limit)
        
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                rows = cur.fetchall()
                
                if not rows:
                    raise HTTPException(
                        status_code=404,
                        detail=f"Nenhuma cotação encontrada para {ticker.upper()}"
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
                    "ticker": ticker.upper(),
                    "total": len(cotacoes),
                    "dados": cotacoes
                }
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")


@app.get("/api/cotacoes/{ticker}/latest")
def cotacao_mais_recente(ticker: str):
    """Retorna a cotação mais recente de um ativo"""
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
                cur.execute(query, (ticker.upper(),))
                row = cur.fetchone()
                
                if not row:
                    raise HTTPException(
                        status_code=404,
                        detail=f"Ativo {ticker.upper()} não encontrado"
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
