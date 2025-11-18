# ProjetoCloud-Analise-B3

Pipeline ETL para Análise de Cotações da B3 - **Azure Cloud**

## 🚀 Visão Geral

Pipeline automatizado na Azure que extrai cotações diárias da B3, processa arquivos XML e armazena dados estruturados em PostgreSQL, com API FastAPI e frontend Streamlit.

### Arquitetura

```
🌐 B3 Website → ⚡ Azure Functions (Timer) → 📁 Azure Blob Storage 
                     ↓
              ⚡ Azure Functions (Blob Trigger) → 🔄 Transform → 🗄️ PostgreSQL Azure
                     ↓
              🌐 FastAPI (App Service) ← 📊 Streamlit Frontend
```

## 🏗️ Componentes

### Backend
- **Azure Functions ETL** (`functions-etl/`)
  - Timer Trigger: Extrai arquivos SPRE.zip da B3 diariamente (22:00 UTC, dias úteis)
  - Blob Trigger: Processa XMLs automaticamente ao detectar novos arquivos
  - Transform: Parser XML com XPath e filtros de mercado à vista
  - Load: Insere/atualiza cotações no PostgreSQL (upsert)

- **API FastAPI** (`api-backend/`)
  - Endpoints REST para consultar cotações, ativos e datas disponíveis
  - Integração com PostgreSQL Azure
  - Migrações com Alembic

- **Frontend Streamlit** (`frontend/`)
  - Visualização de cotações por data
  - Busca de ativos
  - Consultas por intervalo de datas
  - Exportação CSV

### Infraestrutura Azure
- **Azure Blob Storage**: Armazenamento de XMLs processados
- **Azure PostgreSQL Flexible Server**: Banco de dados relacional
- **Azure App Service**: Hospedagem da API e Frontend

## 🎯 Filtros Aplicados

O pipeline captura apenas **ações do mercado à vista**:

1. **Mercado**: `MktIdrCd ∈ {BVMF, XBSP, BOVESPA}`
2. **Formato Ticker**: Regex `[A-Z]{3,5}\d{1,2}` (ex: PETR4, VALE3, GOL3)
3. **Units**: Regex `[A-Z]{4,5}11` (ex: ITUB11, SANB11)
4. **Dados válidos**: Deve ter preço de fechamento

## 🚀 Deploy

### Azure Functions (ETL)
```bash
cd functions-etl
func azure functionapp publish <FUNCTION_APP_NAME>
```

### API Backend
```bash
cd api-backend
# Deploy via Azure App Service ou Container
az webapp up --name <API_APP_NAME> --runtime PYTHON:3.10
```

### Frontend
```bash
cd frontend
# Atualizar API_URL no código
# Deploy via Azure App Service
az webapp up --name <FRONTEND_APP_NAME> --runtime PYTHON:3.10
```

## 🔧 Desenvolvimento Local

### Executar API
```bash
cd api-backend
python start.py
# API disponível em http://localhost:8000
```

### Executar Frontend
```bash
cd frontend
streamlit run app.py
# UI disponível em http://localhost:8501
```

### Testar Azure Functions Localmente
```bash
cd functions-etl
func start
```

## 📊 Endpoints da API

- `GET /api/cotacoes` - Lista todas cotações
- `GET /api/cotacoes/data/{data}` - Cotações de uma data específica
- `GET /api/cotacoes/datas` - Lista datas disponíveis
- `GET /api/cotacoes/{codigo_ativo}` - Histórico de um ativo
- `GET /api/cotacoes/{codigo_ativo}/latest` - Última cotação de um ativo
- `GET /api/ativos` - Lista todos ativos disponíveis
- `GET /api/ativos/intervalo` - Ativos por intervalo de datas

## 🔐 Variáveis de Ambiente

### Azure Functions
```
AzureWebJobsStorage=<connection_string>
POSTGRES_HOST=<host>
POSTGRES_PORT=5432
POSTGRES_DB=<database>
POSTGRES_USER=<user>
POSTGRES_PASSWORD=<password>
```

### API Backend
```
POSTGRES_HOST=<host>
POSTGRES_PORT=5432
POSTGRES_DB=<database>
POSTGRES_USER=<user>
POSTGRES_PASSWORD=<password>
```

### Frontend
```
API_URL=<api_url>
```







