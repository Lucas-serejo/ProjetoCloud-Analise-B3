# ProjetoCloud-Analise-B3

Pipeline ETL para Análise de Cotações da B3 - **Versão Local com Docker**

## 🚀 Visão Geral

Pipeline automatizado que extrai cotações diárias da B3, processa arquivos XML e armazena dados estruturados em PostgreSQL, usando Azure Blob Storage (Azurite) como camada intermediária.

### Arquitetura ETL

```
🌐 B3 Website → 📦 Extract → 📁 Azure Blob → 🔄 Transform → 🗄️ PostgreSQL
```

## 🏗️ Componentes

- **Extract**: Baixa arquivos SPRE.zip da B3 e extrai XMLs
- **Transform**: Processa XMLs usando XPath e aplica filtros de mercado à vista  
- **Load**: Insere/atualiza cotações no PostgreSQL com upsert
- **Storage**: Azurite (emulador Azure Blob Storage)
- **Database**: PostgreSQL com migrações Alembic

## 🚀 Como Executar

### Pré-requisitos
- Docker Desktop instalado e rodando
- Git

### 1. Clone e Configure

```powershell
git clone <repository-url>
cd ProjetoCloud-Analise-B3

# Subir todos os serviços
docker compose up -d
```

### 2. Executar Pipeline Completo

```powershell
# Executar o pipeline ETL
docker compose exec etl python run_pipeline.py

# Ou executar com logs detalhados
docker compose exec etl python backend/run_pipeline.py
```

### 3. Verificar Resultados

```powershell
# Testar conexões e ver dados
docker compose exec etl python backend/test_pipeline.py

# Ver logs do pipeline
docker compose logs -f etl
```

## 🔧 Comandos Úteis

### Banco de Dados

```powershell
# Executar migrações
docker compose exec etl bash -c "cd backend && alembic upgrade head"

# Acessar PostgreSQL
docker compose exec postgres psql -U postgres -d cotacoes_b3

# Acessar pgAdmin: http://localhost:8080
# Email: admin@admin.com | Senha: admin
```

## 🎯 Filtros Aplicados

O pipeline aplica filtros para capturar apenas **ações do mercado à vista**:

1. **Mercado**: `MktIdrCd ∈ {BVMF, XBSP, BOVESPA}`
2. **Formato Ticker**: Regex `[A-Z]{3,5}\d{1,2}` (ex: PETR4, VALE3)
3. **Units**: Regex `[A-Z]{4,5}11` (ex: ITUB11)
4. **Dados válidos**: Deve ter preço de fechamento

## 🔍 Monitoramento

### Logs do Pipeline
```powershell
# Ver execução em tempo real
docker compose logs -f etl

# Verificar status dos serviços
docker compose ps







