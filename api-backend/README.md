# B3 Cotações API

API REST simples para consulta de cotações da B3.

## 🚀 Uso Local

### 1. Instalar dependências

```bash
pip install -r requirements.txt
```

### 2. Configurar variáveis de ambiente

```bash
cp .env.example .env
# Edite o .env com suas configurações
```

### 3. Rodar migrações do banco

```bash
# Opção 1: Usar script de inicialização (aplica migrações + inicia API)
python start.py

# Opção 2: Manual
alembic upgrade head
```

### 4. Rodar a API

```bash
# Se não usou start.py acima
uvicorn app.main:app --reload
```

Acesse: http://localhost:8000/docs

## 📚 Endpoints

- `GET /` - Página inicial
- `GET /health` - Status da API
- `GET /api/cotacoes/{ticker}` - Histórico de cotações
- `GET /api/cotacoes/{ticker}/latest` - Última cotação
- `GET /api/ativos` - Lista de ativos disponíveis

## 🗄️ Migrações de Banco

### Criar nova migração
```bash
alembic revision -m "descrição da mudança"
```

### Aplicar migrações
```bash
alembic upgrade head
```

### Reverter última migração
```bash
alembic downgrade -1
```

## 🌐 Deploy Azure Web App

### Opção 1: Via Azure CLI

```bash
# Login
az login

# Criar Web App (Python 3.11)
az webapp up --name app-b3-api --resource-group rg-b3-analytics --runtime "PYTHON:3.11"

# Configurar variáveis de ambiente
az webapp config appsettings set --name app-b3-api --resource-group rg-b3-analytics --settings \
  POSTGRES_HOST=seu-host.postgres.database.azure.com \
  POSTGRES_DB=b3_data \
  POSTGRES_USER=seu_usuario \
  POSTGRES_PASSWORD=sua_senha
```

### Opção 2: Via VS Code

1. Instale a extensão "Azure App Service"
2. Clique com botão direito na pasta `api-backend`
3. Selecione "Deploy to Web App..."
4. Configure as variáveis no portal Azure

## 🔧 Variáveis de Ambiente Necessárias

```
POSTGRES_HOST=seu-host.postgres.database.azure.com
POSTGRES_PORT=5432
POSTGRES_DB=b3_data
POSTGRES_USER=seu_usuario
POSTGRES_PASSWORD=sua_senha
```

## 📝 Exemplos

```bash
# Últimas 10 cotações da PETR4
curl http://localhost:8000/api/cotacoes/PETR4

# Última cotação da VALE3
curl http://localhost:8000/api/cotacoes/VALE3/latest

# Buscar com filtro de data
curl "http://localhost:8000/api/cotacoes/ITUB4?data_inicio=2024-01-01&limit=30"

# Listar todos os ativos
curl http://localhost:8000/api/ativos
```
