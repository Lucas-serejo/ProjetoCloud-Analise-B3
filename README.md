# ProjetoCloud-Analise-B3

Pipeline Cloud para Análise de Cotações da B3 com Azure - **Versão Local com Docker e PostgreSQL**

## 🚀 Como executar o pipeline completo

### Pré-requisitos
- Docker Desktop instalado e rodando
- Python 3.12+ (para desenvolvimento local)
- Poetry (para gerenciamento de dependências)

### 1. Executar o pipeline completo com Docker

Na raiz do projeto:

```powershell
# Subir todos os serviços (Azurite + PostgreSQL + Pipeline)
docker compose up --build

# Ou subir apenas os serviços de infraestrutura primeiro (recomendado)
docker compose up -d azurite postgres

# Depois executar o pipeline
docker compose up extractor transformer
```

### 2. Testar os serviços individualmente

```powershell
# Apenas o emulador do Azure Storage (Azurite)
docker compose up -d azurite

# Apenas o banco de dados PostgreSQL
docker compose up -d postgres

# Apenas a extração e upload para blob
docker compose up extractor

# Apenas a transformação e carga no banco
docker compose up transformer
```

### 3. Verificar o funcionamento

Execute o script de teste:

```powershell
# Via Docker (após os serviços estarem rodando)
docker compose exec extractor python test_pipeline.py

# Ou localmente (após instalar dependências)
cd backend
poetry install
poetry run python test_pipeline.py
```

## 🏗️ Arquitetura do Pipeline

```
📥 B3 (Download) → 📦 Docker Container (Extrair) 
                     ↓
📁 Azure Blob Storage (Azurite) → 🔄 Docker Container (Transformar)
                                     ↓
🗄️ PostgreSQL → 📊 Dados estruturados
```

### Componentes:

1. **Extractor Service** (`extract_and_upload.py`)
   - Baixa arquivos ZIP da B3
   - Extrai arquivos XML
   - Faz upload para o Blob Storage (Azurite)

2. **Transformer Service** (`transform_and_save.py`)
   - Baixa XMLs do Blob Storage
   - Extrai dados de cotações usando XML parsing
   - Salva dados estruturados no PostgreSQL

3. **Azurite** (Emulador Azure Storage)
   - Porta 10000: Blob Storage
   - Porta 10001: Queue Storage
   - Porta 10002: Table Storage

4. **PostgreSQL**
   - Porta 5432: Conexão padrão do PostgreSQL

## 📂 Estrutura dos dados

### No Blob Storage:
```
dados-pregao/
├── raw/
│   └── pregao_YYMMDD.zip
└── xml/
    └── YYMMDD/
        ├── arquivo1.xml
        └── arquivo2.xml
```

### No PostgreSQL (Tabela `cotacoes`):
```sql
CREATE TABLE cotacoes (
    id SERIAL PRIMARY KEY,
    ativo VARCHAR(20) NOT NULL,
    data_pregao DATE NOT NULL,
    abertura NUMERIC(18, 5),
    fechamento NUMERIC(18, 5),
    maximo NUMERIC(18, 5),
    minimo NUMERIC(18, 5),
    volume BIGINT,
    timestamp_processamento TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (ativo, data_pregao)
);
```

## 🔧 Desenvolvimento Local

### Instalar dependências com Poetry:

```powershell
cd backend
poetry install
```

### Executar scripts individualmente:

```powershell
# Extrair e fazer upload
poetry run python extract_and_upload.py

# Transformar e salvar no banco
poetry run python transform_and_save.py

# Teste completo
poetry run python test_pipeline.py
```

## 🐳 Comandos Docker úteis

```powershell
# Ver logs dos serviços
docker compose logs -f extractor
docker compose logs -f transformer
docker compose logs -f postgres

# Acessar o banco de dados PostgreSQL
docker compose exec postgres psql -U admin -d b3_db

# Parar e remover todos os containers
docker compose down -v
```

## 🌐 Acessar interfaces web

- **Azurite**: Use Azure Storage Explorer ou VS Code extension
- **PostgreSQL**: Acesse via cliente PostgreSQL na porta 5432

## 🎯 Próximos passos

- [ ] Adicionar Azure Data Factory local
- [ ] Implementar Azure Functions
- [ ] Adicionar Power BI dashboards
- [ ] Deploy na Azure (produção)
