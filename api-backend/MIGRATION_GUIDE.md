# 🔄 Migração da Estrutura - db-migrations → api-backend

## ✅ O que foi feito

A pasta `db-migrations` foi **integrada** dentro de `api-backend/` para melhor organização e facilitar o deploy no Azure.

### Estrutura Anterior ❌
```
projeto/
├── api-backend/
│   └── main.py
└── db-migrations/          # Separado (confuso!)
    ├── alembic.ini
    └── alembic/
```

### Estrutura Nova ✅
```
projeto/
└── api-backend/
    ├── app/
    │   ├── main.py
    │   ├── database.py
    │   └── models.py
    ├── alembic/            # Integrado!
    │   ├── env.py
    │   └── versions/
    │       └── 1_create_cotacoes_table.py
    ├── alembic.ini
    ├── start.py            # Script de inicialização
    └── requirements.txt
```

## 🎯 Vantagens

1. **Tudo em um lugar**: API + Migrações juntas
2. **Deploy mais simples**: Uma pasta só para subir no Azure
3. **Migrações automáticas**: Script `start.py` roda migrações antes da API
4. **Menos confusão**: Estrutura mais clara

## 🚀 Como usar

### Desenvolvimento Local

```bash
cd api-backend

# Instalar dependências (inclui Alembic)
pip install -r requirements.txt

# Opção 1: Usar script de inicialização (recomendado)
python start.py

# Opção 2: Manual
alembic upgrade head
uvicorn app.main:app --reload
```

### Deploy Azure

O Azure rodará automaticamente:
```bash
alembic upgrade head && uvicorn app.main:app --host 0.0.0.0 --port 8000
```

Ou usar o `start.py`:
```bash
python start.py
```

## 📝 Comandos de Migração

```bash
# Ver status atual
alembic current

# Aplicar todas as migrações
alembic upgrade head

# Reverter última migração
alembic downgrade -1

# Criar nova migração
alembic revision -m "adiciona coluna X"

# Ver histórico
alembic history
```

## ⚠️ Ação Necessária

**Você pode deletar a pasta `db-migrations` antiga** se quiser manter o repositório limpo:

```bash
# Na raiz do projeto
rm -rf db-migrations

# Ou no Windows PowerShell
Remove-Item -Recurse -Force db-migrations
```

## 🔧 Configuração

As migrações usam as **mesmas variáveis de ambiente** da API (arquivo `.env`):

```env
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=b3_data
POSTGRES_USER=user
POSTGRES_PASSWORD=password
```

No Azure, configure essas variáveis nas **Application Settings** do Web App.

---

**Estrutura finalizada e pronta para deploy!** 🎉
