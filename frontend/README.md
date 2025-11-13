# Frontend Streamlit - B3 Cotações

Interface web para consulta de cotações da B3.

## 🚀 Como rodar localmente

### 1. Instalar dependências
```bash
pip install -r requirements.txt
```

### 2. Certifique-se de que a API está rodando
```bash
# Em outro terminal, na pasta api-backend
python start.py
```

### 3. Rodar o Streamlit
```bash
streamlit run app.py
```

O app abrirá automaticamente em: http://localhost:8501

## 📋 Funcionalidades

- **📅 Cotações do Dia**: Consulta todas as cotações de uma data específica
- **🔍 Buscar Ativo**: Busca histórico de um ativo específico (ex: PETR4)
- **📈 Ativos Disponíveis**: Lista todos os ativos disponíveis no banco

## 🌐 Deploy Azure

### Opção 1: Azure Container Instances
```bash
# Criar Dockerfile
# Build e push para Azure Container Registry
# Deploy no Azure Container Instances
```

### Opção 2: Azure Web App (Container)
```bash
az webapp create --resource-group rg-b3-analytics \
  --plan app-plan --name app-b3-frontend \
  --deployment-container-image-name <sua-imagem>
```

## 🔧 Configuração

Para produção, edite `app.py` e altere:
```python
API_URL = "https://app-b3-api.azurewebsites.net"  # URL da sua API no Azure
```
