# Use uma imagem base oficial do Python
FROM python:3.12-slim

WORKDIR /app

# Instala o Poetry
RUN pip install --no-cache-dir poetry

# Copia arquivos de dependências para aproveitar cache do Docker
COPY pyproject.toml poetry.lock ./

# Instala dependências do projeto (não instala o pacote do projeto via --no-root)
RUN poetry config virtualenvs.create false \
    && poetry install --no-root --no-interaction --no-ansi

# Copia o restante do código
COPY . .

# Comando padrão (substituído no docker-compose)
CMD ["python", "--version"]