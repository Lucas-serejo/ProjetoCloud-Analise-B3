# ProjetoCloud-Analise-B3

# Como rodar o projeto (backend)

## 1. Instale o Poetry (caso não tenha)

No PowerShell:
```powershell
(Invoke-WebRequest -Uri https://install.python-poetry.org -UseBasicParsing).Content | python -
```
Ou, se preferir, via pip:
```powershell
pip install poetry
```
> Após instalar, feche e reabra o terminal para garantir que o comando `poetry` funcione.

## 2. Configure o Poetry para criar o ambiente virtual local

> Antes de instalar as dependências, execute:
>
> ```powershell
> poetry config virtualenvs.in-project true
> ```
>
> Isso garante que o ambiente virtual será criado na pasta do projeto (`.venv`).

## 3. Entre na pasta do backend

```powershell
cd backend
```

## 4. Instale as dependências e crie o ambiente virtual

```powershell
poetry install
```

## 5. Ative o ambiente virtual

No PowerShell:
```powershell
.venv\Scripts\Activate.ps1
```

Ou rode scripts diretamente (sem ativar):
```powershell
poetry run python extract.py
```

---

Esses passos garantem que todas as dependências serão instaladas e o ambiente estará pronto para uso, mesmo para quem nunca usou Poetry antes.
