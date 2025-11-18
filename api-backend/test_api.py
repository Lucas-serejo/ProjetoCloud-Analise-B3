"""
Script para testar a API localmente
"""
import requests
import json

BASE_URL = "http://127.0.0.1:8000"

print("=" * 80)
print(" " * 25 + "TESTE DA API BACKEND")
print("=" * 80)

# Teste 1: Página inicial
print("\n🔹 Teste 1: GET / (Página inicial)")
print("-" * 80)
try:
    response = requests.get(f"{BASE_URL}/")
    print(f"Status: {response.status_code}")
    print(f"Resposta: {json.dumps(response.json(), indent=2)}")
    print("✅ SUCESSO!")
except Exception as e:
    print(f"❌ ERRO: {e}")

# Teste 2: Health check
print("\n🔹 Teste 2: GET /health (Health Check)")
print("-" * 80)
try:
    response = requests.get(f"{BASE_URL}/health")
    print(f"Status: {response.status_code}")
    print(f"Resposta: {json.dumps(response.json(), indent=2)}")
    if response.json().get("status") == "healthy":
        print("✅ SUCESSO - Banco conectado!")
    else:
        print("⚠️ AVISO - Banco desconectado!")
except Exception as e:
    print(f"❌ ERRO: {e}")

# Teste 3: Listar ativos
print("\n🔹 Teste 3: GET /api/ativos (Lista de ativos)")
print("-" * 80)
try:
    response = requests.get(f"{BASE_URL}/api/ativos")
    print(f"Status: {response.status_code}")
    data = response.json()
    print(f"Total de ativos: {data.get('total', 0)}")
    
    if data.get('total', 0) > 0:
        print(f"Primeiros 10 ativos: {data['ativos'][:10]}")
        print("✅ SUCESSO!")
        
        # Guardar um ativo para próximo teste
        primeiro_ativo = data['ativos'][0]
    else:
        print("⚠️ AVISO - Nenhum ativo encontrado no banco")
        primeiro_ativo = None
except Exception as e:
    print(f"❌ ERRO: {e}")
    primeiro_ativo = None

# Teste 4: Buscar cotações de um ativo específico
if primeiro_ativo:
    print(f"\n🔹 Teste 4: GET /api/cotacoes/{primeiro_ativo} (Histórico de cotações)")
    print("-" * 80)
    try:
        response = requests.get(f"{BASE_URL}/api/cotacoes/{primeiro_ativo}")
        print(f"Status: {response.status_code}")
        data = response.json()
        print(f"Ticker: {data.get('ticker')}")
        print(f"Total de registros: {data.get('total', 0)}")
        
        if data.get('total', 0) > 0:
            print("\n📋 Primeiras 3 cotações:")
            for i, cotacao in enumerate(data['dados'][:3], 1):
                print(f"   {i}. Data: {cotacao['data_pregao']} | "
                      f"Fechamento: R$ {cotacao['fechamento']:.2f} | "
                      f"Volume: {cotacao['volume']:,}")
            print("✅ SUCESSO!")
        else:
            print("⚠️ AVISO - Nenhuma cotação encontrada")
    except Exception as e:
        print(f"❌ ERRO: {e}")

    # Teste 5: Última cotação
    print(f"\n🔹 Teste 5: GET /api/cotacoes/{primeiro_ativo}/latest (Última cotação)")
    print("-" * 80)
    try:
        response = requests.get(f"{BASE_URL}/api/cotacoes/{primeiro_ativo}/latest")
        print(f"Status: {response.status_code}")
        data = response.json()
        print(f"Ativo: {data.get('ativo')}")
        print(f"Data: {data.get('data_pregao')}")
        print(f"Abertura: R$ {data.get('abertura', 0):.2f}")
        print(f"Fechamento: R$ {data.get('fechamento', 0):.2f}")
        print(f"Máximo: R$ {data.get('maximo', 0):.2f}")
        print(f"Mínimo: R$ {data.get('minimo', 0):.2f}")
        print(f"Volume: {data.get('volume', 0):,}")
        print("✅ SUCESSO!")
    except Exception as e:
        print(f"❌ ERRO: {e}")

# Teste 6: Buscar ativo inexistente
print(f"\n🔹 Teste 6: GET /api/cotacoes/XXXXX (Ativo inexistente)")
print("-" * 80)
try:
    response = requests.get(f"{BASE_URL}/api/cotacoes/XXXXX")
    print(f"Status: {response.status_code}")
    if response.status_code == 404:
        print(f"Mensagem: {response.json().get('detail')}")
        print("✅ SUCESSO - Tratamento de erro 404 funcionando!")
    else:
        print("⚠️ Deveria retornar 404")
except Exception as e:
    print(f"❌ ERRO: {e}")

print("\n" + "=" * 80)
print("✅ TESTES CONCLUÍDOS!")
print("=" * 80)
