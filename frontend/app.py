import streamlit as st
import requests
import pandas as pd
from datetime import date, timedelta

# Configuração da página
st.set_page_config(
    page_title="B3 Cotações - Análise de Mercado",
    page_icon="📊",
    layout="wide"
)

# URL da API (alterar para produção depois do deploy)
API_URL = "http://localhost:8000"

# Título principal
st.title("📊 B3 Cotações - Análise de Mercado")
st.markdown("---")

# Sidebar para navegação
st.sidebar.title("Menu")
opcao = st.sidebar.radio(
    "Selecione uma opção:",
    ["📅 Cotações do Dia", "🔍 Buscar Ativo", "📈 Ativos Disponíveis"]
)

# ============================================================================
# OPÇÃO 1: Cotações do Dia
# ============================================================================
if opcao == "📅 Cotações do Dia":
    st.header("📅 Cotações do Dia")
    
    # Seletor de data
    data_selecionada = st.date_input(
        "Selecione a data:",
        value=date.today() - timedelta(days=1),  # Ontem por padrão
        max_value=date.today()
    )
    
    if st.button("Buscar Cotações", type="primary"):
        with st.spinner("Buscando cotações..."):
            try:
                # Chamada à API
                response = requests.get(f"{API_URL}/api/cotacoes/data/{data_selecionada}")
                
                if response.status_code == 200:
                    data = response.json()
                    
                    # Métricas
                    col1, col2, col3 = st.columns(3)
                    col1.metric("Total de Ativos", data["total"])
                    col2.metric("Data", data["data"])
                    col3.metric("Status", "✅ Disponível")
                    
                    # Converter para DataFrame
                    df = pd.DataFrame(data["dados"])
                    
                    # Formatar valores
                    df["abertura"] = df["abertura"].apply(lambda x: f"R$ {x:,.2f}")
                    df["fechamento"] = df["fechamento"].apply(lambda x: f"R$ {x:,.2f}")
                    df["maximo"] = df["maximo"].apply(lambda x: f"R$ {x:,.2f}")
                    df["minimo"] = df["minimo"].apply(lambda x: f"R$ {x:,.2f}")
                    df["volume"] = df["volume"].apply(lambda x: f"{x:,}")
                    
                    # Renomear colunas
                    df.columns = ["Ativo", "Data Pregão", "Abertura", "Fechamento", "Máximo", "Mínimo", "Volume"]
                    
                    # Exibir tabela
                    st.dataframe(df, use_container_width=True, height=400)
                    
                    # Botão de download
                    csv = df.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        label="📥 Baixar CSV",
                        data=csv,
                        file_name=f"cotacoes_{data_selecionada}.csv",
                        mime="text/csv"
                    )
                    
                elif response.status_code == 404:
                    st.warning(f"⚠️ Nenhuma cotação encontrada para {data_selecionada}")
                else:
                    st.error(f"❌ Erro na API: {response.status_code}")
                    
            except requests.exceptions.ConnectionError:
                st.error("❌ Não foi possível conectar à API. Certifique-se de que ela está rodando em http://localhost:8000")
            except Exception as e:
                st.error(f"❌ Erro: {str(e)}")

# ============================================================================
# OPÇÃO 2: Buscar Ativo Específico
# ============================================================================
elif opcao == "🔍 Buscar Ativo":
    st.header("🔍 Buscar Ativo Específico")
    
    col1, col2 = st.columns([3, 1])
    
    with col1:
        codigo_ativo = st.text_input(
            "Digite o código do ativo:",
            placeholder="Ex: PETR4, VALE3, ITUB4",
            max_chars=10
        ).upper()
    
    with col2:
        limite = st.number_input(
            "Limite de registros:",
            min_value=1,
            max_value=100,
            value=10
        )
    
    if st.button("Buscar", type="primary"):
        if not codigo_ativo:
            st.warning("⚠️ Digite um código de ativo")
        else:
            with st.spinner(f"Buscando {codigo_ativo}..."):
                try:
                    # Chamada à API
                    response = requests.get(f"{API_URL}/api/cotacoes/{codigo_ativo}?limite={limite}")
                    
                    if response.status_code == 200:
                        data = response.json()
                        
                        # Última cotação em destaque
                        st.subheader(f"📊 {data['ativo']}")
                        ultima = data["dados"][0]
                        
                        col1, col2, col3, col4 = st.columns(4)
                        col1.metric("Fechamento", f"R$ {ultima['fechamento']:,.2f}")
                        col2.metric("Máximo", f"R$ {ultima['maximo']:,.2f}")
                        col3.metric("Mínimo", f"R$ {ultima['minimo']:,.2f}")
                        col4.metric("Volume", f"{ultima['volume']:,}")
                        
                        st.markdown("---")
                        
                        # Histórico
                        st.subheader("📈 Histórico")
                        df = pd.DataFrame(data["dados"])
                        
                        # Formatar valores
                        df["abertura"] = df["abertura"].apply(lambda x: f"R$ {x:,.2f}")
                        df["fechamento"] = df["fechamento"].apply(lambda x: f"R$ {x:,.2f}")
                        df["maximo"] = df["maximo"].apply(lambda x: f"R$ {x:,.2f}")
                        df["minimo"] = df["minimo"].apply(lambda x: f"R$ {x:,.2f}")
                        df["volume"] = df["volume"].apply(lambda x: f"{x:,}")
                        
                        # Renomear colunas
                        df.columns = ["Ativo", "Data", "Abertura", "Fechamento", "Máximo", "Mínimo", "Volume"]
                        
                        # Exibir tabela
                        st.dataframe(df, use_container_width=True)
                        
                        # Botão de download
                        csv = df.to_csv(index=False).encode('utf-8')
                        st.download_button(
                            label="📥 Baixar CSV",
                            data=csv,
                            file_name=f"historico_{codigo_ativo}.csv",
                            mime="text/csv"
                        )
                        
                    elif response.status_code == 404:
                        st.warning(f"⚠️ Ativo {codigo_ativo} não encontrado")
                    else:
                        st.error(f"❌ Erro na API: {response.status_code}")
                        
                except requests.exceptions.ConnectionError:
                    st.error("❌ Não foi possível conectar à API. Certifique-se de que ela está rodando em http://localhost:8000")
                except Exception as e:
                    st.error(f"❌ Erro: {str(e)}")

# ============================================================================
# OPÇÃO 3: Lista de Ativos Disponíveis
# ============================================================================
elif opcao == "📈 Ativos Disponíveis":
    st.header("📈 Ativos Disponíveis")
    
    if st.button("Carregar Ativos", type="primary"):
        with st.spinner("Carregando ativos..."):
            try:
                # Chamada à API
                response = requests.get(f"{API_URL}/api/ativos")
                
                if response.status_code == 200:
                    data = response.json()
                    
                    # Métrica
                    st.metric("Total de Ativos", data["total"])
                    
                    # Criar DataFrame
                    ativos = data["ativos"]
                    
                    # Dividir em colunas para melhor visualização
                    num_colunas = 5
                    colunas = st.columns(num_colunas)
                    
                    for i, ativo in enumerate(ativos):
                        col_idx = i % num_colunas
                        with colunas[col_idx]:
                            st.text(ativo)
                    
                    st.markdown("---")
                    
                    # Botão de download
                    df = pd.DataFrame({"Ativo": ativos})
                    csv = df.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        label="📥 Baixar Lista Completa",
                        data=csv,
                        file_name="ativos_b3.csv",
                        mime="text/csv"
                    )
                    
                else:
                    st.error(f"❌ Erro na API: {response.status_code}")
                    
            except requests.exceptions.ConnectionError:
                st.error("❌ Não foi possível conectar à API. Certifique-se de que ela está rodando em http://localhost:8000")
            except Exception as e:
                st.error(f"❌ Erro: {str(e)}")

# Footer
st.markdown("---")
st.markdown(
    """
    <div style='text-align: center; color: gray;'>
        <p>Projeto Cloud - Análise B3 | Desenvolvido com Streamlit 🐍</p>
    </div>
    """,
    unsafe_allow_html=True
)
