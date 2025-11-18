import os
import streamlit as st
import requests
import pandas as pd
from datetime import date, timedelta


# Funções cacheadas para reduzir latência em reruns do Streamlit
@st.cache_data(ttl=600, show_spinner=False)
def fetch_datas(api_url: str):
    r = requests.get(f"{api_url}/api/cotacoes/datas", timeout=12)
    r.raise_for_status()
    return r.json()


@st.cache_data(ttl=180, show_spinner=False)
def fetch_intervalo(api_url: str, inicio: str, fim: str):
    url = f"{api_url}/api/ativos/intervalo?inicio={inicio}&fim={fim}"
    r = requests.get(url, timeout=20)
    if r.status_code == 404:
        return {"status": 404}
    r.raise_for_status()
    return r.json()

# Lista de ativos cacheada
@st.cache_data(ttl=600, show_spinner=False)
def fetch_ativos(api_url: str):
    r = requests.get(f"{api_url}/api/ativos", timeout=12)
    r.raise_for_status()
    return r.json().get("ativos", [])

# Série de fechamento por ativo no intervalo (cacheada)
@st.cache_data(ttl=180, show_spinner=False)
def fetch_fechamento(api_url: str, inicio: str, fim: str, ativo: str):
    url = f"{api_url}/api/ativos/intervalo?inicio={inicio}&fim={fim}&ativo={ativo}"
    r = requests.get(url, timeout=20)
    if r.status_code == 404:
        return {"status": 404}
    r.raise_for_status()
    return r.json()

import plotly.graph_objects as go

# Configuração da página
st.set_page_config(
    page_title="B3 Cotações - Análise de Mercado",
    page_icon="📊",
    layout="wide"
)

# URL da API (env ou fallback local)
API_URL = os.getenv("API_URL", "http://localhost:8000")

# Título
st.title("📊 B3 Cotações - Análise de Mercado")
st.markdown("---")

# Menu lateral
st.sidebar.title("Menu")
opcao = st.sidebar.radio(
    "Selecione uma opção:",
    [
        "📅 Cotações do Dia",
        "🔍 Buscar Ativo",
        "📈 Ativos Disponíveis",
        "🗓️ Ativos por Intervalo"
    ]
)

# Cotações do dia
if opcao == "📅 Cotações do Dia":
    st.header("📅 Cotações do Dia")

    # Carrega datas disponíveis (cacheado)
    datas_disponiveis = []
    try:
        payload = fetch_datas(API_URL)
        datas_disponiveis = [str(item["data"]) for item in payload.get("datas", [])]
    except requests.exceptions.HTTPError as e:
        if getattr(e.response, "status_code", None) == 404:
            st.warning("Nenhuma data disponível encontrada na API.")
        else:
            st.error(f"Erro carregando datas: {getattr(e.response, 'status_code', 'desconhecido')}")
    except requests.exceptions.ConnectionError:
        st.error("❌ Não foi possível conectar à API para listar as datas.")
    except Exception as e:
        st.error(f"❌ Erro ao carregar datas: {e}")

    # Seletor de data baseado no banco
    if datas_disponiveis:
        default_idx = len(datas_disponiveis) - 1  # última data por padrão
        data_selecionada = st.selectbox(
            "Selecione a data disponível:",
            options=datas_disponiveis,
            index=default_idx
        )
    else:
        # fallback se não conseguiu carregar datas
        data_selecionada = st.date_input(
            "Selecione a data:",
            value=date.today() - timedelta(days=1),
            max_value=date.today()
        ).isoformat()

    if st.button("Buscar Cotações", type="primary"):
        with st.spinner("Buscando cotações..."):
            try:
                # Chama API
                response = requests.get(f"{API_URL}/api/cotacoes/data/{data_selecionada}")
                
                if response.status_code == 200:
                    data = response.json()
                    
                    # Métricas
                    col1, col2, col3 = st.columns(3)
                    col1.metric("Total de Ativos", data["total"])
                    col2.metric("Data", data["data"])
                    col3.metric("Status", "✅ Disponível")
                    
                    # DataFrame
                    df = pd.DataFrame(data["dados"])
                    
                    # Formatação
                    df["abertura"] = df["abertura"].apply(lambda x: f"R$ {x:,.2f}")
                    df["fechamento"] = df["fechamento"].apply(lambda x: f"R$ {x:,.2f}")
                    df["maximo"] = df["maximo"].apply(lambda x: f"R$ {x:,.2f}")
                    df["minimo"] = df["minimo"].apply(lambda x: f"R$ {x:,.2f}")
                    df["volume"] = df["volume"].apply(lambda x: f"{x:,}")
                    
                    # Renomeia colunas
                    df.columns = ["Ativo", "Data Pregão", "Abertura", "Fechamento", "Máximo", "Mínimo", "Volume"]
                    
                    # Tabela
                    st.dataframe(df, use_container_width=True, height=400)
                    
                    # Download CSV
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

# Buscar ativo específico
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
                    # Chama API
                    response = requests.get(f"{API_URL}/api/cotacoes/{codigo_ativo}?limite={limite}")
                    
                    if response.status_code == 200:
                        data = response.json()
                        
                        # Última cotação
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
                        
                        # Formatação
                        df["abertura"] = df["abertura"].apply(lambda x: f"R$ {x:,.2f}")
                        df["fechamento"] = df["fechamento"].apply(lambda x: f"R$ {x:,.2f}")
                        df["maximo"] = df["maximo"].apply(lambda x: f"R$ {x:,.2f}")
                        df["minimo"] = df["minimo"].apply(lambda x: f"R$ {x:,.2f}")
                        df["volume"] = df["volume"].apply(lambda x: f"{x:,}")
                        
                        # Renomeia colunas
                        df.columns = ["Ativo", "Data", "Abertura", "Fechamento", "Máximo", "Mínimo", "Volume"]
                        
                        # Tabela
                        st.dataframe(df, use_container_width=True)
                        
                        # Download CSV
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

# Lista de ativos disponíveis
elif opcao == "📈 Ativos Disponíveis":
    st.header("📈 Ativos Disponíveis")
    
    if st.button("Carregar Ativos", type="primary"):
        with st.spinner("Carregando ativos..."):
            try:
                # Chama API
                response = requests.get(f"{API_URL}/api/ativos")
                
                if response.status_code == 200:
                    data = response.json()
                    
                    # Métrica
                    st.metric("Total de Ativos", data["total"])
                    
                    # DataFrame
                    ativos = data["ativos"]
                    
                    # Exibição em colunas
                    num_colunas = 5
                    colunas = st.columns(num_colunas)
                    
                    for i, ativo in enumerate(ativos):
                        col_idx = i % num_colunas
                        with colunas[col_idx]:
                            st.text(ativo)
                    
                    st.markdown("---")
                    
                    # Download CSV
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

# Ativos por intervalo de datas
elif opcao == "🗓️ Ativos por Intervalo":
    st.header("🗓️ Série de Fechamento por Intervalo")

    # Carrega datas disponíveis (cacheado)
    datas_disponiveis = []
    try:
        payload = fetch_datas(API_URL)
        datas_disponiveis = [str(item["data"]) for item in payload.get("datas", [])]
    except requests.exceptions.HTTPError as e:
        if getattr(e.response, "status_code", None) == 404:
            st.warning("Nenhuma data disponível encontrada na API.")
        else:
            st.error(f"Erro carregando datas: {getattr(e.response, 'status_code', 'desconhecido')}")
    except Exception as e:
        st.error(f"Erro ao carregar datas: {e}")

    if not datas_disponiveis:
        st.info("Carregue dados primeiro para habilitar essa consulta.")
    else:
        # Carrega ativos (cacheado)
        try:
            ativos = fetch_ativos(API_URL)
        except Exception:
            ativos = []

        col1, col2, col3 = st.columns(3)
        with col1:
            idx_inicio = max(len(datas_disponiveis) - 30, 0)
            data_inicio = st.selectbox(
                "Data inicial:", options=datas_disponiveis, index=idx_inicio
            )
        with col2:
            data_fim = st.selectbox(
                "Data final:", options=datas_disponiveis, index=len(datas_disponiveis) - 1
            )
        with col3:
            ativo_sel = st.selectbox(
                "Ativo:", options=ativos if ativos else [""], index=0
            )

        if data_fim < data_inicio:
            st.warning("A data final deve ser maior ou igual à inicial.")
        elif not ativo_sel:
            st.warning("Selecione um ativo para continuar.")
        else:
            if st.button("Buscar Fechamento", type="primary"):
                with st.spinner("Consultando série de fechamento..."):
                    try:
                        payload = fetch_fechamento(API_URL, data_inicio, data_fim, ativo_sel)
                        if isinstance(payload, dict) and payload.get("status") == 404:
                            st.warning("Nenhum dado encontrado para o ativo/período informado.")
                        else:
                            # Métricas
                            colm1, colm2, colm3 = st.columns(3)
                            colm1.metric("Ativo", payload.get("ativo", ativo_sel))
                            colm2.metric("Início", payload.get("inicio", data_inicio))
                            colm3.metric("Fim", payload.get("fim", data_fim))

                            serie = payload.get("serie", [])
                            if serie:
                                df = pd.DataFrame(serie)
                                df["data"] = pd.to_datetime(df["data"])  # para eixo do tempo

                                # Gráfico de linha com marcadores
                                fig = go.Figure(
                                    data=[
                                        go.Scatter(
                                            x=df["data"],
                                            y=df["fechamento"],
                                            mode="lines+markers",
                                            name="Fechamento",
                                            line=dict(color="#1f77b4", width=2),
                                            marker=dict(size=5),
                                            hovertemplate="Data: %{x|%Y-%m-%d}<br>Fechamento: R$ %{y:.2f}<extra></extra>",
                                        )
                                    ]
                                )
                                fig.update_layout(
                                    margin=dict(l=0, r=0, t=10, b=0),
                                    height=420,
                                    xaxis_title="Data",
                                    yaxis_title="Preço de Fechamento (R$)",
                                    xaxis=dict(showgrid=True, gridcolor="rgba(0,0,0,0.08)", tickangle=-30),
                                    yaxis=dict(showgrid=True, gridcolor="rgba(0,0,0,0.08)", zeroline=False),
                                )
                                st.plotly_chart(fig, use_container_width=True, theme="streamlit")

                                # Tabela e CSV
                                df_out = df.copy()
                                df_out["data"] = df_out["data"].dt.date
                                st.dataframe(df_out.rename(columns={"data": "Data", "fechamento": "Fechamento"}), use_container_width=True, height=360)

                                csv = df_out.rename(columns={"data": "Data", "fechamento": "Fechamento"}).to_csv(index=False).encode("utf-8")
                                st.download_button(
                                    label="📥 Baixar CSV",
                                    data=csv,
                                    file_name=f"fechamento_{ativo_sel}_{data_inicio}_a_{data_fim}.csv",
                                    mime="text/csv"
                                )
                    except requests.exceptions.ConnectionError:
                        st.error("❌ Não foi possível conectar à API.")
                    except Exception as e:
                        st.error(f"Erro: {e}")

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
