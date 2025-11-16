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

# URL da API (ajuste para produção após deploy)
API_URL = "http://localhost:8000"

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

    # Carrega datas disponíveis
    datas_disponiveis = []
    try:
        resp_datas = requests.get(f"{API_URL}/api/cotacoes/datas", timeout=20)
        if resp_datas.status_code == 200:
            payload = resp_datas.json()
            # Lista de strings ISO das datas
            datas_disponiveis = [str(item["data"]) for item in payload.get("datas", [])]
        elif resp_datas.status_code == 404:
            st.warning("Nenhuma data disponível encontrada na API.")
        else:
            st.error(f"Erro carregando datas: {resp_datas.status_code}")
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
    st.header("🗓️ Ativos por Intervalo de Datas")

    # Carrega datas disponíveis para limitar o range
    datas_disponiveis = []
    try:
        resp_datas = requests.get(f"{API_URL}/api/cotacoes/datas", timeout=20)
        if resp_datas.status_code == 200:
            payload = resp_datas.json()
            datas_disponiveis = [str(item["data"]) for item in payload.get("datas", [])]
        elif resp_datas.status_code == 404:
            st.warning("Nenhuma data disponível encontrada na API.")
        else:
            st.error(f"Erro carregando datas: {resp_datas.status_code}")
    except Exception as e:
        st.error(f"Erro ao carregar datas: {e}")

    if not datas_disponiveis:
        st.info("Carregue dados primeiro para habilitar essa consulta.")
    else:
        col1, col2 = st.columns(2)
        with col1:
            idx_inicio = max(len(datas_disponiveis) - 5, 0)
            data_inicio = st.selectbox(
                "Data inicial:", options=datas_disponiveis, index=idx_inicio
            )
        with col2:
            data_fim = st.selectbox(
                "Data final:", options=datas_disponiveis, index=len(datas_disponiveis) - 1
            )

        if data_fim < data_inicio:
            st.warning("A data final deve ser maior ou igual à inicial.")
        else:
            if st.button("Buscar Ativos", type="primary"):
                with st.spinner("Consultando ativos no intervalo..."):
                    try:
                        url = f"{API_URL}/api/ativos/intervalo?inicio={data_inicio}&fim={data_fim}"
                        r = requests.get(url, timeout=60)
                        if r.status_code == 200:
                            payload = r.json()

                            col1, col2, col3 = st.columns(3)
                            col1.metric("Ativos distintos", payload.get("total_ativos", 0))
                            col2.metric("Registros no intervalo", payload.get("total_registros", 0))
                            col3.markdown(
                                f"<div style='font-size:0.9rem; color:gray; line-height:1.2'><b>Período</b><br>{payload.get('inicio')} → {payload.get('fim')}</div>",
                                unsafe_allow_html=True
                            )

                            dados = payload.get("ativos", [])
                            if dados:
                                df = pd.DataFrame(dados)
                                df = df.rename(columns={"ativo": "Ativo", "total": "Qtde Registros"})
                                df = df.sort_values("Qtde Registros", ascending=False)
                                st.dataframe(df, use_container_width=True, height=420)

                                csv = df.to_csv(index=False).encode("utf-8")
                                st.download_button(
                                    label="📥 Baixar CSV",
                                    data=csv,
                                    file_name=f"ativos_intervalo_{data_inicio}_a_{data_fim}.csv",
                                    mime="text/csv"
                                )
                        elif r.status_code == 404:
                            st.warning("Nenhum ativo encontrado no intervalo informado.")
                        else:
                            st.error(f"Erro da API: {r.status_code}")
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
