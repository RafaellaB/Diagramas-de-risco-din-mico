
#script marca somente o ponto da hora atual
#sript com funcionamento dos dados da api do cemaden, mas com cálculos de maré para ajeitar. 
# ==============================================================================
import streamlit as st
import pandas as pd
import numpy as np
import requests
import sys
import plotly.graph_objects as go
from datetime import datetime, timedelta
from urllib.parse import quote

# ==============================================================================
# CONFIGURAÇÃO DA PÁGINA
# ==============================================================================
st.set_page_config(
    layout="wide",
    page_title="Diagramas de Risco para Alagamentos e Inundações"
)

# ==============================================================================
# DICIONÁRIOS DE MAPEAMENTO E FUNÇÕES DA API
# ==============================================================================
# Substitua com seu e-mail e senha
email = "rafaellabmoura@gmail.com"
senha = "Rf3m25@BrXt!"

# Mapeamento do código da estação para o nome
mapeamento_nomes = {
    '261160614A': 'Campina do Barreto',
    '261160609A': 'Imbiribeira',
    '261160623A': 'RECIFE - APAC',
    '261160618A': 'Torreão'
}

# Mapeamento de geolocalização e município
mapeamento_localizacao = {
    '261160614A': {'municipio': 'Recife', 'uf': 'PE', 'latitude': -8.0494, 'longitude': -34.897},
    '261160609A': {'municipio': 'Recife', 'uf': 'PE', 'latitude': -8.1091, 'longitude': -34.914},
    '261160623A': {'municipio': 'Recife', 'uf': 'PE', 'latitude': -8.0542, 'longitude': -34.881},
    '202401010000': {'municipio': 'Recife', 'uf': 'PE', 'latitude': -8.0416, 'longitude': -34.892}
}

def obter_token(email, senha):
    """Obtém o token de autenticação da API do CEMADEN."""
    try:
        token_url = 'https://sgaa.cemaden.gov.br/SGAA/rest/controle-token/tokens'
        login = {'email': email, 'password': senha}
        response = requests.post(token_url, json=login)
        response.raise_for_status()
        content = response.json()
        return content.get('token')
    except requests.exceptions.RequestException as e:
        st.error(f"Erro ao obter token: {e}")
        return None

def buscar_dados_brutos_recentes(token, estacoes_especificas, rede='11', sensor='10', uf='PE'):
    """
    Busca os dados brutos de chuva recentes de cada estação da API do CEMADEN.
    """
    url_base = 'https://sws.cemaden.gov.br/PED/rest/pcds/pcds-dados-recentes'
    lista_dfs = []
    
    for codestacao in estacoes_especificas:
        params = {
            'codestacao': codestacao,
            'rede': rede,
            'uf': uf,
            'sensor': sensor,
            'formato': 'JSON'
        }
        headers = {'token': token}
        
        try:
            response = requests.get(url_base, headers=headers, params=params, timeout=10)
            response.raise_for_status()
            dados = response.json()
            if dados:
                df_temp = pd.DataFrame(dados)
                lista_dfs.append(df_temp)
            else:
                st.warning(f"Nenhum dado bruto disponível para estação {codestacao}.")
        except requests.exceptions.RequestException as e:
            st.error(f'Erro ao buscar dados brutos para estação {codestacao}: {e}')

    if not lista_dfs:
        return pd.DataFrame()
    return pd.concat(lista_dfs, ignore_index=True)

# ==============================================================================
# FUNÇÕES PRINCIPAIS DO STREAMLIT
# ==============================================================================
@st.cache_data(ttl=60)
def carregar_dados_dinamicos(email, senha):
    """
    Busca os dados de chuva da API do CEMADEN, processa e retorna o DataFrame final.
    A função é otimizada com cache para ser executada a cada 60 segundos.
    """
    st.info("Carregando dados da API do CEMADEN...")
    estacoes_especificas = list(mapeamento_nomes.keys())

    token_acesso = obter_token(email, senha)
    if not token_acesso:
        return pd.DataFrame()

    df_bruto = buscar_dados_brutos_recentes(token_acesso, estacoes_especificas)

    if df_bruto.empty:
        return pd.DataFrame()
    
    # === AQUI ESTÁ A LÓGICA DE CÁLCULO E CONVERSÃO ===
    df_bruto['datahora'] = pd.to_datetime(df_bruto['datahora'])
    df_bruto.set_index('datahora', inplace=True)
    df_bruto = df_bruto[df_bruto['id_sensor'] == 10]
    
    # Converte o fuso horário de UTC para o de Recife
    fuso_horario_recife = 'America/Recife'
    df_bruto = df_bruto.tz_localize('UTC').tz_convert(fuso_horario_recife)

    # Removendo o fuso horário para compatibilidade com o df_mare
    df_bruto.index = df_bruto.index.tz_localize(None)

    lista_vp = []
    for codestacao, grupo in df_bruto.groupby('codestacao'):
        v10min_df = grupo['valor'].resample('10T').sum().fillna(0)
        v2horas_df = grupo['valor'].resample('120T').sum().fillna(0)
        
        v10min_ultimo = v10min_df.iloc[-1] if not v10min_df.empty else 0
        v2horas_ultimo = v2horas_df.iloc[-1] if not v2horas_df.empty else 0
        
        vp_calculado = (v10min_ultimo * 6) + v2horas_ultimo
        
        linha_df = pd.DataFrame([{
            'codEstacao': codestacao,
            'nomeEstacao': grupo['nome'].iloc[0],
            'datahora': grupo.index[-1],
            'valorMedida': vp_calculado
        }])
        lista_vp.append(linha_df)

    if not lista_vp:
        return pd.DataFrame()
    
    df_chuva_final = pd.concat(lista_vp, ignore_index=True)
    df_chuva_final['data'] = df_chuva_final['datahora'].dt.date.astype(str)

    return df_chuva_final

@st.cache_data
def carregar_dados_brutos_mare():
    """Carrega os dados de maré do GitHub e aplica cache."""
    print("Carregando dados brutos de maré do ano de 2025...")
    
    arquivo_mare = 'mare_porto_recife_2025.csv'
    url_mare = f'https://raw.githubusercontent.com/RafaellaB/Dados-Pluviom-tricos-CEMADEN/main/{quote(arquivo_mare)}'
    
    # Ler o arquivo e renomear as colunas
    df_mare = pd.read_csv(url_mare, sep=',', encoding='latin1')
    df_mare.rename(columns={'altura_m': 'altura'}, inplace=True)
    
    # Tratar erros de data com 'errors=coerce' e remover as linhas com erro
    df_mare['data'] = pd.to_datetime(df_mare['data'], format='%d/%m/%Y', errors='coerce')
    df_mare.dropna(subset=['data'], inplace=True)
    
    return df_mare

def executar_analise(df_chuva, df_mare, estacoes_desejadas):
    """
    Executa o pipeline de análise com o cálculo de AM dinâmico
    para cada intervalo de maré.
    """
    # PROCESSAMENTO DA CHUVA ---
    st.info("Processando dados de chuva...")
    df_filtrado_chuva = df_chuva[df_chuva['nomeEstacao'].isin(estacoes_desejadas)].copy()
    if df_filtrado_chuva.empty:
        return pd.DataFrame()

    df_filtrado_chuva['VP'] = df_filtrado_chuva['valorMedida']
    df_filtrado_chuva['hora_ref'] = pd.to_datetime(df_filtrado_chuva['datahora']).dt.strftime('%H:%M:%S')

    # PROCESSAMENTO DA MARÉ POR INTERVALO
    st.info("Processando dados de maré por intervalo...")
    df_mare['data_str'] = df_mare['data'].dt.strftime('%Y-%m-%d')
    df_mare['datahora'] = pd.to_datetime(df_mare['data_str'] + ' ' + df_mare['hora'])
    dados_filtrados_mare = df_mare[df_mare['data_str'].isin(df_filtrado_chuva['data'].unique())].copy()
    dados_filtrados_mare.sort_values(by='datahora', inplace=True)
    
    intervalos_am = []
    for data, grupo_dia in dados_filtrados_mare.groupby('data_str'):
        for i in range(len(grupo_dia) - 1):
            ponto_atual = grupo_dia.iloc[i]
            ponto_seguinte = grupo_dia.iloc[i+1]
            I1 = ponto_atual['altura']
            I2 = ponto_seguinte['altura']
            AM_intervalo = round(((I1 - I2) / 6) + I1, 2)
            intervalos_am.append({
                'datahora_inicio': ponto_atual['datahora'],
                'datahora_fim': ponto_seguinte['datahora'],
                'data': data,
                'AM': AM_intervalo
            })
    df_am_intervalos = pd.DataFrame(intervalos_am)
    
    st.info("Unindo dados de chuva e maré...")
    df_final = df_filtrado_chuva.copy()
    df_final['AM'] = np.nan
    
    for _, intervalo in df_am_intervalos.iterrows():
        inicio = intervalo['datahora_inicio']
        fim = intervalo['datahora_fim']
        indices = df_final[(df_final['datahora'] >= inicio) & (df_final['datahora'] < fim)].index
        df_final.loc[indices, 'AM'] = intervalo['AM']
    
    # ANÁLISE DE RISCO ---
    df_final.dropna(subset=['AM'], inplace=True) 
    df_final['VP'] = df_final['VP'].round(2)
    df_final['AM'] = df_final['AM'].round(2)
    df_final['Nivel_Risco_Valor'] = (df_final['VP'] * df_final['AM']).fillna(0).round(2)
    bins = [-np.inf, 30, 50, 100, np.inf]
    labels = ['Baixo', 'Moderado', 'Moderado Alto', 'Alto']
    df_final['Classificacao_Risco'] = pd.cut(df_final['Nivel_Risco_Valor'], bins=bins, labels=labels, right=False)
    
    return df_final.sort_values(by=['data', 'nomeEstacao', 'hora_ref'], ignore_index=True)

def gerar_diagramas(df_analisado):
    # Dicionário que mapeia a classificação para uma cor
    mapa_de_cores = {'Alto': '#D32F2F', 'Moderado Alto': '#FFA500', 'Moderado': '#FFC107', 'Baixo': '#4CAF50'}
    
    # Dicionário com o texto para cada nível de risco na legenda
    definicoes_risco = {
        'Baixo': 'RA < 30',
        'Moderado': '30 ≤ RA < 50',
        'Moderado Alto': '50 ≤ RA < 100',
        'Alto': 'RA ≥ 100'
    }

    # Loop para criar um gráfico para cada dia e estação
    for (data, estacao), grupo in df_analisado.groupby(['data', 'nomeEstacao']):
        if grupo.empty: continue
        
        st.subheader(f"Diagrama de Risco: {estacao} - {pd.to_datetime(data).strftime('%d/%m/%Y')}")
        fig = go.Figure()

        # Configuração do gráfico (fundo, limites, etc.)
        lim_x = max(110, grupo['VP'].max() * 1.2) if not grupo.empty else 110
        lim_y = 5
        x_grid, y_grid = np.arange(0, lim_x, 1), np.linspace(0, lim_y, 100)
        z_grid = np.array([x * y for y in y_grid for x in x_grid]).reshape(len(y_grid), len(x_grid))
        colorscale = [[0, "#90EE90"], [30/100, "#FFD700"], [50/100, "#FFA500"], [1.0, "#D32F2F"]]
        fig.add_trace(go.Heatmap(x=x_grid, y=y_grid, z=z_grid, colorscale=colorscale, showscale=False, zmin=0, zmax=100))
        
        fig.add_trace(go.Heatmap(
            x=x_grid, y=y_grid, z=z_grid, colorscale=colorscale, showscale=False, zmin=0, zmax=100,
            hoverinfo='none' 
        ))
        
        grupo = grupo.sort_values(by='hora_ref')
        fig.add_trace(go.Scatter(x=grupo['VP'], y=grupo['AM'], mode='lines', line=dict(color='black', width=1.5, dash='dash'), hoverinfo='none', showlegend=False))
        
        for _, ponto in grupo.iterrows():
            cor_ponto = mapa_de_cores.get(ponto['Classificacao_Risco'], 'black')
            fig.add_trace(go.Scatter(
                x=[ponto['VP']], y=[ponto['AM']], mode='markers',
                marker=dict(color=cor_ponto, size=12, line=dict(width=1, color='black')),
                hoverinfo='text',
                hovertext=f"<b>Hora:</b> {ponto['hora_ref']}<br><b>Risco:</b> {ponto['Classificacao_Risco']} ({ponto['Nivel_Risco_Valor']})<br><b>VP:</b> {ponto['VP']}<br><b>AM:</b> {ponto['AM']}",
                showlegend=False
            ))

        for risco, definicao in definicoes_risco.items():
            fig.add_trace(go.Scatter(
                x=[None], y=[None],
                mode='markers',
                marker=dict(color=mapa_de_cores[risco], size=10, symbol='square'),
                name=f"<b>{risco}</b>: {definicao}"
            ))
        
        fig.update_layout(
            title=f'<b>{estacao}</b>',
            xaxis_title='Índice de Precipitação (mm)',
            yaxis_title='índice de Altura da Maré (m)',
            margin=dict(l=40, r=40, t=40, b=40),
            showlegend=True,
            legend_title_text='<b>Níveis de Risco</b>'
        )
        
        chave_unica = f"chart_{data}_{estacao}"
        st.plotly_chart(fig, use_container_width=True, key=chave_unica)
        

# ==============================================================================
# INTERFACE DO STREAMLIT
# ==============================================================================

st.title("Diagramas de Risco para Alagamentos e Inundações")
st.info("O período de análise considera os dados mais recentes disponíveis. Atualização automática a cada 60 segundos.")

df_chuva_raw = carregar_dados_dinamicos(email, senha)
df_mare_raw = carregar_dados_brutos_mare()

st.sidebar.header("Filtros da Análise")
ESTACOES_DO_ESTUDO = list(mapeamento_nomes.values())
estacoes_selecionadas = st.sidebar.multiselect(
    'Selecione as Estações',
    options=ESTACOES_DO_ESTUDO,
    default=ESTACOES_DO_ESTUDO
)

if not estacoes_selecionadas:
    st.warning("Por favor, selecione pelo menos uma estação na barra lateral.")
else:
    if st.button("Iniciar Análise"):
        if df_chuva_raw.empty:
            st.info("Nenhum dado de chuva encontrado da API para o momento atual.")
        else:
            with st.spinner(f"Analisando dados para {len(estacoes_selecionadas)} estações..."):
                dados_analisados = executar_analise(df_chuva_raw, df_mare_raw, estacoes_selecionadas)

                if dados_analisados.empty:
                    st.info("Nenhum dado encontrado para os filtros selecionados.")
                else:
                    st.success("Análise concluída!")
                    
                    st.header("Relatório de Pontos por Zona de Risco")
                    for zona in ['Alto', 'Moderado Alto', 'Moderado', 'Baixo']:
                        pontos_na_zona = dados_analisados[dados_analisados['Classificacao_Risco'] == zona]
                        with st.expander(f"Pontos na Zona de Risco '{zona}': {len(pontos_na_zona)} ponto(s)"):
                            if not pontos_na_zona.empty:
                                st.dataframe(pontos_na_zona[['data', 'hora_ref', 'nomeEstacao', 'Nivel_Risco_Valor', 'VP', 'AM']])
     
                gerar_diagramas(dados_analisados)