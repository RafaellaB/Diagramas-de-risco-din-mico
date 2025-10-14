import pandas as pd
import requests
import numpy as np
from datetime import datetime, date
import pytz 
import streamlit as st 
import plotly.graph_objects as go 

# 1. ambiente dos arquivos
URL_BASE_CHUVAS = 'https://raw.githubusercontent.com/RafaellaB/Diagramas-de-risco-din-mico/main/chuva_recife_' 
SUFIXO_ARQUIVO_CHUVAS = '.csv'
URL_ARQUIVO_MARE_AM = 'https://raw.githubusercontent.com/RafaellaB/Diagramas-de-risco-din-mico/main/tide/mare_calculada_hora_em_hora_ano-completo.csv'
CSV_DELIMITADOR = ',' 
COLUNAS_NO_CSV_CHUVAS = ['datahora', 'nome', 'valor'] 
COLUNAS_ESPERADAS_VP = ['datahora', 'nomeEstacao', 'valorMedida'] 
# ==============================================================================


# 2. funções cache para melhorar a performance  


@st.cache_data(show_spinner=False)
def carregar_dados_mare_cache(url_am_data):
    """ Carrega o arquivo de maré (AM) ANUAL. Cache estático. """
    df_am_raw = pd.read_csv(url_am_data)
    df_am_raw.rename(columns={'datahora': 'datahora', 'altura': 'AM'}, inplace=True)
    df_am_raw['datahora'] = pd.to_datetime(df_am_raw['datahora'])
    df_am_raw['data'] = df_am_raw['datahora'].dt.strftime('%Y-%m-%d')
    df_am_raw['hora_ref'] = df_am_raw['datahora'].dt.strftime('%H:00:00')
    return df_am_raw[['data', 'hora_ref', 'AM']]


@st.cache_data(ttl=300, show_spinner=False) # TTL = 300 segundos (5 minutos)
def carregar_dados_chuva_cache(url_base, data_de_hoje_str, separador, colunas_csv):
    """ Lê o arquivo de chuva do dia atual. Cache expira a cada 5 minutos. """
    url_completa = f"{url_base}{data_de_hoje_str}{SUFIXO_ARQUIVO_CHUVAS}"
    
    try:
        df_chuva_raw = pd.read_csv(url_completa, encoding='utf-8', sep=separador)
        
        if not all(col in df_chuva_raw.columns for col in colunas_csv):
             st.error(f"ERRO DE COLUNA: O arquivo de chuva não tem as colunas esperadas: {colunas_csv}")
             return pd.DataFrame()

        df_chuva_raw.rename(columns={'nome': 'nomeEstacao', 'valor': 'valorMedida'}, inplace=True)
        df_chuva_raw['datahora'] = pd.to_datetime(df_chuva_raw['datahora'])
        
        return df_chuva_raw
    
    except Exception as e:
        st.error(f"ERRO ao carregar arquivo de chuva de hoje ({data_de_hoje_str}). Verifique se o arquivo já existe no GitHub. Detalhe: {e}")
        return pd.DataFrame()


# 3. funções de processamento

def processar_dados_chuva_simplificado(df_chuva, datas_desejadas, estacoes_desejadas):
    """ Calcula o indicador horário de chuva 'VP'. """
    df = df_chuva[df_chuva['nomeEstacao'].isin(estacoes_desejadas)].copy()
    df['data'] = df['datahora'].dt.date.astype(str)
    df = df[df['data'].isin(datas_desejadas)]
    if df.empty: return pd.DataFrame()
    df = df.set_index('datahora').sort_index()
    resultados_por_estacao = []
    for estacao, grupo in df.groupby('nomeEstacao'):
        chuva_10min = grupo['valorMedida'].rolling('10min').sum()
        chuva_2h = grupo['valorMedida'].rolling('2h').sum()
        temp_df = pd.DataFrame({'chuva_10min': chuva_10min, 'chuva_2h': chuva_2h})
        agregado_horario = temp_df.resample('h').last()
        agregado_horario['VP'] = (agregado_horario['chuva_10min'] * 6) + agregado_horario['chuva_2h']
        agregado_horario['nomeEstacao'] = estacao
        resultados_por_estacao.append(agregado_horario)
    df_vp = pd.concat(resultados_por_estacao).reset_index()
    df_vp.dropna(subset=['VP'], inplace=True)
    df_vp['data'] = df_vp['datahora'].dt.strftime('%Y-%m-%d')
    df_vp['hora_ref'] = df_vp['datahora'].dt.strftime('%H:00:00')
    return df_vp[['data', 'hora_ref', 'nomeEstacao', 'VP']]


def calcular_risco(df_final):
    """ Calcula o Nível de Risco (VP * AM) e a Classificação. """
    if df_final.empty: return pd.DataFrame()
    df_final['VP'] = pd.to_numeric(df_final['VP'], errors='coerce').round(2) 
    df_final['AM'] = pd.to_numeric(df_final['AM'], errors='coerce').round(2)
    df_final['Nivel_Risco_Valor'] = (df_final['VP'] * df_final['AM']).fillna(0).round(2)
    bins = [-np.inf, 30, 50, 100, np.inf]
    labels = ['Baixo', 'Moderado', 'Moderado Alto', 'Alto']
    df_final['Classificacao_Risco'] = pd.cut(df_final['Nivel_Risco_Valor'], bins=bins, labels=labels, right=False)
    return df_final


def executar_analise_risco_completa(df_vp_calculado, df_am):
    """ Mescla VP e AM e chama o cálculo de risco. """
    if df_vp_calculado.empty: return pd.DataFrame()
    df_final = pd.merge(df_vp_calculado, df_am, on=['data', 'hora_ref'], how='left')
    df_risco = calcular_risco(df_final)
    return df_risco


def gerar_diagramas(df_analisado):
    """ Gera o diagrama de risco (Heatmap + Scatter) para cada estação/dia. """
    mapa_de_cores = {'Alto': '#D32F2F', 'Moderado Alto': '#FFA500', 'Moderado': '#FFC107', 'Baixo': '#4CAF50'}
    definicoes_risco = {'Baixo': 'RA < 30', 'Moderado': '30 ≤ RA < 50', 'Moderado Alto': '50 ≤ RA < 100', 'Alto': 'RA ≥ 100'}
    
    for (data, estacao), grupo in df_analisado.groupby(['data', 'nomeEstacao']):
        if grupo.empty: continue
        
        st.subheader(f"Diagrama de Risco: {estacao} - {pd.to_datetime(data).strftime('%d/%m/%Y')}")
        fig = go.Figure()
        
        lim_x = max(110, grupo['VP'].max() * 1.2 if not grupo.empty else 110)
        lim_y = 5 
        x_grid, y_grid = np.arange(0, lim_x, 1), np.linspace(0, lim_y, 100)
        z_grid = np.array([x * y for y in y_grid for x in x_grid]).reshape(len(y_grid), len(x_grid))
        
        colorscale = [[0, "#90EE90"], [30/100, "#FFD700"], [50/100, "#FFA500"], [1.0, "#D32F2F"]]
        fig.add_trace(go.Heatmap(x=x_grid, y=y_grid, z=z_grid, colorscale=colorscale, showscale=False, zmin=0, zmax=100, hoverinfo='none'))
        
        grupo = grupo.sort_values(by='hora_ref')
        fig.add_trace(go.Scatter(x=grupo['VP'], y=grupo['AM'], mode='lines', line=dict(color='black', width=1.5, dash='dash'), hoverinfo='none', showlegend=False))
        
        for _, ponto in grupo.iterrows():
            cor_ponto = mapa_de_cores.get(ponto['Classificacao_Risco'], 'black')
            fig.add_trace(go.Scatter(x=[ponto['VP']], y=[ponto['AM']], mode='markers', marker=dict(color=cor_ponto, size=12, line=dict(width=1, color='black')), 
                                     hoverinfo='text', 
                                     hovertext=f"<b>Hora:</b> {ponto['hora_ref']}<br><b>Risco:</b> {ponto['Classificacao_Risco']} ({ponto['Nivel_Risco_Valor']})<br><b>VP:</b> {ponto['VP']}<br><b>AM:</b> {ponto['AM']}", 
                                     showlegend=False))
        
        for risco, definicao in definicoes_risco.items():
            fig.add_trace(go.Scatter(x=[None], y=[None], mode='markers', marker=dict(color=mapa_de_cores[risco], size=10, symbol='square'), name=f"<b>{risco}</b>: {definicao}"))
        
        fig.update_layout(title=f'<b>{estacao}</b>', 
                          xaxis_title='Índice de Precipitação (mm)', 
                          yaxis_title='Índice de Altura da Maré (m)', 
                          margin=dict(l=40, r=40, t=40, b=40), 
                          showlegend=True, 
                          legend_title_text='<b>Níveis de Risco</b>')
        
        st.plotly_chart(fig, use_container_width=True, key=f"chart_{data}_{estacao}")


#bloco de execução principal


if __name__ == "__main__":
    
    # Define a data de hoje (America/Recife)
    fuso_horario_referencia = pytz.timezone('America/Recife') 
    data_hoje = datetime.now(fuso_horario_referencia).date()
    data_hoje_str = data_hoje.strftime('%Y-%m-%d')
    
    datas_para_analise = [data_hoje_str]
    estacoes_desejadas = ["Campina do Barreto", "Torreão", "RECIFE - APAC", "Imbiribeira", "Dois Irmãos"]
    
    st.title("Diagramas de Risco Dinâmico para Alagamentos - Hoje")

    #botão de refresh e status
    col1, col2 = st.columns([1, 4])
    
    # O botão de atualização, agora garantindo a limpeza
    if col1.button("Atualizar Dados", type="primary"):
        # Limpa o cache da função de chuva
        carregar_dados_chuva_cache.clear()
        # Força o Streamlit a reexecutar o script a partir do topo
        st.rerun() 
        

   
    try:
        # Carrega a Maré (AM) - Estático
        with st.spinner("Carregando Maré..."):
             df_am = carregar_dados_mare_cache(URL_ARQUIVO_MARE_AM)
        
        # Carrega a Chuva (VP) - Dinâmico (cache de 5 min ou botão)
       
        df_chuva_raw = carregar_dados_chuva_cache(
            URL_BASE_CHUVAS, 
            data_hoje_str, 
            CSV_DELIMITADOR, 
            COLUNAS_NO_CSV_CHUVAS
        )
        
    except Exception as e:
        st.error(f"Ocorreu um erro no carregamento inicial dos dados. Detalhe: {e}")
        st.stop() 

    # Condicional de exibição e cálculo
    if df_chuva_raw.empty or df_am.empty:
        st.warning(f"Não foi possível iniciar a análise. Verifique o log de erros ou se os arquivos existem para {data_hoje_str}.")
        
    else:
        # 3. Processa VP e Calcula Risco (Silencioso)
        df_vp_calculado = processar_dados_chuva_simplificado(df_chuva_raw, datas_para_analise, estacoes_desejadas)
        df_risco_final = executar_analise_risco_completa(df_vp_calculado, df_am)
        
        if not df_risco_final.empty:
            st.success("Análise de Risco Concluída!")
            
            # 4. geração e exibição dos diagramas
            gerar_diagramas(df_risco_final)

            # Opção para ver a tabela detalhada (Streamlit)
            with st.expander("Ver Tabela de Risco Detalhada"):
                 st.dataframe(df_risco_final[['data', 'hora_ref', 'nomeEstacao', 'VP', 'AM', 'Nivel_Risco_Valor', 'Classificacao_Risco']])

        else:
            st.error("O cálculo de risco final falhou. Verifique as colunas de merge.")