# Arquivo: atualizar_dados.py
import os
import sys
import pandas as pd
import requests
from datetime import datetime
from pytz import timezone

# --- A função obter_token e atualizar_csv_diario continuam as mesmas ---

def obter_token(email, senha):
    """Obtém o token de autenticação da API do CEMADEN."""
    if not email or not senha:
        print("ERRO: Credenciais do Cemaden (email/senha) não encontradas nos segredos.", file=sys.stderr)
        sys.exit(1)
    try:
        token_url = 'https://sgaa.cemaden.gov.br/SGAA/rest/controle-token/tokens'
        login = {'email': email, 'password': senha}
        print("Tentando obter o token de acesso...")
        response = requests.post(token_url, json=login)
        response.raise_for_status()
        content = response.json()
        token = content.get('token')
        if token:
            print("✅ Token obtido com sucesso!")
            return token
        else:
            print("❌ Erro: A resposta da API não continha um token.", file=sys.stderr)
            return None
    except requests.exceptions.RequestException as e:
        print(f"❌ Erro ao obter token: {e}", file=sys.stderr)
        return None

def buscar_dados_cemaden(token, lista_estacoes, uf='PE', rede='11', sensor='10'):
    """
    Busca os dados e JÁ CONVERTE os horários para o fuso local de Recife.
    """
    if not token:
        print("❌ Token de acesso não fornecido.", file=sys.stderr)
        return pd.DataFrame()
    
    url_base = 'https://sws.cemaden.gov.br/PED/rest/pcds/pcds-dados-recentes'
    headers = {'token': token}
    lista_dfs = []
    print(f"\nBuscando dados para {len(lista_estacoes)} estações...")
    
    for codestacao in lista_estacoes:
        params = {'codestacao': codestacao, 'uf': uf, 'rede': rede, 'sensor': sensor, 'formato': 'JSON'}
        try:
            response = requests.get(url_base, headers=headers, params=params)
            response.raise_for_status()
            dados = response.json()
            if isinstance(dados, dict) and 'Nenhum resultado foi encontrado' in dados.get('Info', ''):
                print(f"⚠️ Estação {codestacao} retornou uma mensagem de 'não encontrado'. Ignorando.")
                continue
            if dados:
                if isinstance(dados, dict):
                    dados_para_df = [dados]
                else:
                    dados_para_df = dados
                lista_dfs.append(pd.DataFrame(dados_para_df))
            else:
                print(f"⚠️ Nenhum dado encontrado para a estação {codestacao}.")
        except requests.exceptions.RequestException as e:
            print(f"❌ Erro ao buscar dados para a estação {codestacao}: {e}", file=sys.stderr)
            
    if not lista_dfs:
        print("Nenhum dado foi retornado pela API.")
        return pd.DataFrame()
        
    print("✅ Dados obtidos com sucesso!")
    df_final = pd.concat(lista_dfs, ignore_index=True)

  
    if not df_final.empty and 'datahora' in df_final.columns:
        print("Convertendo novos dados para o fuso horário de Recife (UTC-3)...")
        # 1. Converte a coluna para o tipo datetime
        df_final['datahora'] = pd.to_datetime(df_final['datahora'])
        # 2. Informa que o fuso original é UTC e converte para o fuso de Recife
        df_final['datahora'] = df_final['datahora'].dt.tz_localize('UTC').dt.tz_convert('America/Recife')
        # 3. Formata de volta para texto, para salvar um CSV limpo
        df_final['datahora'] = df_final['datahora'].dt.strftime('%Y-%m-%d %H:%M:%S')
    
    
    return df_final


def atualizar_csv_diario(df_novos_dados, nome_arquivo):
    if os.path.exists(nome_arquivo):
        print(f"Arquivo '{nome_arquivo}' encontrado. Carregando dados existentes...")
        try:
            df_existente = pd.read_csv(nome_arquivo)
            df_combinado = pd.concat([df_existente, df_novos_dados], ignore_index=True)
            print("Dados novos e antigos combinados.")
        except pd.errors.EmptyDataError:
            df_combinado = df_novos_dados
    else:
        print(f"Arquivo '{nome_arquivo}' não encontrado. Será criado um novo.")
        df_combinado = df_novos_dados
    
    num_linhas_antes = len(df_combinado)
    df_final = df_combinado.drop_duplicates(subset=['codestacao', 'datahora'], keep='last')
    num_linhas_depois = len(df_final)
    
    num_removidas = num_linhas_antes - num_linhas_depois
    if num_removidas > 0:
        print(f"{num_removidas} linha(s) duplicada(s) foram removidas.")

    df_final.to_csv(nome_arquivo, index=False)
    print(f"✅ Arquivo '{nome_arquivo}' salvo com sucesso! Total de {num_linhas_depois} registros.")


def main():
    """Função principal que orquestra todo o processo."""
    
    cemaden_email = os.getenv("CEMADEN_EMAIL")
    cemaden_senha = os.getenv("CEMADEN_SENHA")
    
    token_acesso = obter_token(cemaden_email, cemaden_senha)
    
    if token_acesso:
        estacoes_de_recife = [
            '261160614A', '261160609A', '261160623A', '261160618A', '261160603A'
        ]
        
        df_chuva_recente = buscar_dados_cemaden(token_acesso, estacoes_de_recife)

        if not df_chuva_recente.empty:
            tz_recife = timezone('America/Recife')
            agora_em_recife = datetime.now(tz_recife)
            data_hoje = agora_em_recife.strftime('%Y-%m-%d')
            
            nome_arquivo_diario = f"chuva_recife_{data_hoje}.csv"
            atualizar_csv_diario(df_chuva_recente, nome_arquivo_diario)
        else:
            print("Nenhum dado novo foi retornado pela API.")
    else:
        print("Falha ao obter token do Cemaden, finalizando a execução.")

if __name__ == "__main__":
    main()