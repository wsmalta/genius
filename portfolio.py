import hashlib
from datetime import date
import re
# import sqlite3 # Removido
import psycopg2 # Adicionado: Driver PostgreSQL
from psycopg2 import errors as psycopg2_errors
import json
import pandas as pd
import yfinance
import datetime
import time
import os
import io 
import csv
import logging
import urllib.parse
import concurrent.futures # Adicionado para performance
from google import genai
from google.genai import types
from google.genai.errors import APIError
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_JUSTIFY

# Definições de estilo (Cores)
PRIMARY_BLUE = '#0D47A1'
DARK_GREY = '#424242'

DB_URL = os.environ.get("DATABASE_URL")

# Tentativa de importar Streamlit
try:
    import streamlit as st
    STREAMLIT_ENV = True 
except ImportError:
    STREAMLIT_ENV = False

# --- Configuração do Logging ---
def setup_logging():
    """Configura o sistema de logging."""
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        if not STREAMLIT_ENV:
            file_handler = logging.FileHandler('portfolio.log', mode='a', encoding='utf-8')
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        else:
            stream_handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            stream_handler.setFormatter(formatter)
            logger.addHandler(stream_handler)

# --- Chave API Gemini ---
def get_gemini_api_key():
    """Obtém a chave API do Streamlit secrets (nuvem) ou de variáveis de ambiente (local)."""
    if STREAMLIT_ENV:
        try:
            if st.secrets.get("GEMINI_API_KEY"):
                logging.info("Chave API obtida via st.secrets['GEMINI_API_KEY'].")
                return st.secrets["GEMINI_API_KEY"]
            elif st.secrets.get("GOOGLE_API_KEY"):
                logging.info("Chave API obtida via st.secrets['GOOGLE_API_KEY'].")
                return st.secrets["GOOGLE_API_KEY"]
        except Exception:
            logging.warning("st.secrets não acessível. Caindo para os.environ.")
            pass 
    
    key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
    if key:
        logging.info("Chave API obtida via os.environ.")
    else:
        logging.error("Nenhuma chave API encontrada em st.secrets ou os.environ.")
    return key

# --- Estilos de PDF ---
def get_report_styles():
    """Retorna um dicionário de estilos personalizados para o relatório."""
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name='Base', fontName='Helvetica', fontSize=10, leading=14, textColor=DARK_GREY))
    styles.add(ParagraphStyle(name='RelatorioTitulo', parent=styles['Base'], fontName='Helvetica-Bold', fontSize=24, leading=30, alignment=TA_CENTER, spaceAfter=0.75 * inch, textColor=PRIMARY_BLUE))
    styles.add(ParagraphStyle(name='SecaoPrincipal', parent=styles['Base'], fontName='Helvetica-Bold', fontSize=16, leading=20, spaceBefore=0.3 * inch, spaceAfter=0.1 * inch, textColor=PRIMARY_BLUE))
    styles.add(ParagraphStyle(name='SecaoSub', parent=styles['Base'], fontName='Helvetica-Bold', fontSize=12, leading=16, spaceBefore=0.15 * inch, spaceAfter=0.05 * inch, textColor=DARK_GREY))
    styles.add(ParagraphStyle(name='Corpo', parent=styles['Base'], alignment=TA_JUSTIFY, spaceBefore=6, spaceAfter=6, firstLineIndent=0))
    styles.add(ParagraphStyle(name='SubTituloDestaque', parent=styles['Corpo'], fontName='Helvetica-Bold', textColor=PRIMARY_BLUE, spaceBefore=6, spaceAfter=2))
    styles.add(ParagraphStyle(name='CorpoDestaque', parent=styles['Corpo'], textColor=PRIMARY_BLUE, spaceBefore=2, spaceAfter=6))
    styles.add(ParagraphStyle(name='LinkPequeno', parent=styles['Corpo'], fontSize=8, leading=12, fontName='Helvetica-Oblique', alignment=TA_LEFT, textColor=PRIMARY_BLUE))
    return styles

# --- Cache e Constantes ---
_cache = {}
CACHE_EXPIRATION_SECONDS = 3600 # 1 hora
CACHE_CURRENT_QUOTE_EXPIRATION_SECONDS = 300 # 5 minutos
FUNDAMENTAL_DATA_EXPIRATION_SECONDS = 86400 # 24 horas

# --- Conexão com Banco de Dados (MIGRAÇÃO POSTGRESQL) ---
def conectar_db():
    """Cria e retorna a conexão com o banco de dados PostgreSQL em nuvem."""

    setup_logging()
    
    DB_URL = None
    
    # 1. Tenta obter a URL da variável de ambiente (Render, local, ou qualquer servidor)
    DB_URL = os.environ.get("DATABASE_URL")
    
    # 2. Se não encontrou na variável de ambiente E se estiver no ambiente Streamlit, tenta st.secrets
    if not DB_URL and STREAMLIT_ENV:
        try:
            # Esta seção só deve funcionar se estiver rodando no Streamlit Cloud
            import streamlit as st
            DB_URL = st.secrets["DATABASE_URL"]
        except (KeyError, AttributeError):
            logging.warning("DATABASE_URL não encontrado em st.secrets.")
            pass
            
    if not DB_URL:
        logging.error("FALHA CRÍTICA: Variável DATABASE_URL não foi carregada.")
        return None
    
    
    # -------------------------------------------------------------------
    # 💡 LINHA DE DEBUG CRÍTICA: VERIFICAÇÃO DA STRING ANTES DA CONEXÃO
    # -------------------------------------------------------------------
    try:
        # Redige a senha para segurança nos logs, mas confirma o host e a porta.
        if DB_URL.startswith("postgresql://"):
            partes = DB_URL.split('@')
            host_info = partes[1] if len(partes) > 1 else 'HOST_INFO_MISSING'
            url_debug = f"postgresql://postgres:***SENHA_REDIGIDA***@{host_info}"
        else:
            url_debug = DB_URL # Não é URL, imprime a string inteira (ex: formato DSN)

        logging.info(f"DEBUG: Tentando conectar com URL/DSN: {url_debug}")
    except Exception as e:
        # Se falhar ao redigir, apenas registra que a string existe
        logging.warning(f"DEBUG: Erro ao formatar URL, string existe. Erro: {e}")
    # -------------------------------------------------------------------
    
            
    try:
        conn = psycopg2.connect(DB_URL)
        cursor = conn.cursor()
        logging.info("Conexão com PostgreSQL estabelecida com sucesso.")
        # CORREÇÃO: Nome da tabela mudou de 'ativos' para 'portfolio_assets'
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS portfolio_assets (
            codigo TEXT PRIMARY KEY,
            nome TEXT,
            preco_medio REAL,
            quantidade REAL,
            valor_total REAL,
            tipo TEXT,
            moeda TEXT DEFAULT 'BRL'
        )
        """)
        
        # Adicionar colunas se não existirem (Sintaxe PostgreSQL)
        cursor.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                               WHERE table_name='portfolio_assets' AND column_name='moeda') THEN
                    ALTER TABLE portfolio_assets ADD COLUMN moeda TEXT DEFAULT 'BRL';
                END IF;
            END $$;
        """)

        # Tabela de Cache de Relatório de Carteira
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS cache_relatorio (
            id SERIAL PRIMARY KEY,
            data_geracao TEXT NOT NULL,
            hash_carteira TEXT NOT NULL,
            conteudo_relatorio TEXT NOT NULL
        )
        """)

        # Novas tabelas para dados fundamentalistas (Sintaxe PostgreSQL)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS asset_info (
            ticker TEXT PRIMARY KEY,
            data TEXT,
            timestamp REAL DEFAULT (EXTRACT(EPOCH FROM NOW()))
        )
        """)
        
        cursor.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                               WHERE table_name='asset_info' AND column_name='timestamp') THEN
                    ALTER TABLE asset_info ADD COLUMN timestamp REAL DEFAULT (EXTRACT(EPOCH FROM NOW()));
                END IF;
            END $$;
        """)

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS asset_financials (
            id SERIAL PRIMARY KEY,
            ticker TEXT,
            date TEXT,
            data TEXT,
            timestamp REAL DEFAULT (EXTRACT(EPOCH FROM NOW()))
        )
        """)
        
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS asset_balance_sheet (
            id SERIAL PRIMARY KEY,
            ticker TEXT,
            date TEXT,
            data TEXT,
            timestamp REAL DEFAULT (EXTRACT(EPOCH FROM NOW()))
        )
        """)

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS asset_cash_flow (
            id SERIAL PRIMARY KEY,
            ticker TEXT,
            date TEXT,
            data TEXT,
            timestamp REAL DEFAULT (EXTRACT(EPOCH FROM NOW()))
        )
        """)

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS cache_relatorio_ativo (
            id SERIAL PRIMARY KEY,
            ticker TEXT NOT NULL,
            data_geracao TEXT NOT NULL,
            hash_ativo TEXT NOT NULL,
            conteudo_relatorio TEXT NOT NULL,
            UNIQUE(ticker, data_geracao, hash_ativo)
        )
        """)

        conn.commit()
        return conn

    except Exception as e:
        logging.error(f"Erro ao conectar ao banco de dados PostgreSQL: {e}")
        return None    

# --- Funções CRUD (MIGRAÇÃO POSTGRESQL) ---

def inserir_ativo(ativo_data, atualizar_fundamentos=True):
    """Insere ou atualiza um ativo na tabela 'portfolio_assets' (PostgreSQL)."""
    conn = conectar_db()
    if conn is None:
        return False, "Falha na conexão com o banco de dados."
    
    cursor = conn.cursor()
    try:
        ativo_data['moeda'] = ativo_data.get('moeda', 'BRL')
        
        # CORREÇÃO: Tabela 'portfolio_assets', sintaxe ON CONFLICT e placeholders %s
        cursor.execute("""
        INSERT INTO portfolio_assets (codigo, nome, preco_medio, quantidade, valor_total, tipo, moeda)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (codigo) DO UPDATE SET
            nome = EXCLUDED.nome,
            preco_medio = EXCLUDED.preco_medio,
            quantidade = EXCLUDED.quantidade,
            valor_total = EXCLUDED.valor_total,
            tipo = EXCLUDED.tipo,
            moeda = EXCLUDED.moeda
        """, (
            ativo_data['codigo'], ativo_data['nome'], ativo_data['preco_medio'],
            ativo_data['quantidade'], ativo_data['valor_total'], ativo_data['tipo'], ativo_data['moeda']
        ))

        conn.commit()
        logging.info(f"Ativo inserido/atualizado: {ativo_data['codigo']}")
        
        # !! AQUI ESTÁ A MUDANÇA !!
        # Só atualiza os fundamentos se o parâmetro for True
        if atualizar_fundamentos:
            ticker_yf = ativo_data['codigo']
            if ativo_data['moeda'] == 'BRL' and (ativo_data['tipo'] in ['Ação', 'FII', 'Unit', 'BDR']) and not ticker_yf.endswith('.SA'):
                ticker_yf = f"{ticker_yf}.SA"
            atualizar_dados_fundamentalistas(ticker_yf)
        
        return True, f"Ativo {ativo_data['codigo']} salvo."

    except Exception as e:
        conn.rollback()
        logging.error(f"Erro ao inserir ativo {ativo_data.get('codigo')}: {e}")
        return False, f"Erro no banco de dados: {e}"
    finally:
        if conn:
            conn.close()

# CORREÇÃO: Nome da tabela mudou para 'portfolio_assets'
# OTIMIZAÇÃO: Busca de cotações paralelizada com ThreadPoolExecutor
def visualizar_carteira():
    """Retorna um DataFrame do pandas com todos os ativos da carteira."""
    conn = conectar_db()
    if conn is None:
        return pd.DataFrame(), ["Falha na conexão com o DB"]
        
    try:
        # CORREÇÃO: Querying 'portfolio_assets'
        df = pd.read_sql_query("SELECT codigo, nome, preco_medio, quantidade, valor_total, tipo, moeda FROM portfolio_assets", conn)
        
        if df.empty:
            return pd.DataFrame(), []

        cotacao_dolar = 1.0
        if 'USD' in df['moeda'].values:
            cotacao_dolar = buscar_cotacao_dolar()
            logging.info(f"Cotação do dólar (USD-BRL): {cotacao_dolar}")

        # --- Otimização com Threading ---
        tickers_para_buscar = []
        for index, row in df.iterrows():
            ticker = row['codigo']
            moeda_ativo = row['moeda']
            tipo_ativo = row['tipo']
            
            ticker_yf = ticker
            if moeda_ativo == 'BRL' and (tipo_ativo in ['Ação', 'FII', 'Unit', 'BDR']) and not ticker.endswith('.SA'):
                ticker_yf = f"{ticker}.SA"
            elif moeda_ativo == 'USD' and tipo_ativo == 'Ação' and ticker.endswith('.SA'):
                ticker_yf = ticker.replace('.SA', '')
            tickers_para_buscar.append(ticker_yf)
        
        # Define o número máximo de threads (workers).
        max_workers = min(10, len(tickers_para_buscar) if len(tickers_para_buscar) > 0 else 1)
        
        resultados_cotacoes = []
        logging.info(f"Iniciando busca de cotações em paralelo para {len(tickers_para_buscar)} ativos (Workers: {max_workers})...")
        
        if tickers_para_buscar:
            # Usamos o ThreadPoolExecutor para buscar cotações em paralelo
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                # executor.map mantém a ordem dos resultados igual à ordem de tickers_para_buscar
                resultados_cotacoes = list(executor.map(buscar_cotacao_atual, tickers_para_buscar))
        
        logging.info("Busca de cotações em paralelo concluída.")

        # Atribui os resultados de volta ao DataFrame
        if resultados_cotacoes:
            cotacoes_atuais = [r['price'] for r in resultados_cotacoes]
            variacoes_diarias = [r['daily_change_percent'] for r in resultados_cotacoes]
            cotacoes_encontradas = [r['found'] for r in resultados_cotacoes]
            
            df['cotacao_atual'] = cotacoes_atuais
            df['variacao_diaria_percent'] = variacoes_diarias
            df['cotacao_encontrada'] = cotacoes_encontradas
        else:
            df['cotacao_atual'] = 0.0
            df['variacao_diaria_percent'] = 0.0
            df['cotacao_encontrada'] = False

        # O cálculo de valor_atual_mercado deve usar as colunas vetorizadas
        df['valor_atual_mercado'] = df['cotacao_atual'] * df['quantidade']
        # --- Fim da Otimização ---

        # Otimização: Vetorização dos cálculos de conversão BRL
        df['preco_medio_brl'] = df['preco_medio']
        df['valor_total_brl'] = df['valor_total']
        df['cotacao_atual_brl'] = df['cotacao_atual']
        df['valor_atual_mercado_brl'] = df['valor_atual_mercado']

        indices_usd = df[df['moeda'] == 'USD'].index
        if not indices_usd.empty:
            df.loc[indices_usd, 'preco_medio_brl'] = df.loc[indices_usd, 'preco_medio'] * cotacao_dolar
            df.loc[indices_usd, 'valor_total_brl'] = df.loc[indices_usd, 'valor_total'] * cotacao_dolar
            df.loc[indices_usd, 'cotacao_atual_brl'] = df.loc[indices_usd, 'cotacao_atual'] * cotacao_dolar
            df.loc[indices_usd, 'valor_atual_mercado_brl'] = df.loc[indices_usd, 'valor_atual_mercado'] * cotacao_dolar

        df['lucro_prejuizo'] = df['valor_atual_mercado_brl'] - df['valor_total_brl']
        df['rentabilidade_percent'] = (df['lucro_prejuizo'] / df['valor_total_brl'].replace(0, pd.NA)) * 100
        
        total_portfolio_value_brl = df['valor_atual_mercado_brl'].sum()
        
        if total_portfolio_value_brl > 0:
            df['percent_carteira'] = (df['valor_atual_mercado_brl'] / total_portfolio_value_brl) * 100
        else:
            df['percent_carteira'] = 0.0
            
        df['percent_carteira'].fillna(0, inplace=True)

        # Formatação para exibição
        df_display = df.copy()
        df_display['Preço Médio'] = df.apply(lambda row: f"US$ {row['preco_medio']:.2f}" if row['moeda'] == 'USD' else f"R$ {row['preco_medio']:.2f}", axis=1)
        df_display['Valor Investido'] = df.apply(lambda row: f"US$ {row['valor_total']:.2f}" if row['moeda'] == 'USD' else f"R$ {row['valor_total']:.2f}", axis=1)
        df_display['Cotação Atual'] = df.apply(lambda row: f"US$ {row['cotacao_atual']:.2f}" if row['moeda'] == 'USD' else f"R$ {row['cotacao_atual']:.2f}", axis=1)
        df_display['Valor Atual'] = df.apply(lambda row: f"US$ {row['valor_atual_mercado']:.2f}" if row['moeda'] == 'USD' else f"R$ {row['valor_atual_mercado']:.2f}", axis=1)
        df_display['Lucro (R$)'] = df['lucro_prejuizo'].map(lambda x: f"R$ {x:.2f}")
        df_display['Rentabilidade (%)'] = df['rentabilidade_percent'].map(lambda x: '---' if pd.isna(x) else f"{x:.2f}%")
        df_display['Variação Diária (%)'] = df['variacao_diaria_percent'].map(lambda x: f"{x:.2f}%")
        df_display['% Carteira'] = df['percent_carteira'].map(lambda x: f"{x:.2f}%")

        # Colunas numéricas para cálculos internos no Streamlit
        df_display['valor_total_brl'] = df['valor_total_brl']
        df_display['valor_atual_mercado_brl'] = df['valor_atual_mercado_brl']
        df_display['quantidade_num'] = df['quantidade']
        df_display['preco_medio_brl_num'] = df['preco_medio_brl']
        df_display['cotacao_atual_brl_num'] = df['cotacao_atual_brl']
        df_display['lucro_prejuizo_num'] = df['lucro_prejuizo']
        df_display['rentabilidade_percent_num'] = df['rentabilidade_percent']
        df_display['variacao_diaria_percent_num'] = df['variacao_diaria_percent']
        df_display['percent_carteira_num'] = df['percent_carteira']

        colunas_finais = [
            'codigo', 'nome', 'tipo', 'quantidade', 'moeda',
            'Preço Médio', 'Valor Investido', 'Cotação Atual', 'Valor Atual',
            'Lucro (R$)', 'Rentabilidade (%)', 'Variação Diária (%)', '% Carteira',
            'valor_total_brl', 'valor_atual_mercado_brl', 'quantidade_num',
            'preco_medio_brl_num', 'cotacao_atual_brl_num', 'lucro_prejuizo_num',
            'rentabilidade_percent_num', 'variacao_diaria_percent_num', 'percent_carteira_num'
        ]
        df_display = df_display[colunas_finais]
        
        df_display.rename(columns={
            'codigo': 'Código', 'nome': 'Nome', 'tipo': 'Tipo', 
            'quantidade': 'Quantidade'
        }, inplace=True)

        ativos_sem_cotacao = df[df['cotacao_encontrada'] == False]['codigo'].tolist()

        return df_display, ativos_sem_cotacao
    except Exception as e:
        # O erro "relation 'ativos' does not exist" acontece aqui
        logging.error(f"Erro ao visualizar carteira (PostgreSQL): {e}")
        return pd.DataFrame(), [f"Erro no DB: {e}"]
    finally:
        if conn:
            conn.close()

# CORREÇÃO: Migração para PostgreSQL (%s)
def obter_setor_pais_ativo(ticker):
    """Busca o setor e o país de um ativo no cache de dados (PostgreSQL)."""
    conn = None
    try:
        conn = conectar_db()
        if conn is None:
            return None, None
            
        cursor = conn.cursor()
        
        # CORREÇÃO: Usa placeholder %s
        cursor.execute("SELECT data FROM asset_info WHERE ticker = %s ORDER BY timestamp DESC LIMIT 1", (ticker,))
        
        resultado = cursor.fetchone()
        
        if resultado:
            info_data = json.loads(resultado[0])
            setor = info_data.get('sector')
            pais = info_data.get('country')
            return setor, pais
        
        return None, None
    except (Exception, psycopg2_errors.Error) as e:
        logging.error(f"Erro ao buscar setor/país (PostgreSQL) para {ticker}: {e}")
        return None, None
    finally:
        if conn:
            conn.close()

# ... (Mantenha buscar_cotacao_atual, buscar_cotacao_dolar, buscar_dados_historicos) ...
def buscar_cotacao_atual(ticker):
    """Busca o preço atual e a variação diária de um ativo usando o yfinance com cache."""
    ticker_sanitizado = ticker.replace('.SA.SA', '.SA').replace('.sa.sa', '.sa')
    current_time = time.time()
    cache_key = f"{ticker_sanitizado}_current"
    
    # Verifica o cache interno (in-memory)
    if cache_key in _cache and (current_time - _cache[cache_key]['timestamp'] < CACHE_CURRENT_QUOTE_EXPIRATION_SECONDS):
        return _cache[cache_key]['data']
        
    default_return = {'price': 0.0, 'daily_change_percent': 0.0, 'found': False}
    
    try:
        # logging.info(f"Buscando cotação atual para {ticker_sanitizado}") # Log muito verboso para threads
        ativo = yfinance.Ticker(ticker_sanitizado)
        info = ativo.info
        
        if not info or ('regularMarketPrice' not in info and 'currentPrice' not in info):
            logging.warning(f"Cotação não encontrada para {ticker_sanitizado}.")
            return default_return

        current_quote = {
            'price': info.get('regularMarketPrice') or info.get('currentPrice', 0.0),
            'daily_change_percent': info.get('regularMarketChangePercent', 0.0) or 0.0,
            'found': True
        }
        
        # Salva no cache interno (in-memory)
        _cache[cache_key] = {'data': current_quote, 'timestamp': current_time}
        return current_quote
        
    except Exception as e:
        logging.error(f"Erro ao buscar cotação atual para {ticker_sanitizado}: {e}")
        return default_return

def buscar_cotacao_dolar():
    """Busca a cotação atual do dólar (USD-BRL) usando o yfinance com cache."""
    current_time = time.time()
    cache_key = "USD_BRL_current"
    if cache_key in _cache and (current_time - _cache[cache_key]['timestamp'] < CACHE_CURRENT_QUOTE_EXPIRATION_SECONDS):
        return _cache[cache_key]['data']
    try:
        logging.info("Buscando cotação USD-BRL")
        ticker_dolar = yfinance.Ticker("BRL=X")
        info = ticker_dolar.info
        if not info or ('regularMarketPrice' not in info and 'currentPrice' not in info):
            logging.warning("Não foi possível obter a cotação do dólar. Usando 5.0 como padrão.")
            dolar_quote = 5.0
        else:
            dolar_quote = info.get('regularMarketPrice') or info.get('currentPrice', 5.0)
        _cache[cache_key] = {'data': dolar_quote, 'timestamp': current_time}
        return dolar_quote
    except Exception as e:
        logging.error(f"Erro ao buscar cotação do dólar: {e}. Usando 5.0 como padrão.")
        return 5.0

def buscar_dados_historicos(ticker, periodo_str):
    """Busca dados históricos (Close, Adj Close) com cache e retentativas."""
    ticker_sanitizado = ticker.replace('.SA.SA', '.SA').replace('.sa.sa', '.sa')
    periodo_map = {
        '1 Dia': {'period': '1d', 'interval': '5m'},
        '1 Semana': {'period': '5d', 'interval': '30m'},
        '1 Mês': {'period': '1mo', 'interval': '1d'},
        '12 Meses': {'period': '1y', 'interval': '1d'},
        '2 Anos': {'period': '2y', 'interval': '1wk'},
        '5 Anos': {'period': '5y', 'interval': '1wk'},
        '10 Anos': {'period': '10y', 'interval': '1mo'},
        '20 Anos': {'period': '20y', 'interval': '1mo'},
        'Máximo (MAX)': {'period': 'max', 'interval': '1mo'}
    }
    yf_params = periodo_map.get(periodo_str)
    if not yf_params:
        logging.warning(f"Período '{periodo_str}' inválido.")
        return pd.DataFrame() # Retorna DataFrame vazio
    yf_period = yf_params['period']
    yf_interval = yf_params['interval']
    current_time = time.time()
    cache_key = f"{ticker_sanitizado}_{yf_period}_{yf_interval}"
    if cache_key in _cache and (current_time - _cache[cache_key]['timestamp'] < CACHE_EXPIRATION_SECONDS):
        return _cache[cache_key]['data']
    
    max_retries = 3
    delay = 5
    for retry in range(max_retries):
        try:
            logging.info(f"Buscando dados históricos para {ticker_sanitizado} (Período: {yf_period}, Intervalo: {yf_interval})")
            hist = yfinance.download(
                ticker_sanitizado, 
                period=yf_period,  
                interval=yf_interval,
                progress=False,
                auto_adjust=False
            )
            if not hist.empty:
                data = hist[['Close', 'Adj Close']]
                _cache[cache_key] = {'data': data, 'timestamp': current_time}
                return data
            return pd.DataFrame()
        except Exception as e:
            if 'Rate limited' in str(e) or '404' in str(e):
                logging.warning(f"Rate limit/404 para {ticker_sanitizado}. Tentando novamente em {delay}s... (Tentativa {retry + 1}/{max_retries})")
                time.sleep(delay)
                delay *= 2
            else:
                logging.error(f"Erro inesperado ao buscar dados históricos para {ticker_sanitizado}: {e}")
                return pd.DataFrame()
    logging.error(f"Todas as {max_retries} tentativas falharam para {ticker_sanitizado}.")
    return pd.DataFrame()

# CORREÇÃO: Migração para PostgreSQL (%s) e tabela 'portfolio_assets'
def limpar_carteira():
    """Exclui todos os registros da tabela 'portfolio_assets'."""
    conn = conectar_db()
    if conn is None:
        return False
        
    cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM portfolio_assets")
        conn.commit()
        logging.info("Todos os ativos foram excluídos da carteira.")
        return True
    except (Exception, psycopg2_errors.Error) as e:
        logging.error(f"Erro ao limpar a carteira (PostgreSQL): {e}")
        conn.rollback()
        return False
    finally:
        if conn:
            conn.close()

# CORREÇÃO: Migração para PostgreSQL (%s) e tabela 'portfolio_assets'
def gerar_hash_carteira():
    """Gera um hash SHA256 da carteira atual (PostgreSQL)."""
    conn = None
    ativos_data = []
    try:
        conn = conectar_db()
        if conn is None:
            return ""
        cursor = conn.cursor()
        cursor.execute("""
            SELECT codigo, preco_medio, quantidade, tipo, moeda 
            FROM portfolio_assets 
            ORDER BY codigo
        """)
        ativos_data = cursor.fetchall()
    except (Exception, psycopg2_errors.Error) as e:
        logging.error(f"Erro ao gerar hash da carteira (PostgreSQL): {e}") 
    finally:
        if conn:
            conn.close()
    hash_string = "".join([str(item) for ativo in ativos_data for item in ativo])
    return hashlib.sha256(hash_string.encode('utf-8')).hexdigest()

# CORREÇÃO: Migração para PostgreSQL (%s)
def obter_relatorio_em_cache():
    """Verifica e retorna o relatório em cache (PostgreSQL)."""
    conn = conectar_db()
    if conn is None: return None
    cursor = conn.cursor()
    hash_atual = gerar_hash_carteira()
    data_atual = date.today().strftime('%Y-%m-%d')
    logging.debug(f"Verificando cache da carteira para Data: {data_atual}, Hash: {hash_atual}")
    try:
        cursor.execute("""
            SELECT data_geracao, conteudo_relatorio 
            FROM cache_relatorio
            WHERE data_geracao = %s AND hash_carteira = %s
        """, (data_atual, hash_atual))
        resultado = cursor.fetchone()
        if resultado:
            logging.debug("Relatório da carteira encontrado no cache.")
            return resultado
        logging.debug("Relatório da carteira NÃO encontrado no cache.")
    except (Exception, psycopg2_errors.Error) as e:
        logging.error(f"Erro ao obter cache da carteira (PostgreSQL): {e}")
    finally:
        conn.close()

# CORREÇÃO: Migração para PostgreSQL (%s)
def salvar_relatorio_em_cache(hash_carteira, relatorio_conteudo):
    """Salva o relatório gerado na cache (PostgreSQL)."""
    data_atual = date.today().strftime('%Y-%m-%d')
    conn = conectar_db()
    if conn is None: return
    cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM cache_relatorio")
        cursor.execute("""
            INSERT INTO cache_relatorio (data_geracao, hash_carteira, conteudo_relatorio)
            VALUES (%s, %s, %s)
        """, (data_atual, hash_carteira, relatorio_conteudo))
        conn.commit()
    except (Exception, psycopg2_errors.Error) as e:
        logging.error(f"Erro ao salvar relatório no cache (PostgreSQL): {e}")
        conn.rollback()
    finally:
        conn.close()

def importar_ativos_do_arquivo(file_path):
    """Lê um arquivo CSV e insere os ativos no banco de dados (PostgreSQL/SQLite)."""
    
    tickers_importados_yf = [] # <- NOVO: Lista para guardar os tickers para atualização
    count_sucesso = 0

    try:
        with open(file_path, mode='r', encoding='utf-8') as infile:
            reader = csv.reader(infile, delimiter=';')
            next(reader)  # Pula o cabeçalho
            
            for i, row in enumerate(reader, 1):
                logging.info(f"Processando linha {i}: {row}")
                # ... (todo o código de validação de linha permanece o mesmo) ...

                if len(row) not in [5, 6]:
                    logging.error(f"Erro na linha {i}: Número incorreto de colunas.")
                    continue

                try:
                    codigo, nome, preco_medio_str, quantidade_str, tipo = row[:5]
                    moeda = 'BRL'
                    if len(row) == 6 and row[5]:
                        moeda = row[5]

                    codigo = codigo.strip().upper()
                    nome = nome.strip()
                    tipo = tipo.strip()
                    moeda = moeda.strip().upper()

                    if tipo in ['Acao BR', 'Acao EUA']: tipo = 'Ação'
                    if tipo not in ['Ação', 'FII', 'ETF', 'Unit', 'BDR']:
                        logging.error(f"Erro na linha {i}: Tipo de ativo inválido: '{tipo}'.")
                        continue
                    if moeda in ['REAL']: moeda = 'BRL'
                    if moeda in ['DÓLAR', 'DOLAR']: moeda = 'USD'
                    
                    preco_medio = float(preco_medio_str.replace(',', '.'))
                    quantidade = float(quantidade_str.replace(',', '.'))

                    # ... (código de processamento de 'codigo', 'nome', 'preco_medio', etc.) ...
                    
                    ativo_data = {
                        'codigo': codigo,
                        'nome': nome,
                        'preco_medio': preco_medio,
                        'quantidade': quantidade,
                        'valor_total': preco_medio * quantidade,
                        'tipo': tipo,
                        'moeda': moeda
                    }

                    # !! AQUI ESTÁ A MUDANÇA !!
                    # Passamos 'atualizar_fundamentos=False' para desligar a chamada de API
                    sucesso_insert, _ = inserir_ativo(ativo_data, atualizar_fundamentos=False)
                    
                    if sucesso_insert:
                        count_sucesso += 1
                        
                        # <- NOVO: Adiciona o ticker formatado (com .SA) à lista
                        ticker_yf = codigo
                        if moeda == 'BRL' and (tipo in ['Ação', 'FII', 'Unit', 'BDR']) and not ticker_yf.endswith('.SA'):
                            ticker_yf = f"{ticker_yf}.SA"
                        elif moeda == 'USD' and tipo == 'Ação' and ticker_yf.endswith('.SA'):
                            ticker_yf = ticker_yf.replace('.SA', '')
                        
                        if ticker_yf not in tickers_importados_yf:
                            tickers_importados_yf.append(ticker_yf)
                        
                except (ValueError, TypeError) as e:
                     logging.error(f"Erro de dados na linha {i}: {e}. Linha: {row}")
                except Exception as e:
                     logging.error(f"Erro inesperado na linha {i}: {e}. Linha: {row}")

        logging.info(f"Ativos importados do arquivo: {file_path}. {count_sucesso} ativos salvos.")
        
        # !! AQUI ESTÁ A MUDANÇA !!
        # Retorna a lista de tickers importados junto com a mensagem
        return (True, f"{count_sucesso} ativos importados com sucesso!", tickers_importados_yf)
        
    except FileNotFoundError:
        return (False, f"Erro: O arquivo '{file_path}' não foi encontrado.", [])
    except Exception as e:
        return (False, f"Ocorreu um erro inesperado durante a importação: {e}", [])


# --- FUNÇÃO PARA NOVO RECURSO: GRÁFICO COMPARATIVO ---
@st.cache_data(ttl=datetime.timedelta(hours=12), show_spinner=False)
def obter_precos_historicos_normalizados(tickers: list, periodo="1y"):
    """
    Busca os preços históricos de uma lista de ativos e retorna um DataFrame normalizado.
    Normalização: (Preço Atual / Preço Inicial) -> O dia 1 sempre será 1.0 para todos.
    """
    if not tickers:
        return pd.DataFrame(), "Nenhum ativo fornecido para comparação."

    try:
        # 🔹 Corrige tickers automaticamente
        ativos_para_busca = []
        for t in tickers:
            t = t.strip().upper()
            # Se for ação BR (4 letras + número), adiciona .SA
            if any(ch.isdigit() for ch in t[-2:]):
                ativos_para_busca.append(f"{t}.SA")
            else:
                ativos_para_busca.append(t)

        # 🔹 Baixa os dados do Yahoo Finance
        data = yfinance.download(
            ativos_para_busca,
            period=periodo,
            progress=False,
            auto_adjust=False
        )

        if data.empty or 'Adj Close' not in data.columns:
            return pd.DataFrame(), "Não foi possível obter dados históricos para os ativos selecionados."

        # 🔹 Seleciona preços ajustados
        precos = data['Adj Close'] if isinstance(data.columns, pd.MultiIndex) else data

        # 🔹 Renomeia as colunas para os tickers sem sufixo .SA
        precos.columns = [col.replace('.SA', '') for col in precos.columns]

        # 🔹 Remove linhas totalmente vazias (não remove se faltar só 1 ativo)
        precos = precos.dropna(how="all")

        if precos.empty:
            return pd.DataFrame(), "Não há dados históricos suficientes para gerar o comparativo."

        # 🔹 Normaliza: divide cada série pelo seu primeiro valor
        normalizado = precos / precos.iloc[0]
        normalizado.reset_index(inplace=True)
        normalizado.rename(columns={'Date': 'Data'}, inplace=True)

        return normalizado, ""  # Sucesso

    except Exception as e:
        logging.error(f"Erro ao buscar preços históricos: {e}")
        return pd.DataFrame(), f"Erro interno ao buscar dados históricos: {e}"


# ... (Mantenha gerar_analise_ia_carteira como está, parece OK) ...
def gerar_analise_ia_carteira(carteira_json):
    """Gera a análise da carteira pela IA, usando cache se disponível."""
    relatorio_cache = obter_relatorio_em_cache()
    if relatorio_cache:
        try:
            logging.debug("Relatório da carteira encontrado no cache.")
            return json.loads(relatorio_cache[1]), True
        except json.JSONDecodeError:
            logging.error("Erro ao decodificar JSON do cache. Recalculando.")

    hash_para_salvar = gerar_hash_carteira()
    logging.debug(f"Relatório da carteira não encontrado no cache ou inválido. Gerando nova análise. Hash: {hash_para_salvar}")
    
    api_key = get_gemini_api_key()
    if not api_key:
        return {"erro": "Erro de Configuração: A chave da API Gemini não foi encontrada."}, False
    try:
        client = genai.Client(api_key=api_key) 
    except Exception as e:
        logging.error(f"Erro ao inicializar o cliente Gemini: {e}")
        return {"erro": f"Erro ao inicializar a API: {e}."}, False
    
    system_instruction = (
        "Você é um analista financeiro sênior. Sua tarefa é analisar a carteira de investimentos e "
        "retornar o resultado **EXCLUSIVAMENTE em formato JSON**. "
        "Use suas ferramentas de busca na web para obter contexto de mercado. "
        "NÃO inclua texto fora do bloco de código JSON. NUNCA faça cálculos de somatórios ou percentuais. "
        "O JSON deve seguir a estrutura fornecida no prompt."
    )
    # ... (O prompt permanece o MESMO) ...
    prompt = f"""
    Analise a seguinte carteira de investimentos. 'valor_atual' é a cotação unitária mais recente.

    Dados da Carteira:
    '''json
    {carteira_json}
    '''

    Gere a sua análise **EXCLUSIVAMENTE** no formato JSON a seguir (incluindo as chaves `analise_geral`, `analise_ativos`, `proximos_passos` e a **nova chave `ferramentas_comparacao`**).

    '''json
    {{
        "data_analise": "{datetime.date.today().strftime('%Y-%m-%d')}",
        "analise_geral": {{
            "titulo": "Resumo Geral da Carteira",
            "resumo_qualitativo": "[Avalie a diversificação, risco e rentabilidade aparente (comparando custo vs valor atual geral). Concentre-se no contexto de mercado.]",
            "risco_geral": "[Descreva o risco geral da carteira: Baixo/Moderado/Alto. Justifique.]"
        }},
        "analise_ativos": [
            {{
                "codigo": "HOOD",
                "contexto": "[Análise concisa do ativo e seu contexto de mercado]",
                "acao_sugerida": "Manter/Comprar mais/Reduzir posição/Vender",
                "justificativa": "[Justificativa para a ação sugerida]"
            }}
            // ... (para todos os ativos)
        ],
        "proximos_passos": {{
            "titulo": "Sugestão de Rebalanceamento e Próximos Passos",
            "estrategia_rebalanceamento": "[Sugestão de estratégia geral, se houver necessidade]",
            "lista_passos": [
                "[Primeiro passo recomendado]",
                "[Segundo passo recomendado]",
                "[Terceiro passo recomendado]"
            ],
            "ferramentas_comparacao": {{ 
                "titulo": "Ferramenta de Comparação Rápida",
                "compra_brl": ["(LISTA de 1 a 5 tickers BRL recomendados para COMPRA, ex: 'PETR4')"], 
                "compra_usd": ["(LISTA de 1 a 5 tickers USD recomendados para COMPRA, ex: 'MSFT')"],
                "venda_brl": ["(LISTA de 1 ticker BRL com maior indicação de VENDA, ex: 'OIBR3')"],
                "venda_usd": ["(LISTA de 1 ticker USD com maior indicação de VENDA, ex: 'TSLA')"]  
            }}
        }},
        "noticias_carteira": [
            {{
                "titulo": "[Título da notícia]",
                "data": "[Data da notícia no formato DD-MM-YYYY]",
                "resumo": "[Breve resumo da notícia]",
                "link": "https://www.portugues.com.br/redacao/anoticiaumgenerotextualcunhojornalistico.html"
            }}
            // ... (máximo de 20 notícias relevantes)
        ]
    }}
    '''
    
    // ... (Instruções de Link e Notícias) ...
    
    **ATENÇÃO:** O JSON final deve estar no bloco de código.
    """
    
    logging.info(f"Enviando dados para análise da IA (JSON output)")
    try:
        response = client.models.generate_content(
            model='gemini-2.5-flash',
            contents=[prompt],
            config=types.GenerateContentConfig(
                system_instruction=system_instruction,
                tools=[{"google_search": {}}]
            )
        )
        
        relatorio_json_string = response.text
        if relatorio_json_string is None:
            raise ValueError("A resposta da API Gemini está vazia.")

        match = re.search(r"```json\s*(\{[\s\S]*?\})\s*```", relatorio_json_string)
        json_conteudo = match.group(1).strip() if match else relatorio_json_string
        
        if not isinstance(json_conteudo, str) or not json_conteudo.strip():
             raise ValueError(f"Conteúdo JSON inválido: '{json_conteudo}'")

        analise_data = json.loads(json_conteudo)
        logging.info("Análise da IA recebida e decodificada com sucesso.")
        salvar_relatorio_em_cache(hash_para_salvar, json_conteudo)
        return analise_data, False
    
    except (APIError, Exception) as e:
        logging.error(f"Erro na geração da análise da carteira: {e}")
        return {"erro": f"Erro na API Gemini: {e}"}, False
    except json.JSONDecodeError as e:
        logging.error(f"Erro ao decodificar JSON da IA: {e}\nResposta bruta: {relatorio_json_string}")
        return {"erro": f"A IA retornou um formato inválido. Tente novamente. Detalhe: {e}"}, False

# ... (Mantenha o início da função formatar_analise_para_texto, imports e _gerar_link_de_pesquisa) ...

def _gerar_link_de_pesquisa(titulo, ativo_ou_contexto=""):
    query = f"{titulo} {ativo_ou_contexto} investimento" 
    encoded_query = urllib.parse.quote(query) 
    return f"https://www.google.com/search?q={encoded_query}"

# Função Auxiliar para gerar o markup especial para o PDF (ReportLab)
def _gerar_link_reportlab(estilo, link_texto, url):
    """Gera a marcação customizada para ser interpretada pelo exportar_para_pdf.
       Formato: {{RL_LINK::ESTILO::TEXTO_BASE::URL}}
    """
    logging.info(f"PDF Link Original: Estilo='{estilo}', URL='{url}'")
    return f"{{{{RL_LINK::{estilo}::{link_texto}::{url}}}}}"

# Função auxiliar para re.sub (usada no final da função principal)
# Em portfolio.py (função auxiliar)

def _converter_rl_para_markdown(match):
    """
    Converte a marcação {{RL_LINK::estilo::texto::url}} para a marcação
    HTML que o ReportLab usa para hyperlinks (<a href>).
    """
    estilo = match.group(1) # Estilo (Ex: LinkPequeno)
    texto = match.group(2)  # Texto do Link
    url = match.group(3)    # URL Final

    # CORREÇÃO CRUCIAL: Retorna a marcação HTML para o link.
    # O ReportLab processa isso como um link clicável sem imprimir a URL no texto.
    return f'<a href="{url}">{texto}</a>'

def formatar_analise_para_texto(analise_data):
    """Formata o dicionário de análise da IA em uma string de texto legível."""
    if not isinstance(analise_data, dict):
        return "Erro: Formato de dados de análise inválido."
        
    report_parts = []
    
    is_relatorio_ativo_unico = "analise_quantitativa" in analise_data
    
    # ... (Blocos de código anteriores: Título, Data, analise_geral, analise_quantitativa, analise_ativos) ...
    if is_relatorio_ativo_unico:
        report_parts.append(f"# Relatório de Análise do Ativo: {analise_data.get('ticker', 'N/A')}")
    else:
        report_parts.append("# Relatório de Análise da Carteira de Investimentos")
        
    if analise_data.get("data_analise"):
        report_parts.append(f"Data da Análise: {analise_data['data_analise']}\n")
        
    if "analise_geral" in analise_data:
        geral = analise_data["analise_geral"]
        report_parts.append(f"## {geral.get('titulo', '1. Análise Geral')}")
        report_parts.append(f"{geral.get('resumo_qualitativo', 'N/A')}") 
        report_parts.append(f"* Risco Geral: **{geral.get('risco_geral', 'N/A')}**\n")

    if "analise_quantitativa" in analise_data:
        quant = analise_data["analise_quantitativa"]
        report_parts.append(f"## {quant.get('titulo', '2. Análise Quantitativa')}")
        if "indicadores_chave" in quant:
            for ind in quant["indicadores_chave"]:
                report_parts.append(f"* {ind.get('nome')}: **{ind.get('valor')}** - {ind.get('analise')}")
        if "outros_pontos" in quant:
            report_parts.append(f"\n{quant.get('outros_pontos')}\n")

    ativos_da_carteira = []
    if "analise_ativos" in analise_data and analise_data["analise_ativos"]:
        report_parts.append("## 2. Análise por Ativo")
        for ativo in analise_data["analise_ativos"]:
            report_parts.append(f"### {ativo.get('codigo', 'N/A')}")
            report_parts.append(f"* Contexto: {ativo.get('contexto', 'N/A')}")
            report_parts.append(f"* Ação Sugerida: **{ativo.get('acao_sugerida', 'N/A')}**")
            report_parts.append(f"* Justificativa: {ativo.get('justificativa', 'N/A')}\n")
            ativos_da_carteira.append(ativo.get('codigo', ''))


    # BLOCO 3: Recomendação e Estratégia (com Subseção de Comparação)
    if "proximos_passos" in analise_data:
        passos = analise_data["proximos_passos"]
        report_parts.append(f"## {passos.get('titulo', '3. Próximos Passos')}")
        
        if "estrategia_rebalanceamento" in passos and passos["estrategia_rebalanceamento"] != 'N/A':
            report_parts.append(f"* {passos['estrategia_rebalanceamento']}")
        
        if "acao_sugerida" in passos and passos["acao_sugerida"] != 'N/A':
            report_parts.append(f"* Ação Sugerida: **{passos['acao_sugerida']}**")
        if "justificativa" in passos and passos["justificativa"] != 'N/A':
            report_parts.append(f"* Justificativa: {passos['justificativa']}")

        if "lista_passos" in passos and passos["lista_passos"]:
            for passo in passos["lista_passos"]:
                report_parts.append(f"* {passo}")
                
        # ----------------------------------------------------------------------
        # Unificação da Ferramenta(s) de Comparação
        # ----------------------------------------------------------------------
        
        # 1. Tenta obter o dicionário de ferramentas de dentro de 'proximos_passos' (Ativo Único)
        ferramentas = passos.get("ferramentas_comparacao")
            
        # 3. Processa se a ferramenta foi encontrada (seja no plural ou singular)
        if ferramentas:
            
            # Adiciona um subtítulo para a subseção
            report_parts.append("\n### Ferramenta de Comparação Rápida")
            
            # --- Se for Relatório de Ativo Único, processa o link_gerado ---
            link_markdown = ferramentas.get('link_gerado') 
            if link_markdown:
                    report_parts.append(link_markdown)
                    report_parts.append("") # Linha extra para separação
                
            # --- Processamento dos Links de Compra/Venda (Carteira ou Ativo) ---
            
            links_gerados = False
            
            try:
                # 1. Links de Compra BRL
                compra_brl = [ticker for ticker in ferramentas.get('compra_brl', []) if ticker and ticker.strip()]
                if compra_brl:
                    compra_brl_lista = compra_brl[:5] 
                    link = gerar_link_google_finance(compra_brl_lista, 'BRL')
                    if link:
                        texto = f"Comparação de Compra (BRL): {', '.join(compra_brl)}"
                        report_parts.append(_gerar_link_reportlab("LinkPequeno", texto, link))
                        links_gerados = True

                # 2. Links de Compra USD
                compra_usd = [ticker for ticker in ferramentas.get('compra_usd', []) if ticker and ticker.strip()]
                if compra_usd:
                    compra_usd_lista = compra_usd[:5] 
                    link = gerar_link_google_finance(compra_usd_lista, 'USD')
                    if link:
                        texto = f"Comparação de Compra (USD): {', '.join(compra_usd)}"
                        report_parts.append(_gerar_link_reportlab("LinkPequeno", texto, link))
                        links_gerados = True

                # 3. Links de Venda BRL
                venda_brl = [ticker for ticker in ferramentas.get('venda_brl', []) if ticker and ticker.strip()]
                if venda_brl:
                    link = gerar_link_google_finance(venda_brl, 'BRL') # CHAMADA CORRIGIDA
                    if link:
                        texto = f"Análise de Venda (BRL): {', '.join(venda_brl)}"
                        report_parts.append(_gerar_link_reportlab("LinkPequeno", texto, link))
                        links_gerados = True

                # 4. Links de Venda USD
                venda_usd = [ticker for ticker in ferramentas.get('venda_usd', []) if ticker and ticker.strip()]
                if venda_usd:
                    link = gerar_link_google_finance(venda_usd, 'USD') # CHAMADA CORRIGIDA
                    if link:
                        texto = f"Análise de Venda (USD): {', '.join(venda_usd)}"
                        report_parts.append(_gerar_link_reportlab("LinkPequeno", texto, link))
                        links_gerados = True
                
                if not links_gerados and not link_markdown:
                    report_parts.append("* Nenhuma recomendação específica de comparação foi gerada nesta análise.")
                    
            except Exception as e:
                # Note: 'logging' deve estar definido no portfolio.py
                logging.error(f"Erro ao gerar links de ferramentas de comparação: {e}")
                report_parts.append("* (Erro ao gerar links de comparação)")

        report_parts.append("\n") # Espaço após o bloco de Comparação
    
    # ... (Correção dos Links de Notícias: Já feita para usar o _gerar_link_reportlab)
    if "noticias_carteira" in analise_data and analise_data["noticias_carteira"]:
        contexto_carteira = " ".join(ativos_da_carteira[:3])
        report_parts.append(f"\n## 5. Notícias Relevantes da Carteira")
        for i, noticia in enumerate(analise_data["noticias_carteira"]):
            titulo = noticia.get('titulo', 'Sem Título')
            data_noticia = noticia.get('data', 'Sem Data')
            resumo = noticia.get('resumo', 'Sem resumo.')
            search_link = _gerar_link_de_pesquisa(titulo, contexto_carteira)
            
            report_parts.append(f"### {i+1}. {titulo} ({data_noticia})")
            report_parts.append(f"{resumo}")
            
            # Aplica o estilo LinkPequeno ao link de pesquisa
            texto_link = f"Pesquisar Notícia no Google - Detalhes e Link Original"
            markup_link = _gerar_link_reportlab("LinkPequeno", texto_link, search_link)
            report_parts.append(markup_link) 
            report_parts.append("")

    if "noticias_ativo" in analise_data and analise_data["noticias_ativo"]:
        report_parts.append(f"\n## Notícias Relevantes do Ativo")
        for i, noticia in enumerate(analise_data["noticias_ativo"]):
            titulo = noticia.get('titulo', 'Sem Título')
            data_noticia = noticia.get('data', 'Sem Data')
            resumo = noticia.get('resumo', 'Sem resumo.')
            ticker_ativo = analise_data.get("ticker", "") 
            search_link = _gerar_link_de_pesquisa(titulo, ticker_ativo)
            
            report_parts.append(f"### {i+1}. {titulo} ({data_noticia})")
            report_parts.append(f"{resumo}")
            
            # Aplica o estilo LinkPequeno ao link de pesquisa
            texto_link = f"Pesquisar Notícia no Google - Detalhes e Link Original"
            markup_link = _gerar_link_reportlab("LinkPequeno", texto_link, search_link)
            report_parts.append(markup_link) 
            report_parts.append("")
            
    final_report = "\n".join(report_parts)
    
    # Usa re.sub com re.DOTALL (re.S) para garantir que a URL seja capturada corretamente, mesmo com quebras de linha
    final_report = re.sub(
        # CORREÇÃO: Adicionado '?' após '.+' para torná-lo non-greedy
        r'\{\{RL_LINK::([^:]+)::([^:]+)::(.+?)\}\}', 
        _converter_rl_para_markdown,
        final_report,
        flags=re.DOTALL
    )

    return final_report

# ... (Mantenha gerar_hash_ativo) ...
def gerar_hash_ativo(dados_ativo_dict):
    """Gera um hash SHA256 dos dados de um ativo, excluindo campos dinâmicos."""
    dados_para_hash = dados_ativo_dict.copy()
    dados_para_hash.pop('percent_carteira', None) 
    for key, value in dados_para_hash.items():
        if isinstance(value, float):
            dados_para_hash[key] = f"{value:.8f}"
    hash_string = json.dumps(dados_para_hash, sort_keys=True, separators=(',', ':'), default=str)
    return hashlib.sha256(hash_string.encode('utf-8')).hexdigest()

# CORREÇÃO: Migração para PostgreSQL (%s)
def obter_relatorio_ativo_em_cache(ticker, hash_ativo):
    """Verifica e retorna o relatório em cache para um ativo (PostgreSQL/SQLite)."""
    conn = conectar_db()
    if conn is None: return None
    cursor = conn.cursor()
    is_postgres = hasattr(conn, 'lobject')
    
    data_atual = date.today().strftime('%Y-%m-%d')
    logging.debug(f"Verificando cache do ativo {ticker} para Data: {data_atual}, Hash: {hash_ativo}")
    try:
        sql = "SELECT conteudo_relatorio FROM cache_relatorio_ativo WHERE ticker = %s AND data_geracao = %s AND hash_ativo = %s" if is_postgres else \
              "SELECT conteudo_relatorio FROM cache_relatorio_ativo WHERE ticker = ? AND data_geracao = ? AND hash_ativo = ?"
              
        cursor.execute(sql, (ticker, data_atual, hash_ativo))
        resultado = cursor.fetchone()
        if resultado:
            logging.debug(f"Relatório do ativo {ticker} encontrado no cache.")
            return resultado[0]
        logging.debug(f"Relatório do ativo {ticker} NÃO encontrado no cache.")
    except Exception as e:
        logging.error(f"Erro ao obter cache do ativo: {e}")
    finally:
        conn.close()

# CORREÇÃO: Migração para PostgreSQL (%s)
def salvar_relatorio_ativo_em_cache(ticker, hash_ativo, relatorio_conteudo):
    """Salva o relatório de um ativo no cache (PostgreSQL/SQLite)."""
    data_atual = date.today().strftime('%Y-%m-%d')
    conn = conectar_db()
    if conn is None: return
    cursor = conn.cursor()
    is_postgres = hasattr(conn, 'lobject')
    
    try:
        sql_delete = "DELETE FROM cache_relatorio_ativo WHERE ticker = %s" if is_postgres else \
                     "DELETE FROM cache_relatorio_ativo WHERE ticker = ?"
        cursor.execute(sql_delete, (ticker,))
        
        sql_insert = "INSERT INTO cache_relatorio_ativo (ticker, data_geracao, hash_ativo, conteudo_relatorio) VALUES (%s, %s, %s, %s)" if is_postgres else \
                     "INSERT INTO cache_relatorio_ativo (ticker, data_geracao, hash_ativo, conteudo_relatorio) VALUES (?, ?, ?, ?)"
                     
        cursor.execute(sql_insert, (ticker, data_atual, hash_ativo, relatorio_conteudo))
        conn.commit()
    except Exception as e:
        logging.error(f"Erro ao salvar relatório do ativo no cache: {e}")
        conn.rollback()
    finally:
        if conn:
            conn.close()

# ... (Mantenha gerar_analise_ia_ativo, parece OK) ...
def gerar_analise_ia_ativo(dados_completos_ativo_dict):
    """Gera a análise de um único ativo pela IA."""
    dados_ativo = dados_completos_ativo_dict
    ticker = dados_ativo.get('codigo')
    if not ticker:
        return {"erro": "Dados do ativo incompletos: código não encontrado."}, False

    # Adiciona o ticker ao dict principal para o formatar_analise_para_texto
    dados_ativo['ticker'] = ticker
    
    hash_ativo = gerar_hash_ativo(dados_ativo)
    relatorio_cache = obter_relatorio_ativo_em_cache(ticker, hash_ativo)
    if relatorio_cache:
        try:
            logging.debug(f"Relatório do ativo {ticker} encontrado no cache. Hash: {hash_ativo}")
            return json.loads(relatorio_cache), True
        except json.JSONDecodeError:
            logging.error("Erro ao decodificar JSON do cache de ativo. Recalculando.")

    logging.debug(f"Relatório do ativo {ticker} não encontrado no cache. Gerando nova análise. Hash: {hash_ativo}")
    
    api_key = get_gemini_api_key()
    if not api_key:
        return {"erro": "Erro de Configuração: A chave da API Gemini não foi encontrada."}, False
    try:
        client = genai.Client(api_key=api_key)
    except Exception as e:
        logging.error(f"Erro ao inicializar o cliente Gemini: {e}")
        return {"erro": f"Erro ao inicializar a API: {e}."}, False

    system_instruction = (
        "Você é um analista financeiro sênior. Sua tarefa é realizar uma análise fundamentalista detalhada de um único ativo com base nos dados fornecidos. Retorne o resultado **EXCLUSIVAMENTE em formato JSON**. Use suas ferramentas de busca na web para obter o contexto de mercado atual e notícias recentes sobre o ativo. NÃO inclua texto fora do bloco de código JSON. O JSON deve seguir a estrutura do prompt."
    )
    # ... (O prompt permanece o MESMO) ...
    prompt = f"""
    Analise o seguinte ativo, considerando os dados fundamentalistas, sua posição na carteira e o contexto de mercado atual.

    Dados do Ativo:
    '''json
    {json.dumps(dados_ativo, indent=2, default=str)}
    '''

    Gere sua análise **EXCLUSIVAMENTE** no formato JSON abaixo. Seja detalhado e forneça insights valiosos.

    '''json
    {{
      "data_analise": "{datetime.date.today().strftime('%Y-%m-%d')}",
      "ticker": "{ticker}",
      "analise_geral": {{
        "titulo": "Análise Detalhada do Ativo",
        "resumo_qualitativo": "[Avalie a saúde financeira da empresa...]",
        "risco_geral": "[Descreva o risco associado a este ativo...]"
      }},
      "analise_quantitativa": {{
        "titulo": "Indicadores e Métricas Relevantes",
        "indicadores_chave": [
          {{
            "nome": "P/L (Price/Earnings)",
            "valor": "[Calcule ou obtenha o valor]",
            "analise": "[Interprete o P/L...]"
          }}
          // ... (outros indicadores)
        ],
        "outros_pontos": "[Destaque outros pontos quantitativos...]"
      }},
      "proximos_passos": {{
        "titulo": "Recomendação e Estratégia",
        "acao_sugerida": "Manter/Comprar mais/Reduzir posição/Vender",
        "justificativa": "[Justificativa detalhada...]",
        "ferramentas_comparacao": {{ 
            "titulo": "Ferramenta de Comparação Rápida"
        }}
      }},
      "noticias_ativo": [
        {{
            "titulo": "[Título da notícia]",
            "data": "[Data da notícia no formato DD-MM-YYYY]",
            "resumo": "[Breve resumo da notícia]",
            "link": "https://www.portugues.com.br/redacao/anoticiaumgenerotextualcunhojornalistico.html"
        }} 
        // ... (máximo de 5 notícias)
      ]
    }}
    '''
    
    // ... (Instruções de Link e Notícias) ...
 
    **ATENÇÃO:** O JSON final deve estar no bloco de código. 
    """

    try:
        response = client.models.generate_content(
            model='gemini-2.5-flash',
            contents=[prompt],
            config=types.GenerateContentConfig(
                system_instruction=system_instruction,
                tools=[{"google_search": {}}]
            )
        )
        relatorio_json_string = response.text
        if relatorio_json_string is None:
            raise ValueError("A resposta da API Gemini está vazia.")

        match = re.search(r"```json\s*(\{[\s\S]*?\})\s*```", relatorio_json_string)
        json_conteudo = match.group(1).strip() if match else relatorio_json_string
        
        if not isinstance(json_conteudo, str) or not json_conteudo.strip():
             raise ValueError(f"Conteúdo JSON inválido: '{json_conteudo}'")

        analise_data = json.loads(json_conteudo)
        logging.info("Análise do ativo pela IA recebida e decodificada com sucesso.")
        salvar_relatorio_ativo_em_cache(ticker, hash_ativo, json_conteudo)
        return analise_data, False
    
    except (APIError, Exception) as e:
        logging.error(f"Erro na geração da análise do ativo: {e}")
        return {"erro": f"Erro na API Gemini: {e}"}, False
    except json.JSONDecodeError as e:
        logging.error(f"Erro ao decodificar JSON da IA: {e}\nResposta bruta: {relatorio_json_string}")
        return {"erro": f"A IA retornou um formato inválido. Tente novamente. Detalhe: {e}"}, False

# ... (Mantenha exportar_para_pdf como está) ...
def exportar_para_pdf(caminho_arquivo_ou_buffer, conteudo_texto):
    """Exporta o texto do relatório para um PDF (Aceita buffer em memória)."""
    try:
        doc = SimpleDocTemplate(
            caminho_arquivo_ou_buffer, 
            pagesize=letter,
            leftMargin=0.75*inch, rightMargin=0.75*inch,
            topMargin=0.75*inch, bottomMargin=0.75*inch
        )
        styles = get_report_styles()
        Story = []
        SUBTITULOS_CHAVE = ["Risco Geral", "Ação Sugerida", "Justificativa", "Contexto"]

        for line in conteudo_texto.split('\n'):
            #line = line.replace("link", "a")
            line_strip = line.strip()
            if line_strip == '':
                Story.append(Spacer(1, 0.1 * inch))
                continue
            
            # 2. Trata marcação de Link Customizado (Para LinkPequeno e outros)
            if line_strip.startswith('{{RL_LINK::'):
                # Regex para capturar: {{RL_LINK::ESTILO::TEXTO::URL}}
                # Usa uma expressão não-gananciosa para capturar o conteúdo entre ::
                match = re.match(r'\{\{RL_LINK::([^:]+)::([^:]+)::(.+)\}\}', line_strip)

                if match:
                    estilo_nome = match.group(1)
                    link_texto_base = match.group(2)
                    url = match.group(3)
                    
                    # Remove quebras de linha e o '}}' que o regex não capturou se houver sujeira
                    url = url.strip().rstrip('}')
                    
                    # Cria o markup HTML/XML do ReportLab para links
                    link_markup = f'<a href="{url}">{link_texto_base}</a>'
                    
                    # Adiciona o Parágrafo com o estilo LinkPequeno ou outro customizado
                    if estilo_nome in styles:
                        Story.append(Paragraph(link_markup, styles[estilo_nome]))
                    else:
                        # Fallback para o estilo normal se o nome do estilo for inválido
                        Story.append(Paragraph(link_markup, styles['Corpo']))
                        
                    Story.append(Spacer(1, 0.05 * inch)) 
                    continue            

            
            if line_strip.startswith('<a href="'):   
                Story.append(Paragraph(line_strip, styles['LinkPequeno']))
                Story.append(Spacer(1, 0.05 * inch)) 
                continue            
            if line_strip.startswith('Data da Análise:'):
                Story.append(Paragraph(line_strip, styles['Base'])) 
                Story.append(Spacer(1, 0.1 * inch)) 
                continue 
            if line_strip.startswith('# '):
                Story.append(Paragraph(line_strip[2:], styles['RelatorioTitulo']))
                continue
            elif line_strip.startswith('## '):
                Story.append(Paragraph(line_strip[3:], styles['SecaoPrincipal']))
                continue
            elif line_strip.startswith('### '):
                Story.append(Paragraph(line_strip[4:], styles['SecaoSub']))
                continue
            elif line_strip.startswith('#### '):
                Story.append(Paragraph(f"<b>{line_strip[5:]}</b>", styles['Corpo']))
                continue

            if line_strip.startswith('* '):
                line_sem_asterisco = line_strip[2:]
                partes = line_sem_asterisco.split(':', 1)
                if len(partes) == 2:
                    chave = partes[0].strip()
                    valor = partes[1].strip()
                    if chave in SUBTITULOS_CHAVE:
                        titulo_subsecao = f"{chave}:"
                        Story.append(Paragraph(titulo_subsecao, styles['SubTituloDestaque']))
                        estilo_corpo_subsecao = styles['Corpo']
                        if chave in ["Risco Geral", "Ação Sugerida"]:
                            estilo_corpo_subsecao = styles['CorpoDestaque']
                        valor = valor.replace('**', '') 
                        valor = re.sub(r'\*(.*?)\*', r'<i>\1</i>', valor) 
                        Story.append(Paragraph(valor, estilo_corpo_subsecao))
                        continue 
            
            if line_strip.startswith('* '):
                processed_line = line_strip[2:]
                processed_line = re.sub(r'\*\*(.*?)\*\*', r'<b>\1</b>', processed_line)
                processed_line = re.sub(r'\*(.*?)\*', r'<i>\1</i>', processed_line)
                Story.append(Paragraph(processed_line, styles['Corpo']))
                continue

            processed_line = line
            processed_line = re.sub(r'\*\*(.*?)\*\*', r'<b>\1</b>', processed_line)
            processed_line = re.sub(r'\*(.*?)\*', r'<i>\1</i>', processed_line)
            Story.append(Paragraph(processed_line, styles['Corpo']))
            
        data_hoje = date.today().strftime('%d de %B de %Y')
        Story.append(Spacer(1, 0.5 * inch)) 
        Story.append(Paragraph(f"Gerado em: {data_hoje}", styles['Base']))

        doc.build(Story)
        if not isinstance(caminho_arquivo_ou_buffer, io.BytesIO):
            logging.info(f"Relatório exportado para PDF com sucesso em {caminho_arquivo_ou_buffer}")        
        return True, "Relatório exportado para PDF com sucesso!"

    except Exception as e:
        logging.error(f"Erro ao exportar relatório para PDF: {e}")
        return False, f"Erro ao exportar relatório para PDF: {e}"

# CORREÇÃO: Migração para PostgreSQL (%s) e tabela 'portfolio_assets'
def obter_dados_completos_ativo(codigo_ativo, percent_carteira):
    """Busca todos os dados de um ativo, incluindo dados fundamentalistas (PostgreSQL/SQLite)."""
    conn = conectar_db()
    if conn is None:
        return None
    cursor = conn.cursor()
    is_postgres = hasattr(conn, 'lobject')
    
    try:
        sql_ativo = "SELECT * FROM portfolio_assets WHERE codigo = %s" if is_postgres else "SELECT * FROM ativos WHERE codigo = ?"
        cursor.execute(sql_ativo, (codigo_ativo,))
        ativo_base = cursor.fetchone()
        if not ativo_base:
            return None
        
        colunas_ativos = [description[0] for description in cursor.description]
        dados_completos = dict(zip(colunas_ativos, ativo_base))
        dados_completos['percent_carteira'] = percent_carteira

        sql_info = "SELECT data FROM asset_info WHERE ticker = %s" if is_postgres else "SELECT data FROM asset_info WHERE ticker = ?"
        cursor.execute(sql_info, (codigo_ativo,))
        info_data = cursor.fetchone()
        if info_data:
            dados_completos['info'] = json.loads(info_data[0])

        sql_financials = "SELECT date, data FROM asset_financials WHERE ticker = %s ORDER BY date DESC LIMIT 1" if is_postgres else \
                         "SELECT date, data FROM asset_financials WHERE ticker = ? ORDER BY date DESC LIMIT 1"
        cursor.execute(sql_financials, (codigo_ativo,))
        financials_data = cursor.fetchone()
        if financials_data:
            dados_completos['financials'] = {'date': financials_data[0], 'data': json.loads(financials_data[1])}

        sql_balance = "SELECT date, data FROM asset_balance_sheet WHERE ticker = %s ORDER BY date DESC LIMIT 1" if is_postgres else \
                      "SELECT date, data FROM asset_balance_sheet WHERE ticker = ? ORDER BY date DESC LIMIT 1"
        cursor.execute(sql_balance, (codigo_ativo,))
        balance_sheet_data = cursor.fetchone()
        if balance_sheet_data:
            dados_completos['balance_sheet'] = {'date': balance_sheet_data[0], 'data': json.loads(balance_sheet_data[1])}

        sql_cashflow = "SELECT date, data FROM asset_cash_flow WHERE ticker = %s ORDER BY date DESC LIMIT 1" if is_postgres else \
                       "SELECT date, data FROM asset_cash_flow WHERE ticker = ? ORDER BY date DESC LIMIT 1"
        cursor.execute(sql_cashflow, (codigo_ativo,))
        cash_flow_data = cursor.fetchone()
        if cash_flow_data:
            dados_completos['cash_flow'] = {'date': cash_flow_data[0], 'data': json.loads(cash_flow_data[1])}

        return dados_completos
    except Exception as e:
        logging.error(f"Erro ao obter dados completos para {codigo_ativo}: {e}")
        return None
    finally:
        if conn:
            conn.close()

# CORREÇÃO: Migração para PostgreSQL (%s e ON CONFLICT)
def atualizar_dados_fundamentalistas(ticker):
    """Busca e armazena os dados fundamentalistas de um ativo (PostgreSQL/SQLite)."""
    conn = conectar_db()
    if conn is None:
        return False, "Falha na conexão com o banco de dados."
        
    cursor = conn.cursor()
    is_postgres = hasattr(conn, 'lobject')
    
    try:
        logging.info(f"Atualizando dados fundamentalistas para {ticker}")
        ativo_yf = yfinance.Ticker(ticker)
        current_time = time.time()
        
        # 1. Salvar .info
        try:
            sql_select = "SELECT timestamp FROM asset_info WHERE ticker = %s" if is_postgres else "SELECT timestamp FROM asset_info WHERE ticker = ?"
            cursor.execute(sql_select, (ticker,))
            last_update = cursor.fetchone()
            if not last_update or (current_time - last_update[0] >= FUNDAMENTAL_DATA_EXPIRATION_SECONDS):
                info_data = ativo_yf.info
                if info_data:
                    sql_insert = """
                        INSERT INTO asset_info (ticker, data, timestamp) VALUES (%s, %s, %s)
                        ON CONFLICT (ticker) DO UPDATE SET data = EXCLUDED.data, timestamp = EXCLUDED.timestamp
                    """ if is_postgres else "REPLACE INTO asset_info (ticker, data, timestamp) VALUES (?, ?, ?)"
                    
                    cursor.execute(sql_insert, (ticker, json.dumps(info_data), current_time))
                    logging.info(f"Dados de .info para {ticker} salvos/atualizados.")
            else:
                logging.info(f"Dados de .info para {ticker} estão frescos, pulando.")
        except Exception as e:
            logging.warning(f"Falha ao buscar .info para {ticker}: {e}")
            conn.rollback() 

        # 2. Salvar .financials
        try:
            sql_select = "SELECT timestamp FROM asset_financials WHERE ticker = %s ORDER BY timestamp DESC LIMIT 1" if is_postgres else \
                         "SELECT timestamp FROM asset_financials WHERE ticker = ? ORDER BY timestamp DESC LIMIT 1"
            cursor.execute(sql_select, (ticker,))
            last_update_financials = cursor.fetchone()
            if not last_update_financials or (current_time - last_update_financials[0] >= FUNDAMENTAL_DATA_EXPIRATION_SECONDS):
                financials_df = ativo_yf.financials
                if not financials_df.empty:
                    sql_delete = "DELETE FROM asset_financials WHERE ticker = %s" if is_postgres else "DELETE FROM asset_financials WHERE ticker = ?"
                    cursor.execute(sql_delete, (ticker,))
                    
                    sql_insert = "INSERT INTO asset_financials (ticker, date, data, timestamp) VALUES (%s, %s, %s, %s)" if is_postgres else \
                                 "INSERT INTO asset_financials (ticker, date, data, timestamp) VALUES (?, ?, ?, ?)"
                                 
                    for date_col in financials_df.columns:
                        date_str = date_col.strftime('%Y-%m-%d')
                        data_json = financials_df[date_col].to_json()
                        cursor.execute(sql_insert, (ticker, date_str, data_json, current_time))
                    logging.info(f"Dados de .financials para {ticker} salvos/atualizados.")
            else:
                logging.info(f"Dados de .financials para {ticker} estão frescos, pulando.")
        except Exception as e:
            logging.warning(f"Falha ao buscar .financials para {ticker}: {e}")
            conn.rollback()

        # 3. Salvar .balance_sheet
        try:
            sql_select = "SELECT timestamp FROM asset_balance_sheet WHERE ticker = %s ORDER BY timestamp DESC LIMIT 1" if is_postgres else \
                         "SELECT timestamp FROM asset_balance_sheet WHERE ticker = ? ORDER BY timestamp DESC LIMIT 1"
            cursor.execute(sql_select, (ticker,))
            last_update_balance_sheet = cursor.fetchone()
            if not last_update_balance_sheet or (current_time - last_update_balance_sheet[0] >= FUNDAMENTAL_DATA_EXPIRATION_SECONDS):
                balance_sheet_df = ativo_yf.balance_sheet
                if not balance_sheet_df.empty:
                    sql_delete = "DELETE FROM asset_balance_sheet WHERE ticker = %s" if is_postgres else "DELETE FROM asset_balance_sheet WHERE ticker = ?"
                    cursor.execute(sql_delete, (ticker,))
                    
                    sql_insert = "INSERT INTO asset_balance_sheet (ticker, date, data, timestamp) VALUES (%s, %s, %s, %s)" if is_postgres else \
                                 "INSERT INTO asset_balance_sheet (ticker, date, data, timestamp) VALUES (?, ?, ?, ?)"
                                 
                    for date_col in balance_sheet_df.columns:
                        date_str = date_col.strftime('%Y-%m-%d')
                        data_json = balance_sheet_df[date_col].to_json()
                        cursor.execute(sql_insert, (ticker, date_str, data_json, current_time))
                    logging.info(f"Dados de .balance_sheet para {ticker} salvos/atualizados.")
            else:
                logging.info(f"Dados de .balance_sheet para {ticker} estão frescos, pulando.")
        except Exception as e:
            logging.warning(f"Falha ao buscar .balance_sheet para {ticker}: {e}")
            conn.rollback()

        # 4. Salvar .cashflow
        try:
            sql_select = "SELECT timestamp FROM asset_cash_flow WHERE ticker = %s ORDER BY timestamp DESC LIMIT 1" if is_postgres else \
                         "SELECT timestamp FROM asset_cash_flow WHERE ticker = ? ORDER BY timestamp DESC LIMIT 1"
            cursor.execute(sql_select, (ticker,))
            last_update_cash_flow = cursor.fetchone()
            if not last_update_cash_flow or (current_time - last_update_cash_flow[0] >= FUNDAMENTAL_DATA_EXPIRATION_SECONDS):
                cashflow_df = ativo_yf.cashflow
                if not cashflow_df.empty:
                    sql_delete = "DELETE FROM asset_cash_flow WHERE ticker = %s" if is_postgres else "DELETE FROM asset_cash_flow WHERE ticker = ?"
                    cursor.execute(sql_delete, (ticker,))
                    
                    sql_insert = "INSERT INTO asset_cash_flow (ticker, date, data, timestamp) VALUES (%s, %s, %s, %s)" if is_postgres else \
                                 "INSERT INTO asset_cash_flow (ticker, date, data, timestamp) VALUES (?, ?, ?, ?)"
                                 
                    for date_col in cashflow_df.columns:
                        date_str = date_col.strftime('%Y-%m-%d')
                        data_json = cashflow_df[date_col].to_json()
                        cursor.execute(sql_insert, (ticker, date_str, data_json, current_time))
                    logging.info(f"Dados de .cashflow para {ticker} salvos/atualizados.")
            else:
                logging.info(f"Dados de .cashflow para {ticker} estão frescos, pulando.")
        except Exception as e:
            logging.warning(f"Falha ao buscar .cashflow para {ticker}: {e}")
            conn.rollback()

        conn.commit() 
        return True, f"Atualização de dados fundamentalistas para {ticker} concluída."

    except Exception as e:
        logging.error(f"Erro CRÍTICO ao atualizar dados fundamentalistas para {ticker}: {e}")
        conn.rollback()
        return False, f"Erro crítico ao atualizar dados para {ticker}: {e}"
    finally:
        if conn:
            conn.close()


def atualizar_fundamentos_em_massa(tickers: list):
    """
    Atualiza os dados fundamentalistas para uma lista de tickers em paralelo.
    """
    if not tickers:
        logging.info("Nenhum ticker fornecido para atualização em massa.")
        return True

    # Remove duplicatas
    tickers_unicos = list(set(tickers))
    
    # Define o número máximo de threads (workers).
    max_workers = min(10, len(tickers_unicos))
    
    logging.info(f"Iniciando atualização de fundamentos em paralelo para {len(tickers_unicos)} ativos (Workers: {max_workers})...")
    
    try:
        # Usamos o ThreadPoolExecutor para atualizar em paralelo
        # A função alvo é 'atualizar_dados_fundamentalistas'
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 'list' é usado para forçar a execução e esperar que todos terminem
            # Usamos submit para lidar com possíveis exceções por ticker
            futures = {executor.submit(atualizar_dados_fundamentalistas, ticker): ticker for ticker in tickers_unicos}
            
            for future in concurrent.futures.as_completed(futures):
                ticker = futures[future]
                try:
                    # 'future.result()' vai pegar o retorno ou levantar a exceção
                    future.result()
                except Exception as e:
                    logging.error(f"Falha ao atualizar fundamentos para {ticker} durante a execução em massa: {e}")

        logging.info("Atualização de fundamentos em massa concluída.")
        return True
    except Exception as e:
        logging.error(f"Erro crítico durante a atualização de fundamentos em massa: {e}")
        return False


# CORREÇÃO: Migração para PostgreSQL (%s)
def atualizar_ativo(codigo, novos_dados):
    """Atualiza a quantidade e o preço médio de um ativo (PostgreSQL/SQLite)."""
    conn = conectar_db()
    if conn is None: return False
    cursor = conn.cursor()
    is_postgres = hasattr(conn, 'lobject')
    
    novos_dados['valor_total'] = novos_dados.get('preco_medio', 0) * novos_dados.get('quantidade', 0)
    
    try:
        if is_postgres:
            # Sintaxe PostgreSQL
            set_clauses = []
            values = []
            for key, value in novos_dados.items():
                set_clauses.append(f"{key} = %s")
                values.append(value)
            values.append(codigo)
            
            query = f"UPDATE portfolio_assets SET {', '.join(set_clauses)} WHERE codigo = %s"
            cursor.execute(query, tuple(values))
        else:
            # Sintaxe SQLite
            set_clause = ", ".join([f"{key} = :{key}" for key in novos_dados.keys()])
            query = f"UPDATE ativos SET {set_clause} WHERE codigo = :codigo"
            novos_dados['codigo'] = codigo
            cursor.execute(query, novos_dados)
            
        conn.commit()
        logging.info(f"Ativo atualizado: {codigo}")
        return True
    except Exception as e:
        logging.error(f"Erro ao atualizar ativo {codigo}: {e}")
        conn.rollback()
        return False
    finally:
        if conn:
            conn.close()

# CORREÇÃO: Migração para PostgreSQL (%s)
def excluir_ativo(codigo):
    """Remove um ativo da carteira pelo seu código (PostgreSQL/SQLite)."""
    conn = conectar_db()
    if conn is None: return False
    cursor = conn.cursor()
    is_postgres = hasattr(conn, 'lobject')
    
    try:
        sql = "DELETE FROM portfolio_assets WHERE codigo = %s" if is_postgres else "DELETE FROM ativos WHERE codigo = ?"
        cursor.execute(sql, (codigo,))
        conn.commit()
        logging.info(f"Ativo excluído: {codigo}")
        return True
    except Exception as e:
        logging.error(f"Erro ao excluir ativo {codigo}: {e}")
        conn.rollback()
        return False
    finally:
        if conn:
            conn.close()

# [CÓDIGO NO FINAL DE portfolio.py (ou em uma seção de utilidades)]

# [CÓDIGO EM portfolio.py] - Substitua a função gerar_url_comparacao_google_finance
# Certifique-se de que 'import urllib.parse' está no topo do arquivo.

def gerar_url_comparacao_google_finance_lixooo(ativo_principal: str, bolsa_principal: str, ativos_comparacao: list = None):
    """
    Gera a URL do Google Finance para comparação de ativos.
    SEMPRE inclui o benchmark (IBOV para BVMF, SPY para EUA).

    :param ativo_principal: Ticker principal (e.g., 'OIBR3', 'ORCL')
    :param bolsa_principal: Bolsa do ativo principal (e.g., 'BVMF', 'NYSE')
    :param ativos_comparacao: Lista de tickers ou de "BOLSA:TICKER" para comparação (pode ser vazia).
    :return: URL completa para o Google Finance
    """
    if ativos_comparacao is None:
        ativos_comparacao = []
        
    base_url = "[https://www.google.com/finance/quote/](https://www.google.com/finance/quote/)"
    main_asset = f"{ativo_principal}:{bolsa_principal}"
    
    comparisons = []
    
    # 1. Definir o Benchmark e formatar ativos de comparação
    if bolsa_principal == 'BVMF':
        benchmark = "INDEXBVMF:IBOV"
        bolsa_comparacao = 'BVMF'
        # Adiciona ativos de comparação (formatados)
        for ticker in ativos_comparacao:
            comparisons.append(f"{bolsa_comparacao}:{ticker}")
        
    elif bolsa_principal in ['NYSE', 'NASDAQ', 'NYSEARCA']:
        benchmark = "NYSEARCA:SPY"
        # Adiciona ativos de comparação (mantendo o formato se já existir)
        for item in ativos_comparacao:
            if ":" in item:
                comparisons.append(item)
            else:
                comparisons.append(f"{bolsa_principal}:{item}")
        
    else:
        # Outras bolsas: apenas adiciona os ativos de comparação, sem benchmark
        comparisons.extend(ativos_comparacao)
        benchmark = None

    # 2. Adiciona o Benchmark
    if benchmark:
        # Evita duplicidade se o benchmark for passado acidentalmente na lista de comparação
        if benchmark not in comparisons:
            comparisons.append(benchmark)
    
    # Se não houver comparação (nem mesmo o benchmark)
    if not comparisons:
         return f"{base_url}{main_asset}?hl=pt"

    # 3. Criar a string de comparação e Codificar
    comparison_string_raw = ",".join(comparisons)
    encoded_comparison = urllib.parse.quote(comparison_string_raw)
    
    # 4. Construir a URL final
    final_url = f"{base_url}{main_asset}?hl=pt&comparison={encoded_comparison}"
    
    return final_url

def classificar_ativo_lixooo(ticker):
    """
    Classifica o ativo como BRL ou USD e retorna a bolsa principal (exchange).
    Retorna: (moeda, bolsa)
    """
    # Heurística para BRL: Tickers com 4 letras e 1 ou 2 números (PETR4), ou que parecem FIIs/ETFs BR (HGLG11)
    # Assumimos que a bolsa é a BVMF para a maioria dos ativos brasileiros
    if re.match(r'^[A-Z]{4}\d{1,2}$', ticker) or (ticker.endswith(('11', '12')) and len(ticker) > 4): 
         return 'BRL', 'BVMF'
    
    # Heurística para USD: Tickers curtos (MSFT, ORCL, SPY)
    # Padrão para bolsa americana é NYSE, mas pode ser ajustado
    if ':' in ticker: # Ticker já vem formatado (e.g., NASDAQ:MSFT, INDEXBVMF:IBOV)
        partes = ticker.split(':')
        bolsa = partes[0]
        # Se for um índice brasileiro que já vem formatado
        if 'BVMF' in bolsa:
            return 'BRL', 'INDEXBVMF'
        return 'USD', bolsa
        
    # Padrão para tickers não especificados (EUA)
    return 'USD', 'NYSE'

# [portfolio.py]
# Adicione esta função em qualquer lugar do arquivo (por exemplo, após 'visualizar_carteira')

def obter_dados_para_exportacao():
    """Retorna um DataFrame bruto do pandas com os dados essenciais para exportação."""
    conn = conectar_db()
    if conn is None:
        logging.error("Exportação falhou: Falha na conexão com o DB")
        return pd.DataFrame()
        
    try:
        # Seleciona apenas as colunas necessárias para o formato de importação
        # A ordem das colunas é importante
        df = pd.read_sql_query(
            """SELECT codigo, nome, preco_medio, quantidade, tipo, moeda 
               FROM portfolio_assets 
               ORDER BY codigo""", 
            conn
        )
        return df
    except Exception as e:
        logging.error(f"Erro ao obter dados brutos para exportação (PostgreSQL): {e}")
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()
            

def _get_google_ticker_format(ticker, moeda):
    """
    Identifica o Ticker e a Bolsa para o formato Google Finance.
    Retorna (ticker_limpo, bolsa)
    Ex: ('MSFT', 'NASDAQ') ou ('PETR4', 'BVMF')
    """
    
    if ':' in ticker:
        partes = ticker.split(':')
        bolsa = partes[0]
        ticker_limpo = partes[1]
        return ticker_limpo, bolsa
    else:
        if ticker == "IBOV": return "IBOV", "INDEXBVMF"
        if ticker == "SPY": return "SPY", "NYSEARCA"
        if moeda == 'BRL': return ticker, "BVMF"
        return ticker, bolsa_do_ticker(ticker)
    
def bolsa_do_ticker(ticker: str):
    
    YAHOO_EXCHANGES = {
        "NMS": "NASDAQ",
        "NYQ": "NYSE",
        "ASE": "AMEX",
        "PCX": "ARCA",
        "TOR": "Toronto Stock Exchange",
        "TWO": "Taiwan OTC Exchange",
        "HKG": "Hong Kong Stock Exchange",
        "SAO": "B3 (Brasil)",
        "BVMF": "B3 (Brasil)",
    }
    
    try:
        info = yfinance.Ticker(ticker).info
        codigo = info.get("exchange")
        if not codigo:
            return "Bolsa não encontrada"
        return YAHOO_EXCHANGES.get(codigo, codigo)
    except Exception as e:
        return f"Erro ao buscar dados: {e}"

def gerar_link_google_finance(tickers_lista, moeda):
    """
    Gera um link de comparação do Google Finance com benchmarks (IBOV ou SPY).
    
    Args:
        tickers_lista (list): Lista de tickers (o primeiro é o principal).
        moeda (str): 'BRL' ou 'USD'.
    
    Returns:
        str: A URL completa, ou uma string vazia.
    """
    if not tickers_lista:
        return ""

    # 1. Definir o Benchmark
    benchmark_ticker = "IBOV" if moeda == 'BRL' else "SPY"

    # 2. Formatar o Ativo Principal (Formato: ATIVO:BOLSA)
    try:
        main_ticker, main_bolsa = _get_google_ticker_format(tickers_lista[0], moeda)
        ativo_principal_url = f"{main_ticker}:{main_bolsa}" # Ex: ORCL:NYSE
    except Exception as e:
        logging.error(f"Erro ao formatar ativo principal {tickers_lista[0]}: {e}")
        return ""
    
    # 3. Montar a lista de Comparação (Formato: BOLSA:ATIVO)
    comparacoes_formatada = []
    
    # Lista completa de tickers para comparar
    lista_comp = tickers_lista[1:] # O restante da lista
    lista_comp.append(benchmark_ticker) # Adiciona o benchmark
    
    # Evitar duplicatas
    lista_comp = sorted(list(set(lista_comp))) 

    for ticker in lista_comp:
        # Não comparar o ativo principal com ele mesmo
        if ticker == tickers_lista[0]:
            continue
            
        comp_ticker, comp_bolsa = _get_google_ticker_format(ticker, moeda)
        
        # Formato: BOLSA:ATIVO (Ex: BVMF:PETR4 ou INDEXBVMF:IBOV)
        # O urllib.parse.quote cuidará da codificação de ':' para '%3A'
        comparacoes_formatada.append(f"{comp_bolsa}:{comp_ticker}")

    # 4. Montar a URL
    base_url = f"https://www.google.com/finance/quote/{ativo_principal_url}"
    
    if not comparacoes_formatada:
         return f"{base_url}?hl=pt"

    # 5. Codificar para URL
    # Junta os itens com vírgula (que será codificada para %2C)
    comparison_string = ",".join(comparacoes_formatada)
    
    # Monta os parâmetros
    params = {
        'hl': 'pt',
        'comparison': comparison_string
    }
    
    # Codifica os parâmetros de forma segura
    query_string = urllib.parse.urlencode(params)
    
    return f"{base_url}?{query_string}"
