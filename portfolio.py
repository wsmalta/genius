import hashlib
from datetime import date
import re
import psycopg2 # << NOVO: Driver PostgreSQL
from psycopg2 import sql # Para consultas mais seguras (opcional, mas bom)
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
from google import genai
from google.genai import types
from google.genai.errors import APIError
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_JUSTIFY

# Defina cores para usar consistentemente
PRIMARY_BLUE = '#0D47A1'
DARK_GREY = '#424242'
# Altere as definições de estilo (por volta das linhas onde você usa getSampleStyleSheet())
DATABASE_URL = st.secrets["DATABASE_URL"]
# Tentativa de importar Streamlit e verificar se está no ambiente Cloud
try:
    import streamlit as st
    # Esta variável booleana indica se estamos em um ambiente Streamlit
    STREAMLIT_ENV = True 
except ImportError:
    STREAMLIT_ENV = False

def get_gemini_api_key():
    """Tenta obter a chave API do Streamlit secrets (nuvem) ou de variáveis de ambiente (local)."""
    
    # 1. Tenta buscar no Streamlit Secrets se estiver no ambiente Streamlit
    if STREAMLIT_ENV:
        try:
            # st.secrets funciona como um dicionário
            if st.secrets.get("GEMINI_API_KEY"):
                logging.info("Chave API obtida via st.secrets['GEMINI_API_KEY'].")
                return st.secrets["GEMINI_API_KEY"]
            elif st.secrets.get("GOOGLE_API_KEY"):
                logging.info("Chave API obtida via st.secrets['GOOGLE_API_KEY'].")
                return st.secrets["GOOGLE_API_KEY"]
        except Exception:
            # Caso o secrets.toml esteja malformado ou incompleto
            logging.warning("st.secrets não acessível/incompleto. Caindo para os.environ.")
            pass # Continua para o fallback de os.environ

    # 2. Fallback para variáveis de ambiente (Ambiente local Tkinter ou fallback do Streamlit)
    key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
    if key:
        logging.info("Chave API obtida via os.environ.")
    else:
        logging.error("Nenhuma chave API encontrada em st.secrets ou os.environ.")
    return key

def get_report_styles():
    """Retorna um dicionário de estilos personalizados para o relatório."""
    styles = getSampleStyleSheet()
    
    # ------------------
    # 1. ESTILO BASE
    # ------------------
    # Um estilo base limpo para herança
    styles.add(ParagraphStyle(name='Base', 
                              fontName='Helvetica', 
                              fontSize=10, 
                              leading=14, # Espaço entre linhas (altura da linha)
                              textColor=DARK_GREY))
    
    # ------------------
    # 2. TÍTULO PRINCIPAL (Capa)
    # ------------------
    styles.add(ParagraphStyle(name='RelatorioTitulo',
                              parent=styles['Base'],
                              fontName='Helvetica-Bold',
                              fontSize=24,
                              leading=30,
                              alignment=TA_CENTER,
                              spaceAfter=0.75 * inch, # Maior espaço para o título
                              textColor=PRIMARY_BLUE))

    # ------------------
    # 3. SEÇÃO PRINCIPAL (H1)
    # ------------------
    styles.add(ParagraphStyle(name='SecaoPrincipal',
                              parent=styles['Base'],
                              fontName='Helvetica-Bold',
                              fontSize=16,
                              leading=20,
                              spaceBefore=0.3 * inch, # Espaço antes de cada nova seção
                              spaceAfter=0.1 * inch,
                              textColor=PRIMARY_BLUE)) # Use a cor de destaque

    # ------------------
    # 4. SUBSEÇÃO (H2)
    # ------------------
    styles.add(ParagraphStyle(name='SecaoSub',
                              parent=styles['Base'],
                              fontName='Helvetica-Bold',
                              fontSize=12,
                              leading=16,
                              spaceBefore=0.15 * inch,
                              spaceAfter=0.05 * inch,
                              textColor=DARK_GREY))

    # ------------------
    # 5. CORPO DO TEXTO (Conteúdo da IA)
    # ------------------
    styles.add(ParagraphStyle(name='Corpo',
                              parent=styles['Base'],
                              alignment=TA_JUSTIFY,
                              spaceBefore=6,
                              spaceAfter=6,
                              firstLineIndent=0)) # Evita recuo na primeira linha para blocos de texto
    
    # ------------------
    # 6. SUBTÍTULO DE DESTAQUE (Risco, Ação, etc.) - NOVO
    # ------------------
    # (Tamanho 'Corpo', Negrito, Cor Primária)
    styles.add(ParagraphStyle(name='SubTituloDestaque',
                              parent=styles['Corpo'],      # Herda de 'Corpo' (tamanho 10)
                              fontName='Helvetica-Bold', # Negrito
                              textColor=PRIMARY_BLUE,    # Cor de destaque
                              spaceBefore=6,             # Espaço antes do subtítulo
                              spaceAfter=2))             # Pouco espaço depois (o corpo vem logo abaixo)

    # ------------------
    # 7. CORPO DE TEXTO COM DESTAQUE (Corpo do Risco Geral) - NOVO
    # ------------------
    # (Tamanho 'Corpo', Cor Primária)
    styles.add(ParagraphStyle(name='CorpoDestaque',
                              parent=styles['Corpo'],    # Herda de 'Corpo'
                              textColor=PRIMARY_BLUE,  # Cor de destaque
                              spaceBefore=2,           # Pouco espaço antes
                              spaceAfter=6))           # Espaço normal depois   
    # ------------------
    # 8. ESTILO PARA LINKS PEQUENOS (URL) - NOVO
    # ------------------
    # Fonte menor para URLs, para evitar quebras de linha em PDFs
    styles.add(ParagraphStyle(name='LinkPequeno',
                              parent=styles['Corpo'], # Herda a base (10pt, Helvetica)
                              fontSize=8,             # Fonte menor (ex: 8pt)
                              leading=12,             # Ajusta o espaçamento da linha
                              fontName='Helvetica-Oblique', # Itálico (Helvetica-Oblique)
                              alignment=TA_LEFT,      # Alinhamento à esquerda
                              textColor=PRIMARY_BLUE)) # Cor dos títulos
     
    return styles

# Exemplo de como inicializar e usar:
# ...
# styles = get_report_styles()
# # Substitua: doc = SimpleDocTemplate(filename, pagesize=letter)
# # por: doc = SimpleDocTemplate(filename, pagesize=letter, leftMargin=0.75*inch, rightMargin=0.75*inch) # Margens mais apertadas para visual
# ...


# Adicione esta função em portfolio.py
def obter_setor_pais_ativo(ticker):
    """
    Busca o setor e o país de um ativo no cache de dados fundamentalistas.
    Retorna uma tupla (setor, pais) ou (None, None) se não encontrado.
    """
    conn = None
    try:
        conn = sqlite3.connect('portfolio.db')
        cursor = conn.cursor()

        # Busca a informação mais recente do .info (que contém setor e país)
        cursor.execute("SELECT data FROM asset_info WHERE ticker = %s ORDER BY timestamp DESC LIMIT 1", (ticker,))
        
        resultado = cursor.fetchone()
        
        if resultado:
            # O campo 'data' é um JSON que armazena o resultado do yfinance.info
            info_data = json.loads(resultado[0])
            setor = info_data.get('sector')
            pais = info_data.get('country')
            return setor, pais
        
        return None, None
    except Exception as e:
        logging.error(f"Erro ao buscar setor/país para {ticker}: {e}")
        return None, None
    finally:
        if conn:
            conn.close()

# --- Configuração do Logging ---
def setup_logging():
    """Configura o sistema de logging para registrar eventos em um arquivo."""
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Evita adicionar múltiplos handlers se a função for chamada mais de uma vez
    if not logger.handlers:
        # Handler para arquivo
        file_handler = logging.FileHandler('portfolio.log', mode='a', encoding='utf-8')
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

# --- Cache e Constantes ---
_cache = {}
CACHE_EXPIRATION_SECONDS = 3600 # 1 hora
CACHE_CURRENT_QUOTE_EXPIRATION_SECONDS = 300 # 5 minutos
FUNDAMENTAL_DATA_EXPIRATION_SECONDS = 86400 # 24 horas

def conectar_db():

    setup_logging() # Garante que o logging esteja configurado
    
    """Cria a conexão com o banco de dados PostgreSQL em nuvem e garante as tabelas."""
    try:
        # Conexão via psycopg2 (para PostgreSQL)
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()

        #conn = sqlite3.connect('portfolio.db')
        #cursor = conn.cursor()
        
        
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS ativos (
            codigo TEXT PRIMARY KEY,
            nome TEXT,
            preco_medio REAL,
            quantidade REAL,
            valor_total REAL,
            tipo TEXT,
            moeda TEXT DEFAULT 'BRL'
        )
        """
        )
        # Adicionar a coluna 'moeda' se ela não existir
        cursor.execute("PRAGMA table_info(ativos)")
        columns = [info[1] for info in cursor.fetchall()]
        if 'moeda' not in columns:
            cursor.execute("ALTER TABLE ativos ADD COLUMN moeda TEXT DEFAULT 'BRL'")
            conn.commit()
            logging.info("Coluna 'moeda' adicionada à tabela 'ativos'.")

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS cache_relatorio (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data_geracao TEXT NOT NULL,
            hash_carteira TEXT NOT NULL,
            conteudo_relatorio TEXT NOT NULL
        )
        """
        )

        # Novas tabelas para dados fundamentalistas
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS asset_info (
            ticker TEXT PRIMARY KEY,
            data TEXT,
            timestamp REAL DEFAULT (strftime('%s', 'now')),
            FOREIGN KEY (ticker) REFERENCES ativos(codigo)
        )
        """
        )
        cursor.execute("PRAGMA table_info(asset_info)")
        columns = [info[1] for info in cursor.fetchall()]
        if 'timestamp' not in columns:
            cursor.execute("ALTER TABLE asset_info ADD COLUMN timestamp REAL")
            conn.commit()
            logging.info("Coluna 'timestamp' adicionada à tabela 'asset_info'.")

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS asset_financials (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT,
            date TEXT,
            data TEXT,
            timestamp REAL DEFAULT (strftime('%s', 'now')),
            FOREIGN KEY (ticker) REFERENCES ativos(codigo)
        )
        """
        )
        cursor.execute("PRAGMA table_info(asset_financials)")
        columns = [info[1] for info in cursor.fetchall()]
        if 'timestamp' not in columns:
            cursor.execute("ALTER TABLE asset_financials ADD COLUMN timestamp REAL")
            conn.commit()
            logging.info("Coluna 'timestamp' adicionada à tabela 'asset_financials'.")

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS asset_balance_sheet (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT,
            date TEXT,
            data TEXT,
            timestamp REAL DEFAULT (strftime('%s', 'now')),
            FOREIGN KEY (ticker) REFERENCES ativos(codigo)
        )
        """
        )
        cursor.execute("PRAGMA table_info(asset_balance_sheet)")
        columns = [info[1] for info in cursor.fetchall()]
        if 'timestamp' not in columns:
            cursor.execute("ALTER TABLE asset_balance_sheet ADD COLUMN timestamp REAL")
            conn.commit()
            logging.info("Coluna 'timestamp' adicionada à tabela 'asset_balance_sheet'.")

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS asset_cash_flow (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT,
            date TEXT,
            data TEXT,
            timestamp REAL DEFAULT (strftime('%s', 'now')),
            FOREIGN KEY (ticker) REFERENCES ativos(codigo)
        )
        """
        )
        cursor.execute("PRAGMA table_info(asset_cash_flow)")
        columns = [info[1] for info in cursor.fetchall()]
        if 'timestamp' not in columns:
            cursor.execute("ALTER TABLE asset_cash_flow ADD COLUMN timestamp REAL")
            conn.commit()
            logging.info("Coluna 'timestamp' adicionada à tabela 'asset_cash_flow'.")

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS cache_relatorio_ativo (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            data_geracao TEXT NOT NULL,
            hash_ativo TEXT NOT NULL,
            conteudo_relatorio TEXT NOT NULL,
            UNIQUE(ticker, data_geracao, hash_ativo)
        )
        """
        )

        conn.commit()
        return conn

    except Exception as e:
        logging.error(f"Erro ao conectar ao banco de dados PostgreSQL: {e}")
        # Em caso de falha, retorne None ou trate o erro adequadamente.
        return None    
    


# [portfolio.py] - Função conectar_db()

# ... (Seu bloco STREAMLIT_ENV e get_gemini_api_key deve estar acima) ...


# def conectar_db():
#     """Cria e retorna a conexão com o banco de dados PostgreSQL em nuvem (Supabase)."""
    
#     # 1. Obter URL do Streamlit Secrets
#     if STREAMLIT_ENV:
#         try:
#             DB_URL = st.secrets["DATABASE_URL"]
#         except (KeyError, AttributeError):
#             logging.error("DATABASE_URL não encontrado em st.secrets. Conexão falhou.")
#             return None
#     else:
#         # Fallback para uso local ou desenvolvimento
#         logging.warning("Ambiente local. Conexão com banco de dados PostgreSQL via secrets pulada. Use um banco de dados local ou configure as VAs.")
#         return None # Retorna None se não estiver na nuvem (e falha na inicialização local)
    
#     # 2. Conexão
#     try:
#         # psycopg2.connect pode aceitar o URL completo
#         conn = psycopg2.connect(DB_URL)
#         cursor = conn.cursor()

#         # 3. Criação das Tabelas (Sintaxe PostgreSQL, usando SERIAL para auto-incremento se houver, mas aqui
#         # mantemos o texto como PK)
#         cursor.execute("""
#             CREATE TABLE IF NOT EXISTS portfolio_assets (
#                 ticker TEXT PRIMARY KEY,
#                 peso REAL NOT NULL,
#                 nome_empresa TEXT,
#                 setor TEXT,
#                 cotacao REAL,
#                 timestamp REAL
#             );
#         """)
#         # Nota: Usando 'date' como TEXT para consistência com a lógica SQLite original
#         cursor.execute("""
#             CREATE TABLE IF NOT EXISTS asset_cash_flow (
#                 ticker TEXT,
#                 date TEXT,
#                 data TEXT,
#                 timestamp REAL,
#                 PRIMARY KEY (ticker, date)
#             );
#         """)
#         conn.commit()
#         logging.info("Conexão com PostgreSQL estabelecida e tabelas garantidas.")
#         return conn
    
#     except Exception as e:
#         logging.error(f"Erro CRÍTICO ao conectar/inicializar o banco de dados PostgreSQL: {e}")
#         return None

def inserir_ativo(ativo_data):
    """Insere um novo ativo na tabela."""
    conn = conectar_db()
    cursor = conn.cursor()
    try:
        ativo_data['moeda'] = ativo_data.get('moeda', 'BRL') # Garante que a moeda seja definida, padrão BRL
        cursor.execute("""
        INSERT INTO ativos (codigo, nome, preco_medio, quantidade, valor_total, tipo, moeda)
        VALUES (:codigo, :nome, :preco_medio, :quantidade, :valor_total, :tipo, :moeda)
        """, ativo_data)
        conn.commit()
        logging.info(f"Ativo inserido: {ativo_data['codigo']}")
        
        # Atualiza os dados fundamentalistas após a inserção
        ticker_yf = ativo_data['codigo']
        if ativo_data['moeda'] == 'BRL' and (ativo_data['tipo'] in ['Ação', 'FII', 'Unit', 'BDR']) and not ticker_yf.endswith('.SA'):
            ticker_yf = f"{ticker_yf}.SA"
        atualizar_dados_fundamentalistas(ticker_yf)

    except sqlite3.IntegrityError:
        pass
    finally:
        conn.close()

def visualizar_carteira():
    """Retorna um DataFrame do pandas com todos os ativos da carteira, com conversão de moeda se necessário."""
    conn = conectar_db()
    try:
        df = pd.read_sql_query("SELECT codigo, nome, preco_medio, quantidade, valor_total, tipo, moeda FROM ativos", conn)
        
        if df.empty:
            return pd.DataFrame(), []

        # Buscar cotação do dólar se houver ativos em USD
        cotacao_dolar = 1.0
        if 'USD' in df['moeda'].values:
            cotacao_dolar = buscar_cotacao_dolar()
            logging.info(f"Cotação do dólar (USD-BRL): {cotacao_dolar}")

        # Adicionar colunas para cotação atual e valor atual de mercado
        df['cotacao_atual'] = 0.0
        df['valor_atual_mercado'] = 0.0
        df['variacao_diaria_percent'] = 0.0

        for index, row in df.iterrows():
            ticker = row['codigo']
            moeda_ativo = row['moeda']
            
            # Ajustar ticker para yfinance
            if moeda_ativo == 'BRL' and (row['tipo'] in ['Ação', 'FII', 'Unit', 'BDR']) and not ticker.endswith('.SA'):
                ticker_yf = f"{ticker}.SA"
            elif moeda_ativo == 'USD' and row['tipo'] == 'Ação' and ticker.endswith('.SA'): # Remover .SA para USD
                ticker_yf = ticker.replace('.SA', '')
            else:
                ticker_yf = ticker

            cotacao_info = buscar_cotacao_atual(ticker_yf)
            cotacao = cotacao_info['price']
            variacao_diaria = cotacao_info['daily_change_percent']
            cotacao_encontrada = cotacao_info['found']

            df.loc[index, 'cotacao_atual'] = cotacao
            df.loc[index, 'valor_atual_mercado'] = cotacao * row['quantidade']
            df.loc[index, 'variacao_diaria_percent'] = variacao_diaria
            df.loc[index, 'cotacao_encontrada'] = cotacao_encontrada

            # Converter valores para BRL se a moeda for USD
            if moeda_ativo == 'USD':
                df.loc[index, 'preco_medio_brl'] = row['preco_medio'] * cotacao_dolar
                df.loc[index, 'valor_total_brl'] = row['valor_total'] * cotacao_dolar
                df.loc[index, 'cotacao_atual_brl'] = cotacao * cotacao_dolar
                df.loc[index, 'valor_atual_mercado_brl'] = cotacao * row['quantidade'] * cotacao_dolar
            else:
                df.loc[index, 'preco_medio_brl'] = row['preco_medio']
                df.loc[index, 'valor_total_brl'] = row['valor_total']
                df.loc[index, 'cotacao_atual_brl'] = cotacao
                df.loc[index, 'valor_atual_mercado_brl'] = cotacao * row['quantidade']

        # Calcular lucro/prejuízo e rentabilidade
        df['lucro_prejuizo'] = df['valor_atual_mercado_brl'] - df['valor_total_brl']
        df['rentabilidade_percent'] = (df['lucro_prejuizo'] / df['valor_total_brl']) * 100
        
        # Calculate total portfolio value in BRL
        total_portfolio_value_brl = df['valor_atual_mercado_brl'].sum()
        
        # Calculate percentage of portfolio
        df['percent_carteira'] = (df['valor_atual_mercado_brl'] / total_portfolio_value_brl) * 100
        df['percent_carteira'].fillna(0, inplace=True) # Handle division by zero if total_portfolio_value_brl is 0

        # Formatação para exibição
        df_display = df.copy()
        df_display['Preço Médio'] = df.apply(lambda row: f"US$ {row['preco_medio']:.2f}" if row['moeda'] == 'USD' else f"R$ {row['preco_medio']:.2f}", axis=1)
        df_display['Valor Investido'] = df.apply(lambda row: f"US$ {row['valor_total']:.2f}" if row['moeda'] == 'USD' else f"R$ {row['valor_total']:.2f}", axis=1)
        df_display['Cotação Atual'] = df.apply(lambda row: f"US$ {row['cotacao_atual']:.2f}" if row['moeda'] == 'USD' else f"R$ {row['cotacao_atual']:.2f}", axis=1)
        df_display['Valor Atual'] = df.apply(lambda row: f"US$ {row['valor_atual_mercado']:.2f}" if row['moeda'] == 'USD' else f"R$ {row['valor_atual_mercado']:.2f}", axis=1)
        df_display['Lucro (R$)'] = df['lucro_prejuizo'].map(lambda x: f"R$ {x:.2f}")
        df_display['Rentabilidade (%)'] = df['rentabilidade_percent'].map(lambda x: '---' if pd.isna(x) or x == float('inf') or x == -float('inf') else f"{x:.2f}%")
        df_display['Variação Diária (%)'] = df['variacao_diaria_percent'].map(lambda x: f"{x:.2f}%")
        df_display['% Carteira'] = df['percent_carteira'].map(lambda x: f"{x:.2f}%")

        # Adicionar as colunas numéricas de total em BRL (para uso interno em main.py)
        df_display['valor_total_brl'] = df['valor_total_brl']
        df_display['valor_atual_mercado_brl'] = df['valor_atual_mercado_brl']
        df_display['quantidade_num'] = df['quantidade']
        df_display['preco_medio_brl_num'] = df['preco_medio_brl']
        df_display['cotacao_atual_brl_num'] = df['cotacao_atual_brl']
        df_display['lucro_prejuizo_num'] = df['lucro_prejuizo']
        df_display['rentabilidade_percent_num'] = df['rentabilidade_percent']
        df_display['variacao_diaria_percent_num'] = df['variacao_diaria_percent']
        df_display['percent_carteira_num'] = df['percent_carteira']

        # Selecionar e renomear colunas para exibição
        df_display = df_display[[
            'codigo', 'nome', 'tipo', 'quantidade', 
            'Preço Médio', 'Valor Investido', 
            'Cotação Atual', 'Valor Atual',
            'Lucro (R$)', 'Rentabilidade (%)', 'Variação Diária (%)', '% Carteira',
            'valor_total_brl',
            'valor_atual_mercado_brl',
            'quantidade_num',
            'preco_medio_brl_num',
            'cotacao_atual_brl_num',
            'lucro_prejuizo_num',
            'rentabilidade_percent_num',
            'variacao_diaria_percent_num',
            'percent_carteira_num',
            'moeda'
        ]]
        df_display.columns = [
            'Código', 'Nome', 'Tipo', 'Quantidade', 
            'Preço Médio', 'Valor Investido', 
            'Cotação Atual', 'Valor Atual',
            'Lucro (R$)', 'Rentabilidade (%)', 'Variação Diária (%)', '% Carteira',
            'valor_total_brl',
            'valor_atual_mercado_brl',
            'quantidade_num',
            'preco_medio_brl_num',
            'cotacao_atual_brl_num',
            'lucro_prejuizo_num',
            'rentabilidade_percent_num',
            'variacao_diaria_percent_num',
            'percent_carteira_num',
            'moeda'
        ]

        ativos_sem_cotacao = df[df['cotacao_encontrada'] == False]['codigo'].tolist()

        return df_display, ativos_sem_cotacao
    finally:
        conn.close()

def atualizar_ativo(codigo, novos_dados):
    """Atualiza a quantidade e o preço médio de um ativo."""
    conn = conectar_db()
    cursor = conn.cursor()
    novos_dados['valor_total'] = novos_dados.get('preco_medio', 0) * novos_dados.get('quantidade', 0)
    set_clause = ", ".join([f"{key} = :{key}" for key in novos_dados.keys()])
    query = f"UPDATE ativos SET {set_clause} WHERE codigo = :codigo"
    novos_dados['codigo'] = codigo
    cursor.execute(query, novos_dados)
    conn.commit()
    logging.info(f"Ativo atualizado: {codigo} com dados {novos_dados}")
    conn.close()

def excluir_ativo(codigo):
    """Remove um ativo da carteira pelo seu código."""
    conn = conectar_db()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM ativos WHERE codigo = %s", (codigo,))
    conn.commit()
    logging.info(f"Ativo excluído: {codigo}")
    conn.close()


def adicionar_quantidade_ativo(codigo, quantidade_adicionar, preco_adicionar):
    """Adiciona quantidade a um ativo existente e recalcula o preço médio."""
    conn = conectar_db()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT quantidade, preco_medio, valor_total, moeda FROM ativos WHERE codigo = %s", (codigo,))
        ativo_existente = cursor.fetchone()

        if ativo_existente:
            quantidade_atual, preco_medio_atual, valor_total_atual, moeda_ativo = ativo_existente
            
            nova_quantidade = quantidade_atual + quantidade_adicionar
            novo_valor_total = valor_total_atual + (quantidade_adicionar * preco_adicionar)
            novo_preco_medio = novo_valor_total / nova_quantidade

            cursor.execute("""
                UPDATE ativos
                SET quantidade = %s, preco_medio = %s, valor_total = %s
                WHERE codigo = %s
            """, (nova_quantidade, novo_preco_medio, novo_valor_total, codigo))
            conn.commit()
            logging.info(f"Quantidade {quantidade_adicionar} adicionada ao ativo {codigo}. Novo preço médio: {novo_preco_medio:.2f}")
            return True, "Quantidade adicionada com sucesso."
        else:
            return False, "Ativo não encontrado."
    except Exception as e:
        logging.error(f"Erro ao adicionar quantidade ao ativo {codigo}: {e}")
        return False, f"Erro ao adicionar quantidade: {e}"
    finally:
        conn.close()

def subtrair_quantidade_ativo(codigo, quantidade_subtrair):
    """Subtrai quantidade de um ativo existente."""
    conn = conectar_db()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT quantidade, preco_medio, valor_total, moeda FROM ativos WHERE codigo = %s", (codigo,))
        ativo_existente = cursor.fetchone()

        if ativo_existente:
            quantidade_atual, preco_medio_atual, valor_total_atual, moeda_ativo = ativo_existente

            if quantidade_subtrair > quantidade_atual:
                return False, "Quantidade a subtrair é maior que a quantidade atual."
            
            nova_quantidade = quantidade_atual - quantidade_subtrair
            
            if nova_quantidade == 0:
                # Se a quantidade for zero, remove o ativo
                cursor.execute("DELETE FROM ativos WHERE codigo = %s", (codigo,))
                logging.info(f"Ativo {codigo} removido pois a quantidade chegou a zero.")
            else:
                # Recalcula valor_total e preco_medio (mantendo a proporção do preço médio original)
                # Ou, mais precisamente, remove o custo proporcional da quantidade subtraída
                valor_removido = quantidade_subtrair * preco_medio_atual # Assumindo que a venda é pelo preço médio
                novo_valor_total = valor_total_atual - valor_removido
                # O preço médio não muda se estamos apenas vendendo uma parte da posição pelo preço médio
                # Se a lógica for vender pelo preço de mercado, seria mais complexo.
                # Por simplicidade, mantemos o preco_medio_atual ou recalculamos se o valor_total mudar drasticamente.
                # Para esta operação, o preco_medio_atual permanece o mesmo, apenas a quantidade e valor_total mudam.

                cursor.execute("""
                    UPDATE ativos
                    SET quantidade = %s, valor_total = %s
                    WHERE codigo = %s
                """, (nova_quantidade, novo_valor_total, codigo))
                logging.info(f"Quantidade {quantidade_subtrair} subtraída do ativo {codigo}. Nova quantidade: {nova_quantidade:.2f}")
            
            conn.commit()
            return True, "Quantidade subtraída com sucesso."
        else:
            return False, "Ativo não encontrado."
    except Exception as e:
        logging.error(f"Erro ao subtrair quantidade do ativo {codigo}: {e}")
        return False, f"Erro ao subtrair quantidade: {e}"
    finally:
        conn.close()

def limpar_carteira():
    """Exclui todos os registros da tabela 'ativos'."""
    try:
        conn = conectar_db()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM ativos")
        conn.commit()
        logging.info("Todos os ativos foram excluídos da carteira.")
        conn.close()
        return True
    except Exception as e:
        print(f"Erro ao limpar a carteira: {e}")
        return False

def gerar_hash_carteira():
    """Gera um hash SHA256 da carteira atual (ativos, quantidades e preço médio)."""
    conn = None
    ativos_data = []
    try:
        conn = conectar_db()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT codigo, preco_medio, quantidade, tipo, moeda 
            FROM ativos 
            ORDER BY codigo
        """
        )
        ativos_data = cursor.fetchall()
    except Exception as e:
        print(f"Erro ao gerar hash da carteira: {e}") 
    finally:
        if conn:
            conn.close()
    hash_string = "".join([str(item) for ativo in ativos_data for item in ativo])
    return hashlib.sha256(hash_string.encode('utf-8')).hexdigest()

def obter_relatorio_em_cache():
    """Verifica e retorna o relatório em cache se for do dia e hash atuais."""
    conn = conectar_db()
    cursor = conn.cursor()
    hash_atual = gerar_hash_carteira()
    data_atual = date.today().strftime('%Y-%m-%d')
    logging.debug(f"Verificando cache da carteira para Data: {data_atual}, Hash: {hash_atual}")
    cursor.execute("""
        SELECT data_geracao, conteudo_relatorio 
        FROM cache_relatorio
        WHERE data_geracao = %s AND hash_carteira = %s
    """, (data_atual, hash_atual))
    resultado = cursor.fetchone()
    conn.close()
    if resultado:
        logging.debug("Relatório da carteira encontrado no banco de dados do cache.")
        return resultado
    logging.debug("Relatório da carteira NÃO encontrado no banco de dados do cache.")

def salvar_relatorio_em_cache(hash_carteira, relatorio_conteudo):
    """Salva o relatório gerado na cache, limpando entradas antigas."""
    data_atual = date.today().strftime('%Y-%m-%d')
    conn = conectar_db()
    cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM cache_relatorio")
        cursor.execute("""
            INSERT INTO cache_relatorio (data_geracao, hash_carteira, conteudo_relatorio)
            VALUES (%s, %s, %s)
        """, (data_atual, hash_carteira, relatorio_conteudo))
        conn.commit()
    except Exception as e:
        print(f"Erro ao salvar relatório no cache: {e}")
    finally:
        conn.close()

def importar_ativos_do_arquivo(file_path):
    """Lê um arquivo CSV e insere os ativos no banco de dados."""
    try:
        with open(file_path, mode='r', encoding='utf-8') as infile:
            reader = csv.reader(infile, delimiter=';')
            next(reader)  # Pula o cabeçalho
            for i, row in enumerate(reader, 1):
                logging.info(f"Processando linha {i}: {row}")
                if len(row) not in [5, 6]:
                    logging.error(f"Erro na linha {i}: Número incorreto de colunas.")
                    continue

                codigo, nome, preco_medio_str, quantidade_str, tipo = row[:5]
                moeda = 'BRL'
                if len(row) == 6:
                    moeda = row[5]

                # ADICIONE ESTAS LINHAS PARA LIMPAR OS DADOS:
                codigo = codigo.strip()
                nome = nome.strip()
                tipo = tipo.strip()
                moeda = moeda.strip()
                # Fim da adição

                # Padronizar e validar o tipo de ativo
                if tipo in ['Acao BR', 'Acao EUA']:
                    tipo = 'Ação'
                
                if tipo not in ['Ação', 'FII', 'ETF', 'Unit', 'BDR']:
                    logging.error(f"Erro na linha {i}: Tipo de ativo inválido: '{tipo}'.")
                    continue

                # Padronizar a moeda
                if moeda.upper() == 'REAL':
                    moeda = 'BRL'
                elif moeda.upper() == 'DÓLAR':
                    moeda = 'USD'

                numeric_pattern = re.compile(r"^[+-]?\d*\.?\d+$")

                if not numeric_pattern.match(preco_medio_str.replace(',', '.')):
                    logging.error(f"Erro na linha {i}: 'Preço Médio' inválido: '{preco_medio_str}'.")
                    continue
                if not numeric_pattern.match(quantidade_str.replace(',', '.')):
                    logging.error(f"Erro na linha {i}: 'Quantidade' inválida: '{quantidade_str}'.")
                    continue

                preco_medio = float(preco_medio_str.replace(',', '.'))
                quantidade = float(quantidade_str.replace(',', '.'))
                ativo_data = {
                    'codigo': codigo.upper(),
                    'nome': nome,
                    'preco_medio': preco_medio,
                    'quantidade': quantidade,
                    'valor_total': preco_medio * quantidade,
                    'tipo': tipo,
                    'moeda': moeda.upper()
                }
                inserir_ativo(ativo_data)
        logging.info(f"Ativos importados do arquivo: {file_path}")
        return (True, "Ativos importados com sucesso!")
    except FileNotFoundError:
        return (False, f"Erro: O arquivo '{file_path}' não foi encontrado.")
    except ValueError as e:
        return (False, f"Erro de valor nos dados do CSV: {e}. Verifique o formato do arquivo.")
    except Exception as e:
        return (False, f"Ocorreu um erro inesperado durante a importação: {e}")

def buscar_cotacao_atual(ticker):
    """Busca o preço atual e a variação diária de um ativo usando o yfinance com cache."""
    ticker_sanitizado = ticker.replace('.SA.SA', '.SA').replace('.sa.sa', '.sa')
    current_time = time.time()
    cache_key = f"{ticker_sanitizado}_current"
    if cache_key in _cache and (current_time - _cache[cache_key]['timestamp'] < CACHE_CURRENT_QUOTE_EXPIRATION_SECONDS):
        return _cache[cache_key]['data']
    default_return = {'price': 0.0, 'daily_change_percent': 0.0}
    try:
        logging.info(f"Buscando cotação atual para {ticker_sanitizado}")
        ativo = yfinance.Ticker(ticker_sanitizado)
        info = ativo.info
        if not info or ('regularMarketPrice' not in info and 'currentPrice' not in info):
            logging.warning(f"Cotação não encontrada para {ticker_sanitizado}.")
            return {'price': 0.0, 'daily_change_percent': 0.0, 'found': False}
        current_quote = {
            'price': info.get('regularMarketPrice') or info.get('currentPrice', 0.0),
            'daily_change_percent': info.get('regularMarketChangePercent', 0.0) or 0.0,
            'found': True
        }
        _cache[cache_key] = {'data': current_quote, 'timestamp': current_time}
        return current_quote
    except Exception as e:
        logging.error(f"Erro ao buscar cotação atual para {ticker_sanitizado}: {e}")
        return {'price': 0.0, 'daily_change_percent': 0.0, 'found': False}

def buscar_cotacao_dolar():
    """Busca a cotação atual do dólar (USD-BRL) usando o yfinance com cache."""
    current_time = time.time()
    cache_key = "USD_BRL_current"
    if cache_key in _cache and (current_time - _cache[cache_key]['timestamp'] < CACHE_CURRENT_QUOTE_EXPIRATION_SECONDS):
        return _cache[cache_key]['data']
    
    try:
        logging.info("Buscando cotação USD-BRL")
        # Usar um ticker de ETF ou fundo que replique o dólar ou o próprio par BRL=X
        # BRL=X é o ticker para USD/BRL no Yahoo Finance
        ticker_dolar = yfinance.Ticker("BRL=X")
        info = ticker_dolar.info
        
        if not info or ('regularMarketPrice' not in info and 'currentPrice' not in info):
            logging.warning("Não foi possível obter a cotação do dólar. Usando 5.0 como padrão.")
            dolar_quote = 5.0 # Valor padrão em caso de falha
        else:
            dolar_quote = info.get('regularMarketPrice') or info.get('currentPrice', 5.0)
        
        _cache[cache_key] = {'data': dolar_quote, 'timestamp': current_time}
        return dolar_quote
    except Exception as e:
        logging.error(f"Erro ao buscar cotação do dólar: {e}. Usando 5.0 como padrão.")
        return 5.0 # Retorna um valor padrão em caso de erro

def buscar_dados_historicos(ticker, periodo_str):
    """Busca os dados históricos de fechamento de um ativo para um período selecionado usando yfinance.download com cache e retentativas."""
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
        print(f"Período '{periodo_str}' inválido.")
        return pd.Series()
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
                # Retorna as colunas de Fechamento e Fechamento Ajustado
                data = hist[['Close', 'Adj Close']]
                _cache[cache_key] = {'data': data, 'timestamp': current_time}
                return data
            # Retorna um DataFrame vazio em caso de falha
            return pd.DataFrame()
        except Exception as e:
            if 'Rate limited' in str(e):
                print(f"Rate limit atingido para {ticker_sanitizado}. Tentando novamente em {delay} segundos... (Tentativa {retry + 1}/{max_retries})")
                time.sleep(delay)
                delay *= 2
            else:
                print(f"Erro inesperado ao buscar dados históricos para {ticker_sanitizado}: {e}")
                return pd.Series()
    print(f"Todas as {max_retries} tentativas falharam para {ticker_sanitizado} devido a rate limit.")
    return pd.Series()

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
        # O SDK 0.8.5 geralmente lê automaticamente a chave GEMINI_API_KEY
        client = genai.Client() 
    except Exception as e:
        logging.error(f"Erro ao inicializar o cliente Gemini: {e}")
        return {"erro": "Erro ao inicializar a API. Certifique-se de que a chave 'GEMINI_API_KEY' está configurada corretamente."}, False
    
    system_instruction = (
        "Você é um analista financeiro sênior. Sua tarefa é analisar a carteira de investimentos e "
        "retornar o resultado **EXCLUSIVAMENTE em formato JSON**. "
        "Use suas ferramentas de busca na web para obter contexto de mercado. "
        "NÃO inclua texto fora do bloco de código JSON. NUNCA faça cálculos de somatórios ou percentuais. "
        "O JSON deve seguir a estrutura fornecida no prompt."
    )

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
        }},
        {{
          "codigo": "AAPL",
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
        ]
      }},
      "ferramentas_comparacao": {{
        "titulo": "Ferramentas de Comparação Rápida",
        "link_compra": "[Gere o Link de compra conforme as regras abaixo, substituindo os placeholders pelos ativos e benchmarks apropriados.]",
        "link_venda": "[Gere o Link de venda conforme as regras abaixo, substituindo os placeholders pelos ativos e benchmarks apropriados.]"
      }},
      
      "noticias_carteira": [
        {{
          "titulo": "[Título da notícia]",
          "data": "[Data da notícia no formato DD-MM-YYYY]",
          "resumo": "[Breve resumo da notícia]",
          "link": "[URL completa da notícia]"
        }}
        // ... (máximo de 20 notícias relevantes)
      ]
      
    }}
    '''
    
    // ------------------------------------------------------------------------------------------------------
    // --- INSTRUÇÕES OBRIGATÓRIAS PARA GERAÇÃO DOS LINKS DE COMPARAÇÃO (NO CAMPO 'ferramentas_comparacao') ---
    // ------------------------------------------------------------------------------------------------------
    
    **INSTRUÇÃO OBRIGATÓRIA:** Você DEVE preencher os campos "link_compra" e "link_venda" no JSON (dentro de "ferramentas_comparacao") com URLs COMPLETAS do Google Finance.

    **Regras de Geração do Link:**
    
    1.  **Regras de Mercado:** Você DEVE usar o campo "pais" de cada ativo (fornecido no JSON de entrada) para determinar o formato do link e os benchmarks de comparação:
        * Os ativos '<ATIVO_VENDA_1>' e '<ATIVO_COMPRA_1>' usam o formato `<CÓDIGO>:<BOLSA>`, sendo <BOLSA> a bolsa de valores do ativo.
        * Os ativos seguintes '<ATIVO_VENDA_2>', '<ATIVO_VENDA_3>', '<ATIVO_COMPRA_2>', '<ATIVO_COMPRA_3>', etc., usam o formato `<BOLSA>%3A<CÓDIGO>`.
        * **Se o pais do ativo for 'Brazil': `<BOLSA>` = `BVMF` e o benchmark de comparação `<BENCHMARK>` = `INDEXBVMF%3AIBOV`.
        * **Se o pais do ativo for 'United States': `<BOLSA>` será igual a `NYSE`,`NASDAQ`, etc., e o benchmark de comparação `<BENCHMARK>` = `NYSEARCA%3ASPY`.

    2.  **Geração do Link de Compra (Campo "link_compra"):**
        * Identifique o **Ativo de Maior Recomendação de Compra** (ATIVO_COMPRA_1) caso exista.
        * Gere um link que compare: `ATIVO_COMPRA_1` **vs.** `até 3 outros melhores ativos recomendados para compra se existirem (ATIVO_COMPRA_2, ATIVO_COMPRA_3, ...)` **vs.** `o benchmark do país de ATIVO_COMPRA_1` (INDEXBVMF:IBOV ou NYSEARCA:SPY).
        * No parâmetro de comparação gere caracteres escape para vírgulas (`,`) como `%2C`  e também o caractere de escape para dois pontos (`:`) como `%3A` nos códigos dos ativos.
        * Formato do link: `https://www.google.com/finance/quote/<ATIVO_COMPRA_1>?hl=pt&comparison=<ATIVO_COMPRA_2>%2C<ATIVO_COMPRA_3>%2C<BENCHMARK>`
        * Exemplo de formato (EUA) para ativos TSM e MSFT: `https://www.google.com/finance/quote/TSM:NYSE?hl=pt&comparison=NASDAQ%3AMSFT%2CNYSEARCA%3ASPY`
        * Exemplo de formato (Brasil) para ativos ITUB4 e PETR4: `https://www.google.com/finance/quote/ITUB4:BVMF?hl=pt&comparison=BVMF%3APETR4%2CINDEXBVMF%3AIBOV`
    
    3.  **Geração do Link de Venda (Campo "link_venda"):**
        * Identifique até **Três Ativos de Maior Recomendação de Venda** (ATIVO_VENDA_1, ATIVO_VENDA_2, ...) caso existam.
        * Gere um link que compare: `ATIVO_VENDA_1` **vs.** `até 3 outros ativos de venda` (as maiores recomendações de venda se existirem) **vs.** `o benchmark '<BENCHMARK>' do país de ATIVO_VENDA_1` (INDEXBVMF:IBOV ou NYSEARCA:SPY).
        * Gere caracteres escape para vírgulas (`,`) como `%2C` no parâmetro de comparação e também o caractere de escape para dois pontos (`:`) como `%3A` nos códigos dos ativos.
        * Formato do link: `https://www.google.com/finance/quote/<ATIVO_VENDA_1>?hl=pt&comparison=<ATIVO_VENDA_2>%2C<ATIVO_VENDA_3>%2C<BENCHMARK>`
        * Exemplo de formato (EUA): `https://www.google.com/finance/quote/ORCL:NYSE?hl=pt&comparison=NASDAQ%3AMSFT%2CNYSEARCA:SPY`
        * Exemplo de formato (Brasil): `https://www.google.com/finance/quote/OIBR3:BVMF?hl=pt&comparison=BVMF%3APETR4%2CINDEXBVMF%3AIBOV`
        * Você DEVE usar os códigos de ativos e prefixos de bolsa apropriados em cada link.

    // -----------------------------------------------------------------------------------------
    // --- INSTRUÇÕES OBRIGATÓRIAS PARA AS NOTICIAS ---
    // ----------------------------------------------------------------------------------------- 

        * Para a seção de notícias, você DEVE fornecer um titulo preciso, a data (no formato DD/MM/YYYY) e um resumo informativo. 
        * Você pode deixar o campo link vazio, pois o link de pesquisa será gerado pelo aplicativo."
        
    **ATENÇÃO:** O JSON final deve estar no bloco de código.
    """
    
    # ... (O restante da função permanece inalterado)

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
        # ... (O restante da função permanece inalterado)
        
        relatorio_json_string = response.text

        if relatorio_json_string is None:
            logging.error("A resposta da API Gemini está vazia.")
            return {"erro": "A API Gemini retornou uma resposta vazia. Tente novamente."}, False

        match = re.search(r"```json\s*(\{[\s\S]*?\})\s*```", relatorio_json_string)
        
        if match:
            json_conteudo = match.group(1).strip()
        else:
            json_conteudo = relatorio_json_string
        
        if not isinstance(json_conteudo, str) or not json_conteudo.strip():
            logging.error(f"Conteúdo JSON para decodificação está vazio ou não é uma string válida: '{json_conteudo}'")
            return {"erro": "A IA retornou um conteúdo JSON vazio ou inválido. Tente novamente."}, False

        analise_data = json.loads(json_conteudo)

        logging.info("Análise da IA recebida e decodificada com sucesso.")
        
        salvar_relatorio_em_cache(hash_para_salvar, json_conteudo)
        
        return analise_data, False
    
    except APIError as e:
        logging.error(f"Erro na API Gemini: {e}")
        if "quota exceeded" in str(e).lower() or "resource exhausted" in str(e).lower():
            return {"erro": "Erro na API Gemini: A API está sobrecarregada ou sua cota foi excedida. Tente novamente mais tarde."}, False
        return {"erro": f"Erro durante a chamada à API Gemini: {e}"}, False
    except json.JSONDecodeError as e:
        logging.error(f"Erro ao decodificar JSON da IA: {e}\nResposta bruta: {relatorio_json_string}")
        return {"erro": f"A IA retornou um formato inválido. Tente novamente. Detalhe: {e}"}, False
    except Exception as e:
        logging.error(f"Erro inesperado na geração da análise: {e}")
        return {"erro": f"Ocorreu um erro inesperado durante a geração da análise: {e}"}, False

def formatar_analise_para_texto(analise_data):
    """Formata o dicionário de análise da IA em uma string de texto legível."""
    if not isinstance(analise_data, dict):
        return "Erro: Formato de dados de análise inválido."

# Função auxiliar para gerar um link de pesquisa permanente
    def _gerar_link_de_pesquisa(titulo, ativo_ou_contexto=""):
        """Cria um link de pesquisa do Google com o título da notícia e contexto."""
        # Adiciona o ticker/contexto para refinar a busca
        query = f"{titulo} {ativo_ou_contexto} investimento" 
        # Formata a string para uso seguro na URL
        encoded_query = urllib.parse.quote(query) 
        return f"https://www.google.com/search?q={encoded_query}"
    
    report_parts = []

    report_parts.append("# Relatório de Análise da Carteira de Investimentos")
    if analise_data.get("data_analise"):
        report_parts.append(f"Data da Análise: {analise_data['data_analise']}\n")

    if "analise_geral" in analise_data:
        geral = analise_data["analise_geral"]
        report_parts.append(f"## {geral.get('titulo', '1. Resumo Geral da Carteira')}")
        report_parts.append(f"{geral.get('resumo_qualitativo', 'N/A')}") 
        report_parts.append(f"* Risco Geral: **{geral.get('risco_geral', 'N/A')}**\n")
        
    ativos_da_carteira = []
    
    if "analise_ativos" in analise_data and analise_data["analise_ativos"]:
        report_parts.append("## 2. Análise por Ativo")
        for ativo in analise_data["analise_ativos"]:
            report_parts.append(f"### {ativo.get('codigo', 'N/A')}")
            report_parts.append(f"* Contexto: {ativo.get('contexto', 'N/A')}")
            report_parts.append(f"* Ação Sugerida: **{ativo.get('acao_sugerida', 'N/A')}**")
            report_parts.append(f"* Justificativa: {ativo.get('justificativa', 'N/A')}\n")
            ativos_da_carteira.append(ativo.get('codigo', ''))

    if "proximos_passos" in analise_data:
        passos = analise_data["proximos_passos"]
        report_parts.append(f"## {passos.get('titulo', '3. Sugestão de Rebalanceamento e Próximos Passos')}")
        
        # Check for portfolio-level rebalancing strategy
        if "estrategia_rebalanceamento" in passos and passos["estrategia_rebalanceamento"] != 'N/A':
            report_parts.append(f"* {passos['estrategia_rebalanceamento']}")
        
        # Check for individual asset recommendation
        if "acao_sugerida" in passos and passos["acao_sugerida"] != 'N/A':
            report_parts.append(f"* Ação Sugerida: **{passos['acao_sugerida']}**")
        if "justificativa" in passos and passos["justificativa"] != 'N/A':
            report_parts.append(f"* Justificativa: {passos['justificativa']}")

        if "lista_passos" in passos and passos["lista_passos"]:
            for passo in passos["lista_passos"]:
                report_parts.append(f"* {passo}")

        if "ferramentas_comparacao" in analise_data:
            ferramentas = analise_data["ferramentas_comparacao"]
            report_parts.append(f"## {ferramentas.get('titulo', '4. Ferramentas de Comparação Rápida')}")
            
            link_compra = ferramentas.get('link_compra')
            if link_compra and link_compra.startswith("http"):
                # Usa formatação Markdown para links: [Texto](URL)
                report_parts.append(f"* Comparação dos Tops para Compra:\n{link_compra}")
            
            link_venda = ferramentas.get('link_venda')
            if link_venda and link_venda.startswith("http"):
                report_parts.append(f"* Comparação do Ativo para Venda:\n{link_venda}")
                
        if "ferramenta_comparacao" in analise_data:
            ferramentas = analise_data["ferramenta_comparacao"]
            report_parts.append(f"## {ferramentas.get('titulo', '4. Ferramenta de Comparação Rápida')}")
            
            link_comparacao = ferramentas.get('link_comparacao')
            if link_comparacao and link_comparacao.startswith("http"):
                # Usa formatação Markdown para links: [Texto](URL)
                report_parts.append(f"* Comparação do ativo com o benchmark:\n{link_comparacao}")

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
                # Adiciona o link de pesquisa
                report_parts.append(search_link) 
                report_parts.append("") # Linha vazia para separar visualmente as notícias

        # --- NOVO BLOCO: Notícias do Ativo (Se existir) ---
        if "noticias_ativo" in analise_data and analise_data["noticias_ativo"]:
            # Determina o número da seção (se 'proximos_passos' e 'ferramenta_comparacao' existirem)
            numero_secao = 5 
            report_parts.append(f"\n## {numero_secao}. Notícias Relevantes do Ativo")
            for i, noticia in enumerate(analise_data["noticias_ativo"]):
                titulo = noticia.get('titulo', 'Sem Título')
                data_noticia = noticia.get('data', 'Sem Data')
                resumo = noticia.get('resumo', 'Sem resumo.')
                
                # GERA O LINK DE PESQUISA DO GOOGLE
                # O código do ativo (ticker) deve estar disponível na chamada da função ou no próprio JSON
                # Assumindo que o ticker está em 'analise_data.get("ativo_analisado")' ou 'analise_data.get("ticker")'
                ticker_ativo = analise_data.get("ticker", "") 
                search_link = _gerar_link_de_pesquisa(titulo, ticker_ativo)
                
                report_parts.append(f"### {i+1}. {titulo} ({data_noticia})")
                report_parts.append(f"{resumo}")
                # Adiciona o link de pesquisa
                report_parts.append(search_link) 
                report_parts.append("") # Linha vazia para separar visualmente as notícias


    
    return "\n".join(report_parts)

def gerar_hash_ativo(dados_ativo_dict):
    """Gera um hash SHA256 dos dados de um ativo, excluindo campos dinâmicos como percent_carteira."""
    dados_para_hash = dados_ativo_dict.copy()
    dados_para_hash.pop('percent_carteira', None) # Remover percent_carteira do hash

    # Garantir ordem consistente das chaves e formatação de floats para hashing consistente
    for key, value in dados_para_hash.items():
        if isinstance(value, float):
            dados_para_hash[key] = f"{value:.8f}" # Arredondar para 8 casas decimais

    hash_string = json.dumps(dados_para_hash, sort_keys=True, separators=(',', ':'), default=str)
    
    return hashlib.sha256(hash_string.encode('utf-8')).hexdigest()

def obter_relatorio_ativo_em_cache(ticker, hash_ativo):
    """Verifica e retorna o relatório em cache para um ativo específico."""
    conn = conectar_db()
    cursor = conn.cursor()
    data_atual = date.today().strftime('%Y-%m-%d')
    logging.debug(f"Verificando cache do ativo {ticker} para Data: {data_atual}, Hash: {hash_ativo}")
    cursor.execute("""
        SELECT conteudo_relatorio 
        FROM cache_relatorio_ativo
        WHERE ticker = %s AND data_geracao = %s AND hash_ativo = %s
    """, (ticker, data_atual, hash_ativo))
    resultado = cursor.fetchone()
    conn.close()
    if resultado:
        logging.debug(f"Relatório do ativo {ticker} encontrado no banco de dados do cache.")
        return resultado[0]
    logging.debug(f"Relatório do ativo {ticker} NÃO encontrado no banco de dados do cache.")

def salvar_relatorio_ativo_em_cache(ticker, hash_ativo, relatorio_conteudo):
    """Salva o relatório de um ativo no cache."""
    data_atual = date.today().strftime('%Y-%m-%d')
    conn = conectar_db()
    cursor = conn.cursor()
    try:
        # Limpa o cache antigo para este ticker
        cursor.execute("DELETE FROM cache_relatorio_ativo WHERE ticker = %s", (ticker,))
        # Insere o novo relatório
        cursor.execute("""
            INSERT INTO cache_relatorio_ativo (ticker, data_geracao, hash_ativo, conteudo_relatorio)
            VALUES (%s, %s, %s, %s)
        """, (ticker, data_atual, hash_ativo, relatorio_conteudo))
        conn.commit()
    except Exception as e:
        print(f"Erro ao salvar relatório do ativo no cache: {e}")
    finally:
        conn.close()

def gerar_analise_ia_ativo(dados_completos_ativo_dict):
    """Gera a análise de um único ativo pela IA."""
    dados_ativo = dados_completos_ativo_dict
    ticker = dados_ativo.get('codigo')
    if not ticker:
        return {"erro": "Dados do ativo incompletos: código não encontrado."}, False

    hash_ativo = gerar_hash_ativo(dados_ativo)
    relatorio_cache = obter_relatorio_ativo_em_cache(ticker, hash_ativo)
    if relatorio_cache:
        try:
            logging.debug(f"Relatório do ativo {ticker} encontrado no cache. Hash: {hash_ativo}")
            return json.loads(relatorio_cache), True
        except json.JSONDecodeError:
            logging.error("Erro ao decodificar JSON do cache de ativo. Recalculando.")

    logging.debug(f"Relatório do ativo {ticker} não encontrado no cache ou inválido. Gerando nova análise. Hash: {hash_ativo}")
    api_key = get_gemini_api_key()
    if not api_key:
        return {"erro": "Erro de Configuração: A chave da API Gemini não foi encontrada."}, False

    try:
        client = genai.Client()
    except Exception as e:
        logging.error(f"Erro ao inicializar o cliente Gemini: {e}")
        return {"erro": "Erro ao inicializar a API. Certifique-se de que a chave 'GEMINI_API_KEY' está configurada corretamente."}, False

    system_instruction = (
        "Você é um analista financeiro sênior. Sua tarefa é realizar uma análise fundamentalista detalhada de um único ativo com base nos dados fornecidos. Retorne o resultado **EXCLUSIVAMENTE em formato JSON**. Use suas ferramentas de busca na web para obter o contexto de mercado atual e notícias recentes sobre o ativo. NÃO inclua texto fora do bloco de código JSON. O JSON deve seguir a estrutura do prompt."
    )

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
      "analise_geral": {{
        "titulo": "Análise Detalhada do Ativo",
        "resumo_qualitativo": "[Avalie a saúde financeira da empresa, seus pontos fortes e fracos com base nos dados fundamentalistas. Compare com o setor e o mercado em general.]",
        "risco_geral": "[Descreva o risco associado a este ativo: Baixo/Moderado/Alto. Justifique com base em volatilidade, endividamento, etc.]"
      }},
      "analise_quantitativa": {{
        "titulo": "Indicadores e Métricas Relevantes",
        "indicadores_chave": [
          {{
            "nome": "P/L (Price/Earnings)",
            "valor": "[Calcule ou obtenha o valor]",
            "analise": "[Interprete o P/L no contexto do setor e histórico.]"
          }},
          {{
            "nome": "ROE (Return on Equity)",
            "valor": "[Calcule ou obtenha o valor]",
            "analise": "[Interprete o ROE, indicando a eficiência da empresa em gerar lucro.]"
          }},
          {{
            "nome": "Dívida Líquida/EBITDA",
            "valor": "[Calcule ou obtenha o valor]",
            "analise": "[Interprete o nível de endividamento e sua sustentabilidade.]"
          }}
        ],
        "outros_pontos": "[Destaque outros pontos quantitativos importantes dos demonstrativos financeiros.]"
      }},
      "proximos_passos": {{
        "titulo": "Recomendação e Estratégia",
        "acao_sugerida": "Manter/Comprar mais/Reduzir posição/Vender",
        "justificativa": "[Justificativa detalhada para a ação sugerida, considerando o perfil do investidor (se conhecido), o peso do ativo na carteira e os objetivos de longo prazo.]"
      }},
      "ferramenta_comparacao": {{
        "titulo": "Ferramenta de Comparação Rápida",
        "link_comparacao": "[Gere o link conforme as regras de País (Brasil: IBOV/CDI; EUA: SPY/DJI)]"
      }},
      "noticias_ativo": [
        {{
            "titulo": "[Título da notícia]",
            "data": "[Data da notícia no formato DD-MM-YYYY]",
            "resumo": "[Breve resumo da notícia]",
            "link": "[URL completo da notícia]"
        }} 
        // ... (máximo de 5 notícias relevantes 
      ]
    }}
    '''
    
    // -----------------------------------------------------------------------------------------
    // --- INSTRUÇÕES OBRIGATÓRIAS PARA GERAÇÃO DO LINK DE COMPARAÇÃO (NO CAMPO 'ferramenta_comparacao') ---
    // -----------------------------------------------------------------------------------------
    
    **INSTRUÇÃO OBRIGATÓRIA:** Você DEVE preencher o campo "link_comparacao" no JSON (dentro de "ferramenta_comparacao") com uma URL COMPLETA do Google Finance para que o ativo seja comparado com os benchmarks apropriados do seu país.

    **Regras de Geração do Link:**
    
    1.  **Se o campo "pais" do ativo for "Brazil":**
        * O link principal deve usar o seguinte formato de ativo/benchmark: `<ticker:bolsa>` onde ticker é o codigo do ativo/benchmark (ITUB4, IBOV, etc.) e bolsa é o codigo da bolsa (BVMF, INDEXBVMF, etc).
        * O benchmark de comparação deve ser: `INDEXBVMF%3AIBOV` (Ibovespa).
        * O formato final do link DEVE ser como esse: `https://www.google.com/finance/quote/ITUB4:BVMF?hl=pt&comparison=INDEXBVMF%3AIBOV`
    
    2.  **Considere o ativo IBM como exemplo. Se o campo "pais" do ativo for "United States":**
        * O link principal deve usar o formato: `IBM:NYSE`.
        * O benchmark de comparação deve ser: `NYSEARCA%3ASPY` (S&P 500 ETF).
        * O formato final do link DEVE ser como esse: `https://www.google.com/finance/quote/IBM:NYSE?hl=pt&comparison=NYSEARCA%3ASPY`
        * Você DEVE usar os códigos de ativos e prefixos de bolsa apropriados em cada link.
    
    // -----------------------------------------------------------------------------------------
    // --- INSTRUÇÕES OBRIGATÓRIAS PARA AS NOTICIAS ---
    // -----------------------------------------------------------------------------------------    
        * Para a seção de notícias, você DEVE fornecer um titulo preciso, a data (no formato DD/MM/YYYY) e um resumo informativo. 
        * Você pode deixar o campo link vazio, pois o link de pesquisa será gerado pelo aplicativo."
 
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
            logging.error("A resposta da API Gemini está vazia.")
            return {"erro": "A API Gemini retornou uma resposta vazia. Tente novamente."}, False

        match = re.search(r"```json\s*(\{[\s\S]*?\})\s*```", relatorio_json_string)
        
        if match:
            json_conteudo = match.group(1).strip()
        else:
            json_conteudo = relatorio_json_string
        
        if not isinstance(json_conteudo, str) or not json_conteudo.strip():
            logging.error(f"Conteúdo JSON para decodificação está vazio ou não é uma string válida: '{json_conteudo}'")
            return {"erro": "A IA retornou um conteúdo JSON vazio ou inválido. Tente novamente."}, False

        analise_data = json.loads(json_conteudo)

        logging.info("Análise do ativo pela IA recebida e decodificada com sucesso.")
        salvar_relatorio_ativo_em_cache(ticker, hash_ativo, json_conteudo)
        return analise_data, False
    
    except APIError as e:
        logging.error(f"Erro na API Gemini: {e}")
        if "quota exceeded" in str(e).lower() or "resource exhausted" in str(e).lower():
            return {"erro": "Erro na API Gemini: A API está sobrecarregada ou sua cota foi excedida. Tente novamente mais tarde."}, False
        return {"erro": f"Erro durante a chamada à API Gemini: {e}"}, False
    except json.JSONDecodeError as e:
        logging.error(f"Erro ao decodificar JSON da IA: {e}\nResposta bruta: {relatorio_json_string}")
        return {"erro": f"A IA retornou um formato inválido. Tente novamente. Detalhe: {e}"}, False
    except Exception as e:
        logging.error(f"Erro inesperado na geração da análise: {e}")
        return {"erro": f"Ocorreu um erro inesperado: {e}"}, False

def exportar_para_pdf(caminho_arquivo, conteudo_texto):
    """
    Exporta o texto do relatório para um arquivo PDF usando os estilos personalizados.
    """
    
    try:
        # 1. Configurar o documento com margens
        doc = SimpleDocTemplate(
            caminho_arquivo, 
            pagesize=letter,
            leftMargin=0.75*inch, 
            rightMargin=0.75*inch,
            topMargin=0.75*inch,
            bottomMargin=0.75*inch
        )

        # 2. Obter os estilos personalizados
        styles = get_report_styles()
        Story = []

        # 3. Lista de palavras-chave que queremos tratar como subtítulos
        SUBTITULOS_CHAVE = [
            "Risco Geral", 
            "Ação Sugerida", 
            "Justificativa",
            "Contexto"
        ]

        # 4. Iterar pelo conteúdo e aplicar os estilos
        for line in conteudo_texto.split('\n'):
            
            line_strip = line.strip()
            
            # --- 1. Linhas vazias ---
            if line_strip == '':
                Story.append(Spacer(1, 0.1 * inch))
                continue
            
            # --- 2. Processar 'Data da Análise:' (simplesmente adiciona, sem 'Gerado em:') ---
            if line_strip.startswith('Data da Análise:'):
                # Adiciona a linha "Data da Análise"
                Story.append(Paragraph(line_strip, styles['Base'])) 
                # Adiciona um espaço
                Story.append(Spacer(1, 0.1 * inch)) 
                continue 

            # --- 3. Títulos Principais (H1, H2, H3, H4) ---
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

            # --- 4. Lógica para Subtítulos Especiais (Risco, Ação, etc.) ---
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
                        '''
                        if chave == "Risco Geral":
                            estilo_corpo_subsecao = styles['CorpoDestaque']
                        '''
                        if chave in ["Risco Geral", "Ação Sugerida"]:
                            estilo_corpo_subsecao = styles['CorpoDestaque']
                        
                        valor = valor.replace('**', '') 
                        valor = re.sub(r'\*(.*%s)\*', r'<i>\1</i>', valor) 
                        
                        Story.append(Paragraph(valor, estilo_corpo_subsecao))
                        continue 
            
            # --- 5. Outros Bullet Points (que não foram pegos acima) ---
            if line_strip.startswith('* '):
                processed_line = line_strip[2:]
                processed_line = re.sub(r'\*\*(.*%s)\*\*', r'<b>\1</b>', processed_line)
                processed_line = re.sub(r'\*(.*%s)\*', r'<i>\1</i>', processed_line)
                Story.append(Paragraph(processed_line, styles['Corpo']))
                continue


            # --- 7. **LÓGICA PARA LINKS DE COMPARAÇÃO (URLs)** ---
            # O link começa com 'http' e não tem o '*' de bullet point no início.
            if line_strip.startswith('http'):
                # CORREÇÃO: Utiliza um texto âncora curto e conciso, ideal para links de pesquisa.
                # O texto âncora agora informa que é uma pesquisa.
                link_text = f"<link href='{line_strip}'>[Pesquisar Notícia no Google]</link>"
                Story.append(Paragraph(link_text, styles['LinkPequeno']))
                # Adiciona um pequeno espaço após o link
                Story.append(Spacer(1, 0.05 * inch)) 
                continue


            # --- 8. Texto Normal (default) ---
            processed_line = line
            processed_line = re.sub(r'\*\*(.*%s)\*\*', r'<b>\1</b>', processed_line)
            processed_line = re.sub(r'\*(.*%s)\*', r'<i>\1</i>', processed_line)
            Story.append(Paragraph(processed_line, styles['Corpo']))
            
        # 9. INSERIR 'Gerado em:' NO FINAL DO RELATÓRIO
        data_hoje = date.today().strftime('%d de %B de %Y')
        # Adiciona um espaço antes do rodapé de data para separação
        Story.append(Spacer(1, 0.5 * inch)) 
        # Adiciona o texto no estilo base
        Story.append(Paragraph(f"Gerado em: {data_hoje}", styles['Base']))

        # 10. Construir o PDF
        doc.build(Story)
        if not isinstance(caminho_arquivo, io.BytesIO):
            logging.info(f"Relatório exportado para PDF com sucesso em {caminho_arquivo}")        
        return True, "Relatório exportado para PDF com sucesso!"

    except Exception as e:
        logging.error(f"Erro ao exportar relatório para PDF: {e}")
        return False, f"Erro ao exportar relatório para PDF: {e}"

def obter_dados_completos_ativo(codigo_ativo, percent_carteira):

    """Busca todos os dados de um ativo, incluindo dados fundamentalistas."""

    conn = conectar_db()

    cursor = conn.cursor()

    try:

        # Dados básicos do ativo

        cursor.execute("SELECT * FROM ativos WHERE codigo = %s", (codigo_ativo,))

        ativo_base = cursor.fetchone()

        if not ativo_base:

            return None

        

        colunas_ativos = [description[0] for description in cursor.description]

        dados_completos = dict(zip(colunas_ativos, ativo_base))

        dados_completos['percent_carteira'] = percent_carteira



        # Dados do .info

        cursor.execute("SELECT data FROM asset_info WHERE ticker = %s", (codigo_ativo,))

        info_data = cursor.fetchone()

        if info_data:

            dados_completos['info'] = json.loads(info_data[0])



        # Dados de financials (mais recente)

        cursor.execute("SELECT date, data FROM asset_financials WHERE ticker = %s ORDER BY date DESC LIMIT 1", (codigo_ativo,))

        financials_data = cursor.fetchone()

        if financials_data:

            dados_completos['financials'] = {

                'date': financials_data[0],

                'data': json.loads(financials_data[1])

            }



        # Dados de balance_sheet (mais recente)

        cursor.execute("SELECT date, data FROM asset_balance_sheet WHERE ticker = %s ORDER BY date DESC LIMIT 1", (codigo_ativo,))

        balance_sheet_data = cursor.fetchone()

        if balance_sheet_data:

            dados_completos['balance_sheet'] = {

                'date': balance_sheet_data[0],

                'data': json.loads(balance_sheet_data[1])

            }



        # Dados de cash_flow (mais recente)

        cursor.execute("SELECT date, data FROM asset_cash_flow WHERE ticker = %s ORDER BY date DESC LIMIT 1", (codigo_ativo,))

        cash_flow_data = cursor.fetchone()

        if cash_flow_data:

            dados_completos['cash_flow'] = {

                'date': cash_flow_data[0],

                'data': json.loads(cash_flow_data[1])

            }



        return dados_completos



    except Exception as e:

        logging.error(f"Erro ao obter dados completos para {codigo_ativo}: {e}")

        return None

    finally:

        conn.close()



def atualizar_dados_fundamentalistas(ticker):
    """Busca e armazena os dados fundamentalistas de um ativo (versão resiliente)."""
    conn = conectar_db()
    cursor = conn.cursor()
    try:
        logging.info(f"Atualizando dados fundamentalistas para {ticker}")
        ativo_yf = yfinance.Ticker(ticker)
        current_time = time.time() # Usar um timestamp consistente para a operação

        # 1. Salvar .info
        try:
            cursor.execute("SELECT timestamp FROM asset_info WHERE ticker = %s", (ticker,))
            last_update = cursor.fetchone()

            if not last_update or (current_time - last_update[0] >= FUNDAMENTAL_DATA_EXPIRATION_SECONDS):
                info_data = ativo_yf.info
                if info_data:
                    cursor.execute("REPLACE INTO asset_info (ticker, data, timestamp) VALUES (%s, %s, %s)",
                                   (ticker, json.dumps(info_data), current_time))
                    logging.info(f"Dados de .info para {ticker} salvos/atualizados.")
            else:
                logging.info(f"Dados de .info para {ticker} estão frescos, pulando atualização.")
        except Exception as e:
            # Captura o erro (ex: TypeError) mas permite que o resto da função continue
            logging.warning(f"Falha ao buscar .info para {ticker}: {e}")

        # 2. Salvar .financials
        try:
            cursor.execute("SELECT timestamp FROM asset_financials WHERE ticker = %s ORDER BY timestamp DESC LIMIT 1", (ticker,))
            last_update_financials = cursor.fetchone()

            if not last_update_financials or (current_time - last_update_financials[0] >= FUNDAMENTAL_DATA_EXPIRATION_SECONDS):
                financials_df = ativo_yf.financials
                if not financials_df.empty:
                    cursor.execute("DELETE FROM asset_financials WHERE ticker = %s", (ticker,)) # Limpa dados antigos
                    for date_col in financials_df.columns:
                        date_str = date_col.strftime('%Y-%m-%d')
                        data_json = financials_df[date_col].to_json()
                        cursor.execute("REPLACE INTO asset_financials (ticker, date, data, timestamp) VALUES (%s, %s, %s, %s)",
                                       (ticker, date_str, data_json, current_time))
                    logging.info(f"Dados de .financials para {ticker} salvos/atualizados.")
            else:
                logging.info(f"Dados de .financials para {ticker} estão frescos, pulando atualização.")
        except Exception as e:
            logging.warning(f"Falha ao buscar .financials para {ticker}: {e}")

        # 3. Salvar .balance_sheet
        try:
            cursor.execute("SELECT timestamp FROM asset_balance_sheet WHERE ticker = %s ORDER BY timestamp DESC LIMIT 1", (ticker,))
            last_update_balance_sheet = cursor.fetchone()

            if not last_update_balance_sheet or (current_time - last_update_balance_sheet[0] >= FUNDAMENTAL_DATA_EXPIRATION_SECONDS):
                balance_sheet_df = ativo_yf.balance_sheet
                if not balance_sheet_df.empty:
                    cursor.execute("DELETE FROM asset_balance_sheet WHERE ticker = %s", (ticker,)) # Limpa dados antigos
                    for date_col in balance_sheet_df.columns:
                        date_str = date_col.strftime('%Y-%m-%d')
                        data_json = balance_sheet_df[date_col].to_json()
                        cursor.execute("REPLACE INTO asset_balance_sheet (ticker, date, data, timestamp) VALUES (%s, %s, %s, %s)",
                                       (ticker, date_str, data_json, current_time))
                    logging.info(f"Dados de .balance_sheet para {ticker} salvos/atualizados.")
            else:
                logging.info(f"Dados de .balance_sheet para {ticker} estão frescos, pulando atualização.")
        except Exception as e:
            logging.warning(f"Falha ao buscar .balance_sheet para {ticker}: {e}")

        # 4. Salvar .cashflow
        try:
            cursor.execute("SELECT timestamp FROM asset_cash_flow WHERE ticker = %s ORDER BY timestamp DESC LIMIT 1", (ticker,))
            last_update_cash_flow = cursor.fetchone()

            if not last_update_cash_flow or (current_time - last_update_cash_flow[0] >= FUNDAMENTAL_DATA_EXPIRATION_SECONDS):
                cashflow_df = ativo_yf.cashflow
                if not cashflow_df.empty:
                    cursor.execute("DELETE FROM asset_cash_flow WHERE ticker = %s", (ticker,)) # Limpa dados antigos
                    for date_col in cashflow_df.columns:
                        date_str = date_col.strftime('%Y-%m-%d')
                        data_json = cashflow_df[date_col].to_json()
                        cursor.execute("REPLACE INTO asset_cash_flow (ticker, date, data, timestamp) VALUES (%s, %s, %s, %s)",
                                       (ticker, date_str, data_json, current_time))
                    logging.info(f"Dados de .cashflow para {ticker} salvos/atualizados.")
            else:
                logging.info(f"Dados de .cashflow para {ticker} estão frescos, pulando atualização.")
        except Exception as e:
            logging.warning(f"Falha ao buscar .cashflow para {ticker}: {e}")

        conn.commit() # Salva no banco de dados qualquer dado que tenha sido baixado com sucesso
        return True, f"Atualização de dados fundamentalistas para {ticker} concluída (com ou sem falhas parciais)."

    except Exception as e:
        # Este 'except' agora captura apenas erros críticos (ex: falha no yfinance.Ticker() ou conexão com BD)
        logging.error(f"Erro CRÍTICO ao atualizar dados fundamentalistas para {ticker}: {e}")
        conn.rollback()
        return False, f"Erro crítico ao atualizar dados para {ticker}: {e}"
    finally:
        conn.close()



if __name__ == '__main__':

    pass
