# [streamlit_app.py]
import streamlit as st
import portfolio # Seu módulo de lógica
import pandas as pd
import io 
import os
import tempfile
import plotly.express as px
import logging # Adicionado para logging

# [streamlit_app.py]
import streamlit as st
import portfolio # Seu módulo de lógica
import pandas as pd
import io 
import os
import tempfile
import plotly.express as px
import time # Para simular carregamento
import logging # Adicionado para logging
import datetime

@st.cache_data(show_spinner=False)
def generate_pdf_in_memory(report_text):
    buffer = io.BytesIO()
    # Seu portfolio.py precisa estar ajustado para aceitar um buffer (io.BytesIO)
    sucesso, mensagem = portfolio.exportar_para_pdf(buffer, report_text)
    buffer.seek(0)
    return buffer, sucesso, mensagem

# [INSERIR APÓS AS FUNÇÕES style_negativo_vermelho, style_rentabilidade, etc.]

# --------------------------------------------------------------------
# 0.5. MAPEAMENTOS DE COLUNAS (PARA LIMPEZA E ESTILO)
# --------------------------------------------------------------------
# 1. COLUMNS_MAP: Mapeia nomes originais (com _underscore) para nomes limpos (usados no Styler e para ocultar)
COLUMNS_MAP = {
    'quantidade_num': 'Nr. Cotas',
    'preco_medio_brl_num': 'Preço Médio',
    'valor_total_brl': 'Custo Total (R$)', # Título melhorado!
    'cotacao_atual_brl_num': 'Cotação Atual',
    'variacao_diaria_percent_num': 'Variação Diária',
    'valor_atual_mercado_brl': 'Valor de Mercado (R$)', # Título melhorado!
    'percent_carteira_num': 'Proporção', # Título melhorado!
    'lucro_prejuizo_num': 'Lucro', # Título melhorado!
    'rentabilidade_percent_num': 'Rendimento', # Título melhorado!
}

# 2. COLUMNS_VISIBLE: Mapeia os nomes limpos de volta para versões curtas (apenas para colunas visíveis)
# NOTA: O Streamlit já usa 'Código', 'Tipo', 'Moeda', 'Setor' automaticamente.
COLUMNS_VISIBLE = {
    # As CHAVES aqui DEVEM corresponder aos VALORES do COLUMNS_MAP
    'Nr. Cotas': 'Qtd.',
    'Preço Médio': 'Preço Médio',
    'Custo Total (R$)': 'Custo Total',
    'Cotação Atual': 'Cotação Atual',
    'Variação Diária': 'Var. Diária (%)',
    'Valor de Mercado (R$)': 'V. Mercado',
    'Lucro': 'Lucro/Prejuízo',
    'Rendimento': 'Rentabilidade',
    'Proporção': '% Cart.'
}

# --------------------------------------------------------------------
# 1. FUNÇÃO DE ESTILO: CORRIGIDA PARA USAR OS NOVOS NOMES
# --------------------------------------------------------------------
def aplicar_estilo_condicional_tabela(row):
    """
    Aplica estilo condicional (axis=1), usando as colunas RENOMEADAS (ex: 'Lucro/Prejuízo (R$)') 
    como base e aplicando o estilo na coluna de exibição (ex: 'Lucro/Prejuízo').
    """
    styles = pd.Series(data='', index=row.index)
    
    # As chaves de LEITURA são os nomes longos e limpos (ex: 'Lucro/Prejuízo (R$)')
    # As chaves de APLICAÇÃO são os nomes curtos e visíveis (ex: 'Lucro/Prejuízo')
    
    try:
        # Estilo 1: Lucro/Prejuízo
        styles['Lucro/Prejuízo'] = style_negativo_vermelho(row['Lucro'])

        # Estilo 2: Rentabilidade
        styles['Rentabilidade'] = style_rentabilidade(row['Rendimento'])

        # Estilo 3: Variação Diária
        styles['Var. Diária (%)'] = style_variacao_diaria(row['Variação Diária'])

    except KeyError as e:
        # Garante que o app não quebre se uma coluna for removida no futuro
        logging.warning(f"Erro ao aplicar estilo (KeyError): {e}. Verifique nomes de colunas de apoio.")

    return styles

# [FIM DA INSERÇÃO]

# --------------------------------------------------------------------
# 0. NOVAS FUNÇÕES DE ESTILO (PANDAS) - CORRIGIDAS
# --------------------------------------------------------------------
def style_negativo_vermelho(val):
    """Colore números positivos de verde e negativos de vermelho. Usada para Lucro (R$)."""
    # IMPORTANTE: A formatação condicional deve ser baseada no valor numérico
    if pd.isna(val): return ''
    if val < 0:
        return 'color: red;'
    elif val > 0:
        return 'color: green;'
    return '' # Retorna vazio para 0

def style_rentabilidade(val):
    """
    Regras de estilo para 'Rentabilidade (%)':
    - Positivo: Verde
    - Negativo: Vermelho (com Negrito para <= -15%)
    """
    if pd.isna(val): return ''
    #if val > 0: return 'color: green;'
    if val <= -15.0: return 'color: red; font-weight: bold;'
    if val < 0: return 'color: red;'
    return ''

def style_variacao_diaria(val):
    """
    Regras de estilo para 'Variação Diária (%)':
    - Positivo: Verde (com Negrito para >= 5%)
    - Negativo: Vermelho (com Negrito para <= -5%)
    """
    if pd.isna(val): return ''
    if val >= 5.0: return 'color: green; font-weight: bold;'
    if val > 1.0: return 'color: green;'
    if val <= -5.0: return 'color: red; font-weight: bold;'
    if val < -1.0: return 'color: red;'
    return ''


# --------------------------------------------------------------------
# [CÓDIGO EXISTENTE: Após as definições de funções (como generate_pdf_in_memory) e estilos (style_negativo_vermelho)]

# --------------------------------------------------------------------
# NOVO: Busca e Tratamento da Cotação do Dólar
# --------------------------------------------------------------------
# Chama a função que você confirmou em portfolio.py
cotacao_dolar = portfolio.buscar_cotacao_dolar() 

# Adiciona um aviso se o valor for o padrão de fallback (R$ 5,00)
# Se a sua função retorna 5.0 em caso de erro, este aviso é útil:
if cotacao_dolar is None or cotacao_dolar == 5.0:
    st.warning("⚠️ Cotação do Dólar indisponível ou usando valor padrão (R$ 5,00).")
    # Garante que cotacao_dolar tenha um valor (5.0, se o retorno da função for None ou 5.0)
    cotacao_dolar = cotacao_dolar if cotacao_dolar is not None else 5.0

# [CÓDIGO EXISTENTE: O código de inicialização do df_carteira, barra lateral, e outras lógicas da app continuam aqui]

# --------------------------------------------------------------------
# 1. FUNÇÕES DE ESTADO E RECURSOS (CACHING)
# --------------------------------------------------------------------

# Configura o logging (A função está em portfolio.py)
portfolio.setup_logging()

@st.cache_resource
def get_db_connection():
    # Inicializa a conexão com o banco de dados
    conn = portfolio.conectar_db()
    if conn:
        conn.close() # Apenas testa a conexão e fecha
        return True
    return False

# Função para buscar os dados da carteira

def get_portfolio_data():
    # Chama a função correta (visualizar_carteira)
    # A função retorna um tuple: (dataframe, lista_de_erros)
    return portfolio.visualizar_carteira()

# [streamlit_app.py]
# (Adicione esta função após 'get_portfolio_data()' e antes da Seção 2)

@st.cache_data(show_spinner="Preparando arquivo CSV...")
def get_export_csv_data():
    """
    Busca os dados brutos e converte para o formato CSV (delimitador ; e decimal ,)
    """
    df_exportar = portfolio.obter_dados_para_exportacao()
    if df_exportar.empty:
        return None
    
    # Usa io.StringIO para salvar em memória
    output = io.StringIO()
    
    # Salva no formato exato da importação:
    # sep=';' (delimitador ponto e vírgula)
    # decimal=',' (separador decimal vírgula)
    df_exportar.to_csv(
        output, 
        sep=';', 
        decimal=',', 
        index=False, 
        header=True, # Adiciona o cabeçalho que a importação espera
        encoding='utf-8'
    )
    return output.getvalue()


# --------------------------------------------------------------------
# 2. INTERFACE E LAYOUT PRINCIPAL
# --------------------------------------------------------------------

# Inicialização da Conexão (Executada uma vez)
if not get_db_connection():
    st.error("FALHA CRÍTICA: Não foi possível conectar ao banco de dados. Verifique os Secrets (nuvem) ou a variável de ambiente DATABASE_URL (local).")
    st.stop() # Interrompe a execução se não houver DB

if 'active_tab' not in st.session_state:
    st.session_state['active_tab'] = "📊 Portfólio Atual"

st.set_page_config(page_title="Analisador de Portfólio AI", layout="wide")
st.title("🛡️ Genius Analyst")
st.subheader("Análise Inteligente de Portfólio")

# --- SEÇÃO PRINCIPAL (Carrega os dados antes de desenhar a sidebar) ---
df_carteira, ativos_sem_cotacao = get_portfolio_data()

# --- BARRA LATERAL (Sidebar) para ações e inputs (AGORA COM EXPANDERS) ---
st.sidebar.header("⚙️ Gerenciar Carteira")

# --------------------------------------------------------------------
# 2.1. Adicionar/Atualizar Ativo
# --------------------------------------------------------------------
with st.sidebar.expander("➕ Adicionar/Atualizar Ativo"):
    with st.form("form_adicionar_ativo", clear_on_submit=True):
        # Campos de edição
        ticker = st.text_input("Código (ex: AAPL, PETR4)", max_chars=10).upper()
        nome = st.text_input("Nome (ex: Apple Inc.)")
        preco_medio = st.number_input("Preço Médio Pago", min_value=0.0, step=0.01, format="%.2f", key="add_preco_medio")
        quantidade = st.number_input("Quantidade", min_value=0.0, step=0.01, format="%.4f", key="add_quantidade")
        tipo = st.selectbox("Tipo", ['Ação', 'FII', 'ETF', 'BDR', 'Unit'], key="add_tipo")
        moeda = st.selectbox("Moeda", ['BRL', 'USD'], key="add_moeda")
        
        if st.form_submit_button("Salvar Ativo"):
            if ticker and nome and (preco_medio >= 0) and (quantidade >= 0): 
                ativo_data = {
                    'codigo': ticker,
                    'nome': nome,
                    'preco_medio': preco_medio,
                    'quantidade': quantidade,
                    'valor_total': preco_medio * quantidade,
                    'tipo': tipo,
                    'moeda': moeda
                }
                sucesso, mensagem = portfolio.inserir_ativo(ativo_data)
                if sucesso:
                    st.success(f"Ativo {ticker} salvo com sucesso.")
                else:
                    st.error(f"Erro ao salvar {ticker}: {mensagem}")
                st.cache_data.clear() 
            else:
                st.error("Código, Nome, Tipo e Moeda são obrigatórios.")

# --------------------------------------------------------------------
# 2.2. Importar Ativos
# --------------------------------------------------------------------
# ... (dentro da barra lateral) ...

with st.sidebar.expander("📥 Importar Ativos (.csv)"):
    # Adicione uma 'key' ao file_uploader
    uploaded_file = st.file_uploader("Selecione o arquivo CSV:", type=['csv'], key="csv_uploader")
    
    # !! LÓGICA DE CONTROLE DE LOOP !!
    # Verificamos se o arquivo existe E se o ID dele é diferente do último que processamos
    if uploaded_file is not None and st.session_state.get('processed_file_id') != uploaded_file.file_id:
        try:
            # Cria um arquivo temporário para que o backend possa ler o CSV
            with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp_file:
                tmp_file.write(uploaded_file.getvalue())
                temp_path = tmp_file.name

            # PASSO 1: Importação rápida (sem APIs)
            with st.spinner("Aguarde, importando dados para o banco..."):
                # A função agora retorna a lista de tickers
                sucesso, mensagem, tickers_importados = portfolio.importar_ativos_do_arquivo(temp_path)
            
            os.unlink(temp_path) # Limpa o arquivo temporário
            st.cache_data.clear()

            if sucesso:
                st.success(f"Importação Concluída: {mensagem}")
                
                # Salva o ID do arquivo na sessão para evitar o loop
                st.session_state.processed_file_id = uploaded_file.file_id
                
                # PASSO 2: Atualização em paralelo (com APIs)
                if tickers_importados:
                    with st.spinner(f"Atualizando dados fundamentalistas para {len(tickers_importados)} ativos em paralelo... (Isso pode levar um momento)"):
                        portfolio.atualizar_fundamentos_em_massa(tickers_importados)
                    st.success("Atualização de fundamentos concluída.")
                
            else:
                st.error(f"Erro na Importação: {mensagem}")
            
            st.rerun() # Recarrega para atualizar a tabela (agora de forma segura)
        
        except Exception as e:
            st.error(f"Erro ao processar arquivo: {e}")
            # Limpa o ID em caso de erro para permitir nova tentativa
            st.session_state.processed_file_id = None
            
# --------------------------------------------------------------------
# NOVO: 2.2.5. Exportar Ativos
# --------------------------------------------------------------------
with st.sidebar.expander("📤 Exportar Ativos (.csv)"):
    st.info("Exporte sua carteira atual no formato exato de importação.")
    
    # A função cacheada 'get_export_csv_data' é chamada aqui.
    # O cache dela será limpo sempre que 'st.cache_data.clear()' 
    # for chamado (nas funções de adicionar/excluir/importar).
    csv_data = get_export_csv_data()
    
    if csv_data:
        st.download_button(
            label="Baixar Carteira (.csv)",
            data=csv_data,
            file_name=f"carteira_export_{pd.Timestamp.now().strftime('%Y%m%d')}.csv",
            mime="text/csv",
        )
    else:
        st.info("A carteira está vazia. Adicione ativos para exportar.")
        
# --------------------------------------------------------------------
# 2.3. Exclusão Individual
# --------------------------------------------------------------------
with st.sidebar.expander("➖ Excluir Ativo Específico"):
    if not df_carteira.empty:
        lista_tickers_excluir = df_carteira['Código'].tolist()
        ticker_para_excluir = st.selectbox(
            "Selecione o ativo para excluir:",
            options=lista_tickers_excluir,
            key="select_excluir"
        )
        
        if st.button("Excluir Ativo Selecionado", key="btn_excluir_unico"):
            if ticker_para_excluir:
                
                @st.dialog("Confirmar Exclusão")
                def confirmar_exclusao_unica(ticker):
                    st.warning(f"Você tem certeza que deseja excluir **{ticker}**? Esta ação não pode ser desfeita.")
                    if st.button("Confirmar Exclusão", key="conf_excluir"):
                        # Chama a função excluir_ativo do portfolio.py
                        sucesso, mensagem = portfolio.excluir_ativo(ticker) 
                        if sucesso:
                            st.cache_data.clear() # Limpa o cache para atualizar a tabela
                            st.rerun() # Recarrega a página
                        else:
                            st.error(f"Erro ao excluir: {mensagem}")
                
                confirmar_exclusao_unica(ticker_para_excluir)
    else:
        st.info("Nenhum ativo na carteira para excluir.")

# --------------------------------------------------------------------
# 2.4. Limpar Carteira (Total)
# --------------------------------------------------------------------
with st.sidebar.expander("💣 Limpar Carteira (Tudo)"):
    st.warning("Esta ação removerá **TODOS** os ativos e caches. Use com extrema cautela.")
    
    if st.button("Limpar Carteira (Excluir Tudo)", key="btn_limpar_tudo"):
        @st.dialog("Confirmar Exclusão Total")
        def confirmar_limpeza():
            st.warning("Você tem certeza que deseja excluir **TODOS** os ativos da sua carteira? Esta ação não pode ser desfeita.")
            if st.button("Confirmar Exclusão Total", key="conf_limpar_tudo"):
                try:
                    portfolio.limpar_carteira() 
                    st.cache_data.clear()
                    st.rerun() 
                except Exception as e:
                    st.error(f"Erro ao limpar carteira: {e}")
        
        confirmar_limpeza()

# [CÓDIGO EXISTENTE: ... continua o último expander de gerenciamento de carteira]

# --------------------------------------------------------------------
# NOVO: 2.4.5. Filtros da Carteira
# --------------------------------------------------------------------
# [CÓDIGO EXISTENTE: ... continua o último expander de gerenciamento de carteira]

# --------------------------------------------------------------------
# NOVO: 2.4.5. Filtros da Carteira (Usando st.sidebar.expander)
# --------------------------------------------------------------------

# Cria o expander para ocultar os filtros por padrão.
with st.sidebar.expander("🔍 Filtros da Tabela"):
    
    # 1. Obter e exibir os filtros na sidebar
    # Nota: A variável df_carteira deve estar definida antes deste bloco.
    if not df_carteira.empty:
        # Opções únicas, incluindo 'Todos' para desativar o filtro
        moedas_unicas = ['Todos'] + sorted(df_carteira['moeda'].unique().tolist())
        tipos_unicos = ['Todos'] + sorted(df_carteira['Tipo'].unique().tolist())

        # Agora, os selectbox estão DENTRO do expander
        filtro_moeda = st.selectbox( 
            "Filtrar por Moeda:",
            options=moedas_unicas,
            index=0, # 'Todos' é o padrão
            key="filtro_moeda_sb"
        )

        filtro_tipo = st.selectbox(
            "Filtrar por Tipo de Ativo:",
            options=tipos_unicos,
            index=0, # 'Todos' é o padrão
            key="filtro_tipo_sb"
        )
    else:
        # Valores padrão se a carteira estiver vazia
        # NOTA: Estas variáveis precisam ser definidas antes da lógica de filtragem, 
        # mesmo que o expander não seja aberto.
        filtro_moeda = 'Todos'
        filtro_tipo = 'Todos'
        st.info("Adicione ativos para usar os filtros.") # st.info está dentro do expander

# OBSERVAÇÃO CRÍTICA:
# Quando usamos componentes Streamlit DENTRO de um st.sidebar.expander, 
# devemos usar a chamada simplificada (e.g., st.selectbox) e NÃO a chamada explícita (st.sidebar.selectbox).
# O contexto do 'with st.sidebar.expander(...):' já garante que o componente estará na sidebar.

# --- LÓGICA DE FILTRAGEM (Deve vir DEPOIS deste bloco, na parte principal do script) ---
# ... (Sua lógica de filtragem usando df_carteira, df_filtrada, filtro_moeda e filtro_tipo deve continuar aqui)
# --- LÓGICA DE FILTRAGEM (Deve vir antes da Seção 3: EXIBIÇÃO DA CARTEIRA) ---

# Inicia com o DataFrame completo
df_filtrada = df_carteira.copy()

# Aplica o filtro de Moeda
if filtro_moeda != 'Todos':
    # A coluna 'moeda' é a coluna com o valor original (BRL/USD)
    df_filtrada = df_filtrada[df_filtrada['moeda'] == filtro_moeda]

# Aplica o filtro de Tipo de Ativo
if filtro_tipo != 'Todos':
    # A coluna 'Tipo' é a coluna renomeada para exibição
    df_filtrada = df_filtrada[df_filtrada['Tipo'] == filtro_tipo]

# [CÓDIGO EXISTENTE: NOVO: 2.5. Gráfico de Histórico de Preços (NOVA POSIÇÃO: Sidebar)]
# ... (O restante da sidebar continua aqui)

# --------------------------------------------------------------------
# NOVO: 2.5. Gráfico de Histórico de Preços (NOVA POSIÇÃO: Sidebar)
# --------------------------------------------------------------------
st.sidebar.divider()
st.sidebar.header("Análise Avançada")

with st.sidebar.expander("📈 Gráfico de Histórico"):
    if df_carteira.empty:
        st.info("Adicione ativos primeiro.")
    else:
        lista_tickers_graf = df_carteira['Código'].tolist()
        periodos_graf = ['1 Dia', '1 Semana', '1 Mês', '12 Meses', '2 Anos', '5 Anos', '10 Anos', '20 Anos', 'Máximo (MAX)']
        
        ticker_graf = st.selectbox("Ativo:", lista_tickers_graf, key="graf_ticker_sb")
        periodo_graf = st.selectbox("Período:", periodos_graf, index=3, key="graf_periodo_sb")
        
        if st.button("Gerar Gráfico", key="graf_btn_sb"):
            # Armazena os parâmetros e define a flag para gerar no corpo principal
            st.session_state['show_chart_trigger'] = True
            st.session_state['chart_ticker'] = ticker_graf
            st.session_state['chart_periodo'] = periodo_graf
            
            new_tab = "📈 Histórico de Preços"
            st.session_state['active_tab'] = new_tab
            st.session_state['main_tab_radio'] = new_tab # <-- FORÇA O ESTADO DO WIDGET 
                       
            st.rerun() 

# [streamlit_app.py]
# SUBSTITUA o expander "📊 Gráfico Comparativo Normalizado" (aprox. linha 211) por este:

with st.sidebar.expander("📊 Gráfico Comparativo Normalizado"):
    if df_carteira.empty:
        st.info("Adicione ativos primeiro.")
    else:
        # 1. NOVO: Seletor de Moeda
        moeda_selecionada = st.radio(
            "Selecione a Moeda para Comparação:",
            ['BRL', 'USD'], 
            horizontal=True,
            key='comp_moeda_radio'
        )
        
        # 2. MODIFICADO: Filtra a lista de tickers pela moeda selecionada
        if moeda_selecionada == 'BRL':
            df_filtrado = df_carteira[df_carteira['moeda'] == 'BRL']
            lista_tickers_comp = df_filtrado['Código'].tolist()
        else:
            df_filtrado = df_carteira[df_carteira['moeda'] == 'USD']
            lista_tickers_comp = df_filtrado['Código'].tolist()

        periodos_comp = ['1 Mês', '12 Meses', '2 Anos', '5 Anos', '10 Anos', '20 Anos', 'Máximo (MAX)']

        # 3. MODIFICADO: O multiselect agora usa a lista filtrada
        ativos_selecionados = st.multiselect(
            f"Selecione os ativos ({moeda_selecionada}):", 
            lista_tickers_comp,
            key="comp_multiselect"
        )
        periodo_comp = st.selectbox("Período:", periodos_comp, index=1, key="comp_periodo_select")

        if st.button("Gerar Comparativo"):
            if not ativos_selecionados:
                st.warning("Por favor, selecione pelo menos um ativo para comparar.")
            else:
                st.session_state['comparativo_trigger'] = True
                st.session_state['comparativo_tickers'] = ativos_selecionados
                st.session_state['comparativo_periodo'] = periodo_comp
                # 4. NOVO: Salva a moeda selecionada no estado da sessão
                st.session_state['comparativo_moeda'] = moeda_selecionada 
                st.session_state['active_tab'] = "📊 Comparativo Normalizado"
                st.rerun()


# --------------------------------------------------------------------
# NOVO: 2.6. Geração de Relatório e Análise IA (NOVA POSIÇÃO: Sidebar)
# --------------------------------------------------------------------
with st.sidebar.expander("🤖 Análise de Portfólio (AI)"):
    if df_carteira.empty:
        st.info("Adicione ativos primeiro.")
    else:
        # Análise Completa da Carteira
        if st.button("Análise Completa da Carteira", key="analise_completa_btn"):
            # Define a flag para gerar no corpo principal
            st.session_state['run_ai_analysis'] = 'full_portfolio_pending'
            
            new_tab = "📝 Relatório (Carteira)"
            st.session_state['active_tab'] = new_tab
            st.session_state['main_tab_radio'] = new_tab # <-- FORÇA O ESTADO DO WIDGET
            
            st.rerun()
            
        st.markdown("---")
        
        # Análise de Ativo Único
        lista_tickers = df_carteira['Código'].tolist()
        ticker_selecionado = st.selectbox("Ativo para Análise Individual:", lista_tickers, key="analise_ticker_sb")
        
        if st.button("Analisar Ativo", key="analisar_ativo_btn"):
            # Define a flag e o ticker para gerar no corpo principal
            st.session_state['run_ai_analysis'] = 'single_asset_pending'
            st.session_state['single_asset_ticker'] = ticker_selecionado
            
            new_tab = "🔎 Relatório (Ativo Único)"
            st.session_state['active_tab'] = new_tab
            st.session_state['main_tab_radio'] = new_tab # <-- FORÇA O ESTADO DO WIDGET            
            st.rerun()


# --------------------------------------------------------------------
# 3. EXIBIÇÃO DA CARTEIRA E ABAS DE ANÁLISE (CORRIGIDO COM on_change)
# --------------------------------------------------------------------

# 1. Define a lista de "abas"
tabs_list = [
    "📊 Portfólio Atual", 
    "📈 Histórico de Preços", 
    "📊 Comparativo Normalizado",
    "📝 Relatório (Carteira)", 
    "🔎 Relatório (Ativo Único)"
]

# 2. NOVO: Define o callback
# Quando o *usuário* clica no rádio, esta função é chamada.
# Ela sincroniza o estado do widget (main_tab_radio) para o estado "master" (active_tab)
def update_active_tab_from_radio():
    st.session_state.active_tab = st.session_state.main_tab_radio

# 3. Garante que o estado da sessão é válido e existe
if 'active_tab' not in st.session_state or st.session_state['active_tab'] not in tabs_list:
    st.session_state['active_tab'] = tabs_list[0] # Padrão
    
# 4. Garante que o estado do widget (main_tab_radio) existe e está sincronizado
# (Isso é crucial para a primeira execução)
if 'main_tab_radio' not in st.session_state:
    st.session_state.main_tab_radio = st.session_state.active_tab

# 6. Cria o st.radio (sem 'index')
st.radio(
    "Navegação Principal",
    tabs_list,
    # O parâmetro 'index' FOI REMOVIDO.
    # O 'key' agora controla totalmente o widget.
    key="main_tab_radio", 
    on_change=update_active_tab_from_radio, 
    horizontal=True,
    label_visibility="collapsed"
)

# 7. REMOVIDO: A linha 'st.session_state['active_tab'] = selected_tab'
# que estava aqui foi removida, pois causava a race condition.
# O callback 'update_active_tab_from_radio' agora faz esse trabalho.

# 8. O script principal lê o "master" state (que está sempre correto)
selected_tab = st.session_state.active_tab

# --- CONTEÚDO DA ABA 1: PORTFÓLIO ATUAL ---
if selected_tab == "📊 Portfólio Atual":
    # (O restante do seu código if/elif permanece exatamente o mesmo)
    # Exibe aviso se houver ativos sem cotação
    if ativos_sem_cotacao:
        st.warning(f"Não foi possível obter cotações para: {', '.join(ativos_sem_cotacao)}")
    
    # ... (todo o código da aba "Portfólio Atual" vai aqui) ...
    # (cole o código que você já tinha)
    if df_filtrada.empty:
        if not df_carteira.empty:
            st.info("Nenhum ativo encontrado com os filtros selecionados.")
        else:
            st.info("Sua carteira está vazia. Adicione ativos no menu lateral.")
    else:
        st.subheader("Resumo da Carteira (em R$)")

        # 1. Cálculo dos Totais (Métricas)
        total_investido = df_filtrada['valor_total_brl'].sum()
        total_atual = df_filtrada['valor_atual_mercado_brl'].sum()
        lucro_prejuizo_total = total_atual - total_investido
        rentabilidade_total = (lucro_prejuizo_total / total_investido) * 100 if total_investido != 0 else 0

        # 2. Exibição dos Totais (Métricas)
        col1, col2, col3, col4, col5 = st.columns(5)
        
        # Função local para formatar moeda
        def format_brl(value):
            return f"R$ {value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

        col1.metric("Total Investido", format_brl(total_investido))
        col2.metric("Valor de Mercado Atual", format_brl(total_atual))
        col3.metric(
            "Lucro/Prejuízo Total",
            format_brl(lucro_prejuizo_total),
            f"{rentabilidade_total:,.2f}%".replace(",", "X").replace(".", ",").replace("X", ".")
        )
        col4.metric(
            "Ativos em Carteira",
            f"{len(df_filtrada)} Ativo(s)",
            help="Número de ativos que correspondem aos filtros."
        )
        col5.metric(
            "Cotação USD/BRL",
            f"R$ {cotacao_dolar:,.4f}".replace(",", "X").replace(".", ",").replace("X", ".")
        )

        st.markdown("---")
        st.subheader("Tabela de Ativos Detalhada")

        df_para_estilizar = df_filtrada.copy()

        # [INÍCIO DA CORREÇÃO OBRIGATÓRIA]
        colunas_conflitantes_string = [
            'Preço Médio', 'Valor Investido', 'Cotação Atual', 'Valor Atual',
            'Lucro (R$)', 'Rentabilidade (%)', 'Variação Diária (%)', '% Carteira'
        ]
        cols_para_remover = [col for col in colunas_conflitantes_string if col in df_para_estilizar.columns]
        if cols_para_remover:
            df_para_estilizar.drop(columns=cols_para_remover, inplace=True)
        # [FIM DA CORREÇÃO OBRIGATÓRIA]

        # 1. Renomeia colunas _num para nomes limpos e longos (Fontes de dados)
        df_para_estilizar.rename(columns=COLUMNS_MAP, inplace=True)
        
        # [CORREÇÃO 1: Key Error Index]
        df_para_estilizar.reset_index(drop=True, inplace=True) 

        # 1. Renomeia colunas _num para nomes limpos e longos (Fontes de dados)
        df_para_estilizar.rename(columns=COLUMNS_MAP, inplace=True)
        
        # [CORREÇÃO 2: Trata ValueError de Colunas Duplicadas]
        for long_name, short_name in COLUMNS_VISIBLE.items():
            if long_name in df_para_estilizar.columns:
                col_data = df_para_estilizar[long_name]
                if isinstance(col_data, pd.DataFrame):
                    col_data = col_data.iloc[:, 0] 
                df_para_estilizar[short_name] = col_data

        if 'moeda' in df_para_estilizar.columns:
            df_para_estilizar['Moeda'] = df_para_estilizar['moeda']
        elif 'Moeda' not in df_para_estilizar.columns:
            df_para_estilizar['Moeda'] = 'BRL'
            
            
            
            
            

        # 5. Aplicação dos Estilos (Cores)
        styler = df_para_estilizar.style

        # Aplica as regras de cor (usa Long Names como fonte, Short Names como alvo)
        styler = styler.apply(
            aplicar_estilo_condicional_tabela,
            axis=1
        )
        
        # --------------------------------------------------------------------
        # 5.5. INSERIR O NOVO BLOCO EXATAMENTE AQUI
        # --------------------------------------------------------------------
        # Funções lambda para formatação PT-BR (R$ 1.234,56)
        format_brl = lambda x: f"R$ {x:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        format_percent = lambda x: f"{x:,.2f}%".replace(",", "X").replace(".", ",").replace("X", ".")
        format_qtd = lambda x: f"{x:,.4f}".replace(",", "X").replace(".", ",").replace("X", ".") # Para cotas
        
        # Dicionário de formatadores para as colunas VISÍVEIS
        # (As chaves DEVEM ser os VALORES do COLUMNS_VISIBLE)
        formatter_dict = {
            'Qtd.': format_qtd,
            'Preço Médio': format_brl,
            'Custo Total': format_brl,
            'Cotação Atual': format_brl,
            'V. Mercado': format_brl,
            'Lucro/Prejuízo': format_brl,
            'Var. Diária (%)': format_percent,
            'Rendimento': format_percent,
            '% Cart.': format_percent
        }
        
        # Aplica a formatação de exibição (texto)
        styler = styler.format(formatter_dict)
        # --------------------------------------------------------------------
        # FIM DA INSERÇÃO
        # --------------------------------------------------------------------
        
        # Os valores são os 'VALORES' do COLUMNS_VISIBLE
        COLUMNS_ORDER_LIST = [
            'Código', 'Tipo', 'Moeda', 'Setor', # Colunas originais não mapeadas
            'Qtd.', 'Preço Médio', 'Custo Total', 'Cotação Atual', 
            'V. Mercado', 'Lucro/Prejuízo', # Colunas financeiras principais
            # Colunas de cálculo/percentual no final
            'Var. Diária (%)', 'Rendimento', '% Cart.' 
        ]
        
        # --- CORREÇÃO DA LÓGICA (O SEU CÓDIGO ESTAVA QUEBRADO AQUI) ---
        # Esta lógica deve vir ANTES da chamada do st.dataframe
        colunas_a_esconder_nomes_limpos = list(COLUMNS_MAP.values())
        
        config_colunas = {}
        for col in colunas_a_esconder_nomes_limpos:
            if col in df_para_estilizar.columns:
                config_colunas[col] = {'visible': False}
        # --- FIM DA CORREÇÃO DA LÓGICA ---

        # 6. Exibe a tabela usando o st.dataframe (chamada corrigida)
        tabela_interativa = st.dataframe(
            styler,
            column_config=config_colunas, # Oculta as colunas longas (fontes numéricas)
            column_order=COLUMNS_ORDER_LIST,
            use_container_width=True,
            selection_mode='single-row', # Habilita o evento click
            key="tabela_ativos_selecao" 
        )
        
        # 7. Captura de Evento Click (Seleção de Linha)
        selecao_state = st.session_state.get('tabela_ativos_selecao', {})
        selecao = selecao_state.get('selection', {'rows': []})

        if selecao and selecao.get('rows'):
            indice_selecionado = selecao['rows'][0]
            # Usamos .iloc para acessar a linha correta no DataFrame original (df_filtrada)
            linha_selecionada = df_filtrada.iloc[[indice_selecionado]]
            ativo_selecionado = linha_selecionada['Código'].iloc[0]
            st.toast(f"Ativo '{ativo_selecionado}' selecionado para análise!", icon="✅")




# --- CONTEÚDO DA ABA 2: GRÁFICO DE HISTÓRICO ---
elif selected_tab == "📈 Histórico de Preços":
    # (cole o código que você já tinha)
    st.subheader("📈 Histórico de Preços")

    if st.session_state.get('show_chart_trigger') or 'dados_historicos' in st.session_state:
        if st.session_state.get('show_chart_trigger'):
            st.session_state['show_chart_trigger'] = False
            st.session_state['dados_historicos'] = pd.DataFrame() 

            with st.spinner(f"Buscando histórico para {st.session_state['chart_ticker']}..."):
                try:
                    ticker_graf = st.session_state['chart_ticker']
                    periodo_graf = st.session_state['chart_periodo']
                    ativo_info = df_carteira[df_carteira['Código'] == ticker_graf].iloc[0]
                    moeda_ativo = ativo_info['moeda'] 
                    
                    ticker_yf = ticker_graf
                    if moeda_ativo == 'BRL' and ativo_info['Tipo'] in ['Ação', 'FII', 'Unit', 'BDR'] and not ticker_yf.endswith('.SA'):
                        ticker_yf = f"{ticker_yf}.SA"
                    elif moeda_ativo == 'USD' and ativo_info['Tipo'] == 'Ação' and ticker_yf.endswith('.SA'):
                         ticker_yf = ticker_yf.replace('.SA', '')
                    
                    dados_historicos_raw = portfolio.buscar_dados_historicos(ticker_yf, periodo_graf)
                    
                    if dados_historicos_raw is None or dados_historicos_raw.empty:
                        st.error(f"Não foi possível obter dados históricos para {ticker_yf}.")
                    else:
                        if isinstance(dados_historicos_raw.columns, pd.MultiIndex):
                            dados_historicos_raw.columns = dados_historicos_raw.columns.get_level_values(0)
                        
                        if 'Adj Close' in dados_historicos_raw.columns:
                            dados_historicos = dados_historicos_raw[['Adj Close']].rename(columns={'Adj Close': 'Preço'})
                        elif 'Close' in dados_historicos_raw.columns:
                            dados_historicos = dados_historicos_raw[['Close']].rename(columns={'Close': 'Preço'})
                        else:
                            st.error(f"Erro: Colunas de preço não encontradas no histórico para {ticker_yf}.")
                            dados_historicos = pd.DataFrame() 
                        
                        st.session_state['dados_historicos'] = dados_historicos
                        st.session_state['grafico_ticker'] = ticker_graf
                        st.session_state['grafico_periodo'] = periodo_graf
                        st.session_state['grafico_moeda'] = moeda_ativo
                
                except Exception as e:
                    st.error(f"Erro ao buscar dados para o gráfico: {e}")
                    st.session_state['dados_historicos'] = pd.DataFrame()
                    
        dados = st.session_state.get('dados_historicos')
        ticker_graf = st.session_state.get('grafico_ticker')
        
        if not dados.empty:
            min_preco = dados['Preço'].min()
            max_preco = dados['Preço'].max()
            margem = (max_preco - min_preco) * 0.01 
            auto_min = max(0, min_preco - margem) 
            auto_max = max_preco + margem
            range_min = auto_min
            range_max = auto_max
            
            st.markdown(f"**Histórico ({st.session_state['grafico_periodo']}):** `{ticker_graf}`")

            with st.expander("Ajustar a Escala de Preços (Eixo Y)"):
                manual_range = st.slider(
                    "Selecione o Intervalo de Preços (Eixo Y)",
                    min_value=0.0,
                    max_value=max_preco * 1.5, 
                    value=(auto_min, auto_max),
                    step=0.01,
                    format="%.2f",
                    key='manual_y_range_slider'
                )
                range_min, range_max = manual_range
                
                if st.button("Resetar Escala para Automática"):
                    del st.session_state['manual_y_range_slider']
                    st.rerun()

            moeda_grafico = st.session_state.get('grafico_moeda', 'BRL')
            if moeda_grafico == 'USD':
                label_y_axis = 'Preço (US$)'
            else:
                label_y_axis = 'Preço (R$)'

            fig = px.line(
                dados.reset_index(), 
                x=dados.index.name or 'Date',
                y='Preço',
                title=f'Histórico de Preços para {ticker_graf}',
                labels={'Preço': label_y_axis, 'Date': 'Data'}
            )

            fig.update_yaxes(range=[range_min, range_max])
            st.plotly_chart(fig, use_container_width=True)
            
            st.caption("A escala vertical (Eixo Y) foi ajustada automaticamente ao range de preço do período, mas pode ser modificada no menu 'Ajustar a Escala de Preços'.")

        elif st.session_state.get('grafico_ticker'):
            st.error("Não foi possível gerar o gráfico ou os dados estão vazios.")

    else:
        st.info("Use o painel **📈 Gráfico de Histórico** na barra lateral para gerar um gráfico.")

# --- CONTEÚDO DA ABA 3: GRÁFICO COMPARATIVO ---
elif selected_tab == "📊 Comparativo Normalizado":
    # (cole o código que você já tinha)
    st.subheader("📊 Comparativo Normalizado de Ativos")

    if st.session_state.get('comparativo_trigger') or 'dados_comparativo' in st.session_state:
        if st.session_state.get('comparativo_trigger'):
            st.session_state['comparativo_trigger'] = False
            tickers = st.session_state.get('comparativo_tickers', [])
            periodo = st.session_state.get('comparativo_periodo', '12 Meses')
            
            periodo_yf = "1y" 
            if periodo == "1 Mês": periodo_yf = "1mo"
            if periodo == "2 Anos": periodo_yf = "2y"
            # (Adicione outros mapeamentos se necessário)

            with st.spinner("Gerando gráfico comparativo..."):
                dados_norm, msg = portfolio.obter_precos_historicos_normalizados(tickers, periodo=periodo_yf)
                
                if dados_norm.empty:
                    st.error(msg)
                    st.session_state['dados_comparativo'] = pd.DataFrame()
                else:
                    st.session_state['dados_comparativo'] = dados_norm

        dados_norm = st.session_state.get('dados_comparativo', pd.DataFrame())
        
        moeda_grafico = st.session_state.get('comparativo_moeda', 'BRL')
        
        if moeda_grafico == 'BRL':
            label_y_axis = "Preço Normalizado (R$)"
        else:
            label_y_axis = "Preço Normalizado (US$)"

        if not dados_norm.empty:
            fig = px.line(
                dados_norm,
                x='Data',
                y=dados_norm.columns.drop('Data'),
                title='Comparativo Normalizado (Base = 1.0)',
                labels={'value': label_y_axis, 'variable': 'Ativo'}
            )
            st.plotly_chart(fig, use_container_width=True)
            st.caption("Os preços foram normalizados para começar em 1.0 no início do período.")
        else:
            if st.session_state.get('comparativo_tickers'): 
                st.info("Não há dados para exibir para os ativos selecionados.")
    else:
        st.info("Use o painel **📊 Gráfico Comparativo Normalizado** na barra lateral para gerar um gráfico.")


# --- CONTEÚDO DA ABA 4: RELATÓRIO CARTEIRA ---
elif selected_tab == "📝 Relatório (Carteira)":
    # (cole o código que você já tinha)
    st.subheader("Relatório de Análise da Carteira (Completo)")
    
    if st.session_state.get('run_ai_analysis') == 'full_portfolio_pending':
        
        st.session_state['run_ai_analysis'] = None
        st.session_state['report_text_full'] = None
        st.session_state['report_filename_full'] = None

        with st.spinner("🤖 Analisando carteira e gerando relatório via Google AI..."):
            
            df_para_ia = df_carteira[['Código', 'quantidade_num', 'preco_medio_brl_num', 'cotacao_atual_brl_num']].copy()
            df_para_ia.columns = ['codigo', 'quantidade', 'preco_medio', 'valor_atual']
            
            df_para_ia['setor'] = df_para_ia['codigo'].apply(lambda x: portfolio.obter_setor_pais_ativo(x)[0] or "N/A")
            df_para_ia['pais'] = df_para_ia['codigo'].apply(lambda x: portfolio.obter_setor_pais_ativo(x)[1] or "N/A")

            carteira_json = df_para_ia.to_json(orient='records')
            
            analise_json, cache_used = portfolio.gerar_analise_ia_carteira(carteira_json)
            
            if "erro" not in analise_json:
                report_text = portfolio.formatar_analise_para_texto(analise_json)
                now = datetime.datetime.now()
                timestamp = now.strftime("%d-%m-%Y %H.%M")
                st.session_state['report_text_full'] = report_text
                st.session_state['report_filename_full'] = f"Relatorio de Analise do Portfolio {timestamp}.pdf"
                st.success("Análise da IA concluída! Veja o relatório abaixo.")
                st.markdown(report_text, unsafe_allow_html=True)
            else:
                st.error(f"Erro na análise da IA: {analise_json['erro']}")
                st.session_state['report_text_full'] = f"Erro na análise: {analise_json['erro']}"

    elif 'report_text_full' in st.session_state and st.session_state['report_text_full']:
        st.success("Análise da IA concluída! Veja o relatório abaixo.")
        st.markdown(st.session_state['report_text_full'], unsafe_allow_html=False)
    else:
        st.info("Use o botão **Análise Completa da Carteira** no painel **🤖 Análise de Portfólio (AI)** na barra lateral para gerar o relatório.")


# --- CONTEÚDO DA ABA 5: RELATÓRIO ATIVO ÚNICO ---
elif selected_tab == "🔎 Relatório (Ativo Único)":
    # (cole o código que você já tinha)
    st.subheader("Relatório de Análise de Ativo Único")

    if st.session_state.get('run_ai_analysis') == 'single_asset_pending':
        
        st.session_state['run_ai_analysis'] = None
        ticker_selecionado = st.session_state['single_asset_ticker']
        st.session_state['report_text_single'] = None
        st.session_state['report_filename_single'] = None

        with st.spinner(f"🤖 Analisando {ticker_selecionado}..."):
            try:
                ticker_yf = ticker_selecionado
                ativo_info = df_carteira[df_carteira['Código'] == ticker_selecionado].iloc[0]
                if ativo_info['moeda'] == 'BRL' and (ativo_info['Tipo'] in ['Ação', 'FII', 'Unit', 'BDR']) and not ticker_yf.endswith('.SA'):
                    ticker_yf = f"{ticker_yf}.SA"
                
                sucesso_update, msg_update = portfolio.atualizar_dados_fundamentalistas(ticker_yf)
                if not sucesso_update:
                    st.warning(msg_update)

                percent_carteira = df_carteira[df_carteira['Código'] == ticker_selecionado]['percent_carteira_num'].iloc[0]
                dados_completos = portfolio.obter_dados_completos_ativo(ticker_selecionado, percent_carteira)

                if not dados_completos:
                    st.error(f"Não foi possível obter dados completos para {ticker_selecionado}.")
                    st.session_state['report_text_single'] = f"Não foi possível obter dados completos para {ticker_selecionado}."
                else:
                    analise_json, cache_used = portfolio.gerar_analise_ia_ativo(dados_completos)
					
                    if "erro" not in analise_json:
                        moeda_ativo = ativo_info['moeda']
                        acao = analise_json.get("proximos_passos", {}).get("acao_sugerida", "").lower()
						
                        try:
                            link = portfolio.gerar_link_google_finance([ticker_selecionado], moeda_ativo)
                            
                            if link:
                                link_markdown = f"**<a href='{link}' target='_blank'> {ticker_selecionado} vs Benchmark </a>**"
                                
                                if "proximos_passos" in analise_json and "ferramentas_comparacao" in analise_json["proximos_passos"]:
                                    analise_json["proximos_passos"]["ferramentas_comparacao"]["link_gerado"] = link_markdown
                                else:
                                    logging.warning(f"Não foi possível injetar o link GF no JSON para {ticker_selecionado}. Estrutura 'proximos_passos.ferramentas_comparacao' não encontrada.")
                                         
                        except Exception as e:
                            logging.error(f"Erro ao gerar link GF para {ticker_selecionado}: {e}")
								
                        report_text = portfolio.formatar_analise_para_texto(analise_json)
                        now = datetime.datetime.now()
                        timestamp = now.strftime("%d-%m-%Y %H.%M")						
                        st.session_state['report_text_single'] = report_text
                        st.session_state['report_filename_single'] = f"Relatorio de Analise do Ativo {ticker_selecionado} {timestamp}.pdf"
                        st.success(f"Análise de {ticker_selecionado} concluída!")
                        st.markdown(report_text, unsafe_allow_html=True)
                    else:
                        st.error(f"Erro na análise da IA: {analise_json['erro']}")
                        st.session_state['report_text_single'] = f"Erro na análise: {analise_json['erro']}"
            
            except Exception as e:
                st.error(f"Erro crítico ao analisar ativo: {e}")
                logging.error(f"Erro crítico ao analisar ativo {ticker_selecionado}: {e}", exc_info=True)
                st.session_state['report_text_single'] = f"Erro crítico: {e}"


    elif 'report_text_single' in st.session_state and st.session_state['report_text_single']:
        st.success("Análise da IA concluída! Veja o relatório abaixo.")
        report_text = st.session_state['report_text_single']
        st.markdown(report_text, unsafe_allow_html=True)
        
        report_filename = st.session_state.get('report_filename_single')

        if report_text and report_filename:
            pdf_buffer, sucesso, mensagem = generate_pdf_in_memory(report_text)
            
            if sucesso:
                st.download_button(
                    label=f"💾 Download Relatório de Ativo Único (PDF)",
                    data=pdf_buffer.read(),
                    file_name=report_filename,
                    mime="application/pdf"
                )
            else:
                st.error(f"Erro ao preparar PDF para download: {mensagem}")

    else:
        st.info("Use o seletor **Ativo para Análise Individual** no painel **🤖 Análise de Portfólio (AI)** na barra lateral para gerar o relatório.")

# --------------------------------------------------------------------
# 5. DOWNLOAD (Unificado para ambos os relatórios)
# --------------------------------------------------------------------
# (cole o código que você já tinha)
report_text_full = st.session_state.get('report_text_full')
report_text_single = st.session_state.get('report_text_single')

if report_text_full or report_text_single:
    st.sidebar.divider()
    st.sidebar.header("📥 Baixar Relatórios")
    
    filename_full = st.session_state.get('report_filename_full')
    if report_text_full and isinstance(filename_full, str) and filename_full.startswith('Relatorio de Analise do Portfolio'):
        pdf_buffer_full, sucesso_full, mensagem_full = generate_pdf_in_memory(report_text_full)
        if sucesso_full:
            st.sidebar.download_button(
                label="💾 Análise da Carteira (PDF)",
                data=pdf_buffer_full.read(),
                file_name=st.session_state['report_filename_full'],
                mime="application/pdf"
        )

    filename_single = st.session_state.get('report_filename_single')
    if report_text_single and isinstance(filename_single, str) and filename_single.startswith('Relatorio de Analise do Ativo'):
        pdf_buffer_single, sucesso_single, mensagem_single = generate_pdf_in_memory(report_text_single)
        if sucesso_single:
            st.sidebar.download_button(
                label=f"💾 Análise do Ativo: {st.session_state.get('single_asset_ticker', 'PDF')} (PDF)",
                data=pdf_buffer_single.read(),
                file_name=st.session_state['report_filename_single'],
                mime="application/pdf"
            )