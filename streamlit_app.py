# [streamlit_app.py]
import streamlit as st
import portfolio # Seu módulo de lógica
import pandas as pd
import io 
import os
import tempfile
import time # Para simular carregamento

# --------------------------------------------------------------------
# 1. FUNÇÕES DE ESTADO E RECURSOS (CACHING)
# --------------------------------------------------------------------

# Usa st.cache_resource para manter a conexão com o banco de dados ativa
# entre as execuções (útil para SQLite em um ambiente serverless)
@st.cache_resource
def get_db_connection():
    # Inicializa a conexão com o banco de dados
    portfolio.conectar_db()
    return True # Retorna um indicador de sucesso

# Função para buscar os dados da carteira
@st.cache_data(show_spinner=False)
def get_portfolio_data():
    return portfolio.listar_ativos_em_df()

# --------------------------------------------------------------------
# 2. INTERFACE E LAYOUT PRINCIPAL
# --------------------------------------------------------------------

# Inicialização da Conexão (Executada uma vez)
get_db_connection()

st.set_page_config(page_title="Analisador de Portfólio AI", layout="wide")
st.title("🛡️ Analisador de Portfólio AI")

# --- BARRA LATERAL (Sidebar) para ações e inputs ---
st.sidebar.header("Ações da Carteira")

# Adicionar Ativo
with st.sidebar.form("form_adicionar_ativo", clear_on_submit=True):
    st.subheader("Adicionar Novo Ativo")
    ticker = st.text_input("Ticker do Ativo (ex: AAPL)", max_chars=10).upper()
    peso = st.number_input("Peso na Carteira (%)", min_value=0.0, max_value=100.0, step=0.1)
    if st.form_submit_button("Adicionar"):
        if ticker and peso > 0:
            sucesso, mensagem = portfolio.adicionar_ativo(ticker, peso)
            if sucesso:
                st.sidebar.success(f"Ativo {ticker} adicionado com sucesso.")
            else:
                st.sidebar.error(f"Erro ao adicionar {ticker}: {mensagem}")
            # Invalida o cache para forçar a atualização da tabela
            st.cache_data.clear() 

# Importar Ativos (Substitui o filedialog do Tkinter)
uploaded_file = st.sidebar.file_uploader("Importar ativos (.txt ou .csv)", type=['txt', 'csv'])
if uploaded_file is not None:
    # Salvar o arquivo temporariamente (necessário para o portfolio.py)
    with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
        tmp_file.write(uploaded_file.read())
        temp_path = tmp_file.name

    with st.spinner("Aguarde, importando dados..."):
        sucesso, mensagem = portfolio.importar_ativos_do_arquivo(temp_path)
    
    os.unlink(temp_path) # Deleta o arquivo temporário
    st.cache_data.clear()

    if sucesso:
        st.sidebar.success(f"Importação Concluída: {mensagem}")
    else:
        st.sidebar.error(f"Erro na Importação: {mensagem}")

# Botões de Ação Global
if st.sidebar.button("Atualizar Dados Fundamentalistas"):
    st.cache_data.clear() # Limpa o cache para buscar novos dados
    with st.spinner("Atualizando dados..."):
        sucesso, mensagem = portfolio.atualizar_dados_fundamentalistas_carteira()
    if sucesso:
        st.sidebar.success("Dados fundamentalistas atualizados.")
    else:
        st.sidebar.error("Erro ao atualizar dados. Verifique o log.")

if st.sidebar.button("Limpar Carteira"):
    if st.sidebar.confirm("Tem certeza que deseja limpar todos os ativos da carteira?"):
        portfolio.limpar_carteira()
        st.cache_data.clear()
        st.sidebar.warning("Carteira limpa.")

# --- SEÇÃO PRINCIPAL (Exibição e Análise) ---

st.header("Carteira Atual")
df_carteira = get_portfolio_data()

if df_carteira.empty:
    st.info("Sua carteira está vazia. Adicione ativos na barra lateral.")
else:
    # Exibe a tabela com a carteira (substitui Treeview do Tkinter)
    st.dataframe(df_carteira, use_container_width=True)
    
    # --------------------------------------------------------------------
    # 3. GERAÇÃO DO RELATÓRIO E ANÁLISE IA
    # --------------------------------------------------------------------
    
    st.header("Geração de Relatório de Análise")
    
    # Botão de Análise (Ação principal)
    if st.button("Gerar Análise Completa da Carteira (AI)"):
        with st.spinner("🤖 Analisando carteira e gerando relatório via Google AI..."):
            # 1. Gerar Análise JSON
            analise_json, mensagem = portfolio.gerar_analise_ia_carteira(df_carteira)
            
            if analise_json:
                # 2. Formatar JSON para Texto (Markdown)
                report_text = portfolio.formatar_analise_para_texto(analise_json)
                st.session_state['report_text'] = report_text
                st.session_state['report_filename'] = "Relatorio_Carteira.pdf"
                
                st.success("Análise da IA concluída! Veja o relatório abaixo.")
                
                # 3. Exibir o relatório em formato Markdown
                st.subheader("Prévia do Relatório")
                st.markdown(report_text, unsafe_allow_html=False)
                
            else:
                st.error(f"Erro na análise da IA: {mensagem}")

# --------------------------------------------------------------------
# 4. DOWNLOAD (Substitui o exportar_para_pdf com messagebox)
# --------------------------------------------------------------------

# Verifica se o relatório foi gerado (armazenado em session_state)
if 'report_text' in st.session_state:
    
    # Gerar PDF em memória (Necessário para a Nuvem)
    # Requer ajuste na função exportar_para_pdf para aceitar um BytesIO
    @st.cache_data(show_spinner=False)
    def generate_pdf_in_memory(report_text):
        """Adapta a função exportar_para_pdf para retornar um BytesIO."""
        buffer = io.BytesIO()
        sucesso, mensagem = portfolio.exportar_para_pdf(buffer, report_text)
        buffer.seek(0)
        return buffer, sucesso, mensagem

    pdf_buffer, sucesso, mensagem = generate_pdf_in_memory(st.session_state['report_text'])
    
    if sucesso:
        st.download_button(
            label="💾 Baixar Relatório PDF",
            data=pdf_buffer.read(),
            file_name=st.session_state['report_filename'],
            mime="application/pdf"
        )
    else:
        st.error(f"Erro ao gerar PDF: {mensagem}")