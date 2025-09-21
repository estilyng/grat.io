import streamlit as st
import pandas as pd
import sqlite3
from datetime import datetime
import io

# ---------------------------
# Grat.io - versão com upload,
# filtro por período e ações
# ---------------------------

DB_PATH = "grat_io.db"

st.set_page_config(page_title="Grat.io - Cálculo de Gratificação", layout="wide")

# ---------------------------
# Helpers de banco de dados
# ---------------------------

def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS atendimentos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                profissional_id TEXT,
                profissional TEXT,
                data TEXT,
                tipo TEXT,
                quantidade INTEGER,
                source_file TEXT,
                period TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS descontos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                profissional_id TEXT,
                period TEXT,
                campo TEXT,
                valor INTEGER,
                UNIQUE(profissional_id, period, campo)
            )
            """
        )
        conn.commit()


def save_atendimentos(df: pd.DataFrame, source_file: str = "uploaded"):
    if df is None or df.empty:
        return
    # normalize data types
    df_to_save = df.copy()
    if "data" in df_to_save.columns:
        df_to_save["data"] = pd.to_datetime(df_to_save["data"], errors="coerce").dt.strftime("%Y-%m-%d")
    df_to_save["source_file"] = getattr(df, "source_file", source_file) if "source_file" not in df_to_save.columns else df_to_save["source_file"]
    with sqlite3.connect(DB_PATH) as conn:
        df_to_save.to_sql("atendimentos", conn, if_exists="append", index=False)


def save_descontos(profissional_id: str, period: str, descontos_dict: dict):
    if not descontos_dict:
        return
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        for campo, valor in descontos_dict.items():
            # inserir ou substituir pelo par (profissional_id, period, campo)
            cur.execute(
                "INSERT OR REPLACE INTO descontos (profissional_id, period, campo, valor) VALUES (?, ?, ?, ?)",
                (profissional_id, period, campo, int(valor)),
            )
        conn.commit()


def load_atendimentos() -> pd.DataFrame:
    with sqlite3.connect(DB_PATH) as conn:
        try:
            df = pd.read_sql_query("SELECT * FROM atendimentos", conn)
        except Exception:
            df = pd.DataFrame(columns=["id", "profissional_id", "profissional", "data", "tipo", "quantidade", "source_file", "period"])
    if not df.empty:
        # ensure types
        if "quantidade" in df.columns:
            df["quantidade"] = pd.to_numeric(df["quantidade"], errors="coerce").fillna(0).astype(int)
        if "data" in df.columns:
            df["data"] = pd.to_datetime(df["data"], errors="coerce")
    return df


def load_descontos(profissional_id: str, period: str) -> dict:
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute("SELECT campo, valor FROM descontos WHERE profissional_id=? AND period=?", (profissional_id, period))
        rows = cur.fetchall()
    return {r[0]: r[1] for r in rows} if rows else {}


# ---------------------------
# Parser de relatórios (placeholder)
# - Se .xlsx tenta ler colunas padrão
# - Se .pdf usa um simulador (substituir por OCR/PDF parser real depois)
# ---------------------------


def try_find_column(df: pd.DataFrame, candidates):
    cols = list(df.columns)
    cols_low = [c.lower() for c in cols]
    for cand in candidates:
        if cand.lower() in cols_low:
            return cols[cols_low.index(cand.lower())]
    return None


def parse_xlsx(file) -> pd.DataFrame:
    # tenta ler a planilha assumindo colunas óbvias
    xls = pd.read_excel(file)
    df = xls.copy()
    # Procurar colunas prováveis
    col_prof = try_find_column(df, ["profissional", "nome", "médico", "medico", "professor"])
    col_data = try_find_column(df, ["data", "dt", "dia"])
    col_tipo = try_find_column(df, ["tipo", "atendimento", "procedimento", "descricao"])
    col_qtd = try_find_column(df, ["quantidade", "qtd", "total", "qte"])

    # Se não encontrar, tenta adivinhar por posição
    if col_prof is None and df.shape[1] >= 1:
        col_prof = df.columns[0]
    if col_data is None and df.shape[1] >= 2:
        col_data = df.columns[1]
    if col_tipo is None and df.shape[1] >= 3:
        col_tipo = df.columns[2]
    if col_qtd is None and df.shape[1] >= 4:
        col_qtd = df.columns[3]

    prepared = pd.DataFrame()
    prepared["profissional"] = df[col_prof].astype(str)
    prepared["data"] = pd.to_datetime(df[col_data], errors="coerce")
    prepared["tipo"] = df[col_tipo].astype(str)
    prepared["quantidade"] = pd.to_numeric(df[col_qtd], errors="coerce").fillna(0).astype(int)

    # separar id e nome se estiver no formato '3321 - NOME'
    def split_prof(p):
        if p is None:
            return (None, "")
        p = str(p).strip()
        if "-" in p:
            parts = p.split("-", 1)
            return parts[0].strip(), parts[1].strip()
        return (None, p)

    prof_split = prepared["profissional"].apply(split_prof)
    prepared["profissional_id"] = prof_split.apply(lambda x: x[0] if x and x[0] != "" else None)
    prepared["profissional_nome"] = prof_split.apply(lambda x: x[1] if x and x[1] != "" else None)

    # preferir nome extraído quando existir
    prepared["profissional"] = prepared.apply(lambda r: r["profissional_nome"] if r["profissional_nome"] not in (None, "") else r["profissional"], axis=1)
    prepared.drop(columns=["profissional_nome"], inplace=True)

    # period (YYYY-MM)
    prepared["period"] = prepared["data"].dt.to_period("M").astype(str)

    prepared = prepared[["profissional_id", "profissional", "data", "tipo", "quantidade", "period"]]
    prepared["source_file"] = getattr(file, "name", "uploaded")
    return prepared


def parse_pdf_placeholder(file) -> pd.DataFrame:
    # TODO: substituir por parser real (OCR / pdfminer / tabula etc.)
    # Usamos dados simulados para permitir testes da interface
    data = {
        "profissional_id": ["3321"] * 5,
        "profissional": ["3321 - ANDERSON HERMENGILDO LUIZ PINTO"] * 5,
        "data": pd.to_datetime(["2025-03-21", "2025-03-24", "2025-03-25", "2025-03-26", "2025-03-27"]),
        "tipo": ["Consulta", "Demanda espontânea", "Pediatria", "Pré Natal", "Visita domiciliar"],
        "quantidade": [20, 13, 6, 2, 2],
    }
    df = pd.DataFrame(data)
    df["period"] = df["data"].dt.to_period("M").astype(str)
    df["source_file"] = getattr(file, "name", "uploaded")
    return df


def parse_report(file) -> pd.DataFrame:
    name = getattr(file, "name", "uploaded")
    if name.lower().endswith(".xlsx"):
        try:
            return parse_xlsx(file)
        except Exception as e:
            st.warning(f"Erro lendo xlsx {name}, usando parser placeholder: {e}")
            return parse_pdf_placeholder(file)
    else:
        # pdf or unknown -> placeholder
        return parse_pdf_placeholder(file)


# ---------------------------
# Regras de pontuação (base PSF)
# ---------------------------
WEIGHTS_POSITIVOS = {
    "dias_25_vagas": 20,             # dias com agenda >= 25 vagas
    "dias_demanda_8": 10,            # dias com >= 8 demandas
    "visita_domiciliar": 8,          # por visita
    "semanas_encaminho_ate15": 100,  # por semana
    "semanas_encaminho_acima15": 300,# por semana
    "dias_lme": 15,
    "reunioes": 20,
    "capacitacoes": 30,
    "especialidades_basicas": 36,
    "dias_lancados_sistema": 10,
}

# Regras de desconto (conforme imagem fornecida)
WEIGHTS_NEGATIVOS = {
    "falta_meio": 300,
    "falta_dia": 400,
    "falta_aso": 0,
    "falta_injust": 1650,
    "nao_especialidade": 12,
    "nao_uso_sistema": 210,
    "nao_25_vagas": 1000,
    "recusa_atendimento": 1000,
}


# Mapear o texto do campo 'tipo' do relatório para critérios
def map_tipo_para_criterio(tipo_text: str) -> str:
    t = (tipo_text or "").lower()
    if "visita" in t:
        return "visita_domiciliar"
    if "pré" in t or "pre" in t:
        # diferenciar LME/receita de pré-natal por palavras-chave
        if "lme" in t or "receita" in t or "renov" in t:
            return "dias_lme"
        return "pre_natal"
    if "demanda" in t:
        return "dias_demanda_8"
    if any(x in t for x in ["pediatria", "ginecologia", "clínica", "clinica", "medicina", "hipertensão", "hipertensao", "diabetes"]):
        return "especialidades_basicas"
    if any(x in t for x in ["reunião", "reuniao", "reunioes", "reuniões"]):
        return "reunioes"
    if any(x in t for x in ["capacita", "curso"]):
        return "capacitacoes"
    if any(x in t for x in ["lme", "receita", "renovacao", "renovação"]):
        return "dias_lme"
    if any(x in t for x in ["lançado", "lancad", "maestro", "sistema"]):
        return "dias_lancados_sistema"
    # fallback: consultas/ outros
    if "consulta" in t:
        return "consulta"
    return "outros"


# Calcula pontos positivos a partir de um resumo por critério
def calcula_pontos_positivos_from_summary(summary_counts: dict) -> int:
    pts = 0
    for crit, cnt in summary_counts.items():
        if crit in WEIGHTS_POSITIVOS:
            pts += WEIGHTS_POSITIVOS[crit] * cnt
        # 'consulta' e 'outros' não somam por padrão (ajustar se necessário)
    return int(pts)


# Classificação por faixas
def classify_points(pontos_final: int) -> str:
    if 650 <= pontos_final <= 850:
        return "Tem direito - Gratificação Tipo I"
    if 851 <= pontos_final <= 950:
        return "Tem direito - Gratificação Tipo II"
    if 951 <= pontos_final <= 1050:
        return "Tem direito - Gratificação Tipo III"
    if 1051 <= pontos_final <= 1150:
        return "Tem direito - Gratificação Tipo IV"
    if pontos_final >= 1151:
        return "Tem direito - Gratificação Tipo V"
    return "Não tem direito"


# ---------------------------
# Inicialização DB
# ---------------------------
init_db()

# ---------------------------
# Interface Streamlit
# ---------------------------
st.title("📊 Grat.io - Cálculo de Gratificação")
st.markdown("Upload dos relatórios (sempre disponível)")

# garantir chaves de sessão
if "page" not in st.session_state:
    st.session_state["page"] = "lista"
if "view_prof" not in st.session_state:
    st.session_state["view_prof"] = None
if "view_period" not in st.session_state:
    st.session_state["view_period"] = None

uploaded_files = st.file_uploader("Envie os relatórios (xlsx ou pdf)", type=["xlsx", "pdf"], accept_multiple_files=True)

processed = False
if uploaded_files:
    all_parsed = []
    for f in uploaded_files:
        df_parsed = parse_report(f)
        # padroniza colunas para salvar
        if not df_parsed.empty:
            # garantir colunas
            expected_cols = ["profissional_id", "profissional", "data", "tipo", "quantidade", "period", "source_file"]
            for c in expected_cols:
                if c not in df_parsed.columns:
                    df_parsed[c] = None
            # salvar no DB
            try:
                save_atendimentos(df_parsed[["profissional_id", "profissional", "data", "tipo", "quantidade", "period", "source_file"]])
            except Exception as e:
                st.warning(f"Falha ao salvar no DB via to_sql; tentando salvar linha a linha: {e}")
                with sqlite3.connect(DB_PATH) as conn:
                    df_parsed.to_sql("atendimentos", conn, if_exists="append", index=False)
            all_parsed.append(df_parsed)
    processed = True
    st.success(f"{len(all_parsed)} arquivo(s) processado(s) e salvos.")

# Carregar dados do banco (tudo o que já foi processado)
all_data = load_atendimentos()

if all_data.empty:
    st.info("Nenhum relatório processado ainda. Faça upload para começar (use o painel acima).")
else:
    # periodos disponíveis
    periods = sorted(all_data["period"].dropna().unique())
    if not periods:
        st.info("Não há períodos válidos nos dados. Verifique os uploads.")
    else:
        selected_period = st.selectbox("Filtrar por período (mês)", periods, index=len(periods) - 1)

        # filtrar dados para o período
        data_period = all_data[all_data["period"] == selected_period]

        # agrupar por profissional e mapear critérios
        def resumo_por_profissional(df):
            grouped = df.groupby(["profissional_id", "profissional"], dropna=False)
            rows = []
            for (pid, pnome), g in grouped:
                # contar por tipo mapeado
                crit_counts = {}
                for _, r in g.iterrows():
                    crit = map_tipo_para_criterio(r["tipo"])
                    crit_counts[crit] = crit_counts.get(crit, 0) + int(r["quantidade"]) if pd.notnull(r["quantidade"]) else crit_counts.get(crit, 0)

                pontos_pos = calcula_pontos_positivos_from_summary(crit_counts)
                # carregar descontos já salvos
                key_id = pid if pid not in (None, "", "None") else pnome
                descontos_salvos = load_descontos(key_id, selected_period)
                pontos_neg = sum([v for v in descontos_salvos.values()]) if descontos_salvos else 0

                pontos_final = pontos_pos - pontos_neg
                classific = classify_points(pontos_final)

                row = {
                    "profissional_id": key_id,
                    "profissional": pnome if pnome not in (None, "", "None") else key_id,
                    "crit_counts": crit_counts,
                    "pontos_positivos": pontos_pos,
                    "pontos_negativos": pontos_neg,
                    "pontos_finais": pontos_final,
                    "classificacao": classific,
                }
                rows.append(row)
            if rows:
                return pd.DataFrame(rows)
            return pd.DataFrame(columns=["profissional_id", "profissional", "crit_counts", "pontos_positivos", "pontos_negativos", "pontos_finais", "classificacao"])

        df_summary = resumo_por_profissional(data_period)

        st.subheader("📋 Resumo por Profissional")
        if df_summary.empty:
            st.info("Nenhum dado disponível para o período selecionado.")
        else:
            display_df = df_summary[["profissional_id", "profissional", "pontos_finais", "classificacao"]].copy()
            display_df = display_df.rename(columns={"profissional_id": "ID", "profissional": "Profissional", "pontos_finais": "Pontos Finais", "classificacao": "Classificação"})
            st.dataframe(display_df, use_container_width=True)

            st.markdown("---")
            st.markdown("### Ações")
            # lista compacta com botões de ação por linha (coluna Ações)
            for idx, row in df_summary.iterrows():
                cols = st.columns([6, 1])
                cols[0].markdown(f"**{row['profissional']}** — Pontos: **{row['pontos_finais']}** — {row['classificacao']}")
                if cols[1].button("Detalhes", key=f"det_{row['profissional_id']}_{selected_period}"):
                    st.session_state["view_prof"] = row["profissional_id"]
                    st.session_state["view_period"] = selected_period
                    st.session_state["page"] = "detalhe"
                    st.rerun()

# ---------------------------
# Página de detalhe (quando acionada)
# ---------------------------
if st.session_state.get("page") == "detalhe":
    prof_id = st.session_state.get("view_prof")
    period = st.session_state.get("view_period")
    # recuperar registros do periodo
    df_prof = all_data[(all_data["profissional_id"] == prof_id) | (all_data["profissional"] == prof_id)]
    df_prof = df_prof[df_prof["period"] == period]
    if df_prof.empty:
        st.warning("Dados do profissional não encontrados para o período.")
    else:
        st.header(f"Detalhes: {prof_id} — Período: {period}")
        st.dataframe(df_prof[["data", "tipo", "quantidade", "source_file"]], use_container_width=True)

        # calcular resumo por critério
        crit_counts = {}
        for _, r in df_prof.iterrows():
            crit = map_tipo_para_criterio(r["tipo"])
            crit_counts[crit] = crit_counts.get(crit, 0) + int(r["quantidade"]) if pd.notnull(r["quantidade"]) else crit_counts.get(crit, 0)

        st.subheader("✅ Pontos Positivos (contagens)")
        st.json(crit_counts)

        pontos_pos = calcula_pontos_positivos_from_summary(crit_counts)
        st.write(f"**Pontos positivos (calculados):** {pontos_pos}")

        # carregar descontos salvos e permitir edição
        descontos_salvos = load_descontos(prof_id, period) or {}

        st.subheader("❌ Pontos Negativos (ajustáveis)")
        # criar inputs para cada tipo de desconto; usamos ocorrência -> valor calculado
        edits = {}
        for campo, peso in WEIGHTS_NEGATIVOS.items():
            saved_val = int(descontos_salvos.get(campo, 0))
            initial_count = saved_val // peso if peso and peso > 0 else saved_val
            count = st.number_input(f"{campo} (ocorrências) — peso {peso}", min_value=0, value=int(initial_count), step=1, key=f"neg_cnt_{campo}")
            computed_val = int(count) * int(peso)
            st.write(f"Valor calculado para {campo}: {computed_val}")
            edits[campo] = computed_val

        if st.button("Salvar descontos para este profissional"):
            save_descontos(prof_id, period, edits)
            st.success("Descontos salvos com sucesso!")
            st.rerun()

        pontos_neg = sum(edits.values())
        pontos_final = pontos_pos - pontos_neg
        classific = classify_points(pontos_final)

        st.subheader("📌 Resultado Final")
        if classific == "Não tem direito":
            st.error(f"❌ NÃO tem direito | Pontuação: {pontos_final}")
        else:
            st.success(f"✅ TEM direito | {classific} | Pontuação: {pontos_final}")

        if st.button("⬅️ Voltar"):
            st.session_state["page"] = "lista"
            st.rerun()

# ---------------------------
# Developer tests (button)
# ---------------------------
with st.expander("🔧 Developer tests (executar)"):
    if st.button("Executar testes internos"):
        st.write("-> Teste: cálculo de pontos positivos a partir de contagens de exemplo")
        sample_counts = {"dias_25_vagas": 10, "dias_demanda_8": 5, "visita_domiciliar": 3, "especialidades_basicas": 2}
        st.write("Contagens de exemplo:", sample_counts)
        pts = calcula_pontos_positivos_from_summary(sample_counts)
        st.write("Pontos calculados:", pts)
        st.write("Classificação para esse total:", classify_points(pts))

        st.write("-> Teste: parse placeholder (PDF)")
        sample_pdf_df = parse_pdf_placeholder(type("F", (), {"name": "sample.pdf"})())
        st.dataframe(sample_pdf_df)

        st.success("Testes concluídos")
