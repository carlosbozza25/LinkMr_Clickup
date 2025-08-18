# main.py
import re
import io
import requests
import pandas as pd
import streamlit as st
import altair as alt
from datetime import date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- utils de formatação simples ---
def fmt_days_short(x):
    """Converte dias (float) para texto curto: min / h / d."""
    if x is None or (hasattr(pd, "isna") and pd.isna(x)):
        return "—"
    try:
        d = float(x)
    except Exception:
        return "—"
    if d < (1/24/2):  # < ~30min
        m = int(round(d*24*60))
        return f"{m} min"
    if d < 1:  # < 1 dia
        h = round(d*24, 1)
        h_int = int(h)
        return f"{h_int} h" if abs(h - h_int) < 0.1 else f"{h} h"
    return f"{round(d, 2)} d"

# --- PDF (reportlab) ---
try:
    from reportlab.lib.pagesizes import A4
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib import colors
    from reportlab.lib.units import cm
    from reportlab.lib.utils import ImageReader
    REPORTLAB_AVAILABLE = True
except Exception:
    REPORTLAB_AVAILABLE = False

# ===================== CONFIG (hardcode) =====================
GITLAB_URL       = "https://gitlab.com"
# 🔐 mantenha hard-code aqui se preferir:
PRIVATE_TOKEN    = "glpat-Pd-35OPXUznRPR92BjTJU286MQp1OmZvdjdsCw.01.1212gil0j"
# Grupos (fullPath) a incluir
GROUP_FULL_PATHS = [
    "azulsystems/app-health",
    "azulsystems/azul-controle",
]

# ===================== PAGE / THEME =====================
st.set_page_config(page_title="GitLab MR's Dashboard", layout="wide")
st.title("📊 GitLab MR Dashboard")

# ===================== GLOBAL DARK CSS =====================
st.markdown("""
<style>
:root {
  --bg: #0b1220; --panel: #111827; --muted: #1f2937; --muted-2: #0f172a;
  --text: #e5e7eb; --subtle: #94a3b8; --accent: #22d3ee; --accent-2: #60a5fa;
  --success: #34d399; --danger: #f87171; --shadow: rgba(0,0,0,.35);
}
html, body, .stApp, .block-container { background: var(--bg) !important; color: var(--text) !important; }
a { color: var(--accent-2) !important; text-decoration: none; } a:hover { text-decoration: underline; }
section[data-testid="stSidebar"] { background: #0c1426 !important; }
.card { border-radius: 14px; padding: 14px 16px; color: var(--text);
  background: var(--panel); border: 1px solid #243045; box-shadow: 0 6px 12px var(--shadow); }
.card h3 { font-size: 0.9rem; margin: 0; color: var(--subtle); font-weight: 600; }
.card .num { font-size: 1.8rem; font-weight: 800; margin-top: 6px; }
.card.opened { background: #0a2b33; border-color: #134e4a; }
.card.merged { background: #0b2e22; border-color: #166534; }
.card.closed { background: #33151a; border-color: #7f1d1d; }
.card.wip { background: #121a2a; border-color: #1f2a44; }
.card.wip h4 { margin: 0; font-size: .85rem; color: var(--subtle); }
.card.wip .num { font-size: 1.4rem; margin-top: 6px; }
.mr-table thead th { text-align:center !important; background: var(--muted); color: var(--text); position: sticky; top: 0; z-index:1; }
.mr-table tbody td { padding: 8px 10px; font-size: 0.95rem; color: var(--text); }
.mr-table tbody tr:nth-child(odd){ background: var(--muted-2); }
.mr-table tbody tr:nth-child(even){ background: #0c1629; }
.mr-table tbody td.center { text-align:center; }
hr, .st-emotion-cache-hr { border-color: #1f2937 !important; }
</style>
""", unsafe_allow_html=True)

# ===================== SIDEBAR (Filtros) =====================
st.sidebar.header("🔎 Filtros")
today = date.today()
first_day = today.replace(day=1)

date_range = st.sidebar.date_input(
    "Intervalo (DD/MM/AAAA)",
    value=(first_day, today),
    format="DD/MM/YYYY",
    help="Por padrão, do 1º dia do mês até hoje."
)
if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
    start_date, end_date = date_range
else:
    start_date, end_date = first_day, today

author_filter = st.sidebar.text_input("Autor (contém)")
title_filter  = st.sidebar.text_input("Título (contém)")
states = st.sidebar.multiselect(
    "Estados",
    options=["opened", "merged", "closed"],
    default=["opened", "merged", "closed"]
)

st.sidebar.markdown("---")
fetch_commits = st.sidebar.checkbox(
    "Incluir ranking de commits",
    value=False,
    help="Usa múltiplas chamadas REST paralelas. Marque somente quando precisar."
)
# ✅ novo: visão de equipe (usa REST para commits e tamanho)
fetch_team = st.sidebar.checkbox(
    "Gerar visão de equipe (gargalos) + tamanho do MR",
    value=False,
    help="Agrega por autor: volume, merge rate, lead time, janela de código (commits) e tamanho do MR (linhas alteradas)."
)

# ===================== HELPERS =====================
GRAPHQL_ENDPOINT = f"{GITLAB_URL}/api/graphql"
REST_API_BASE    = f"{GITLAB_URL}/api/v4"
HEADERS_GQL = {"Authorization": f"Bearer {PRIVATE_TOKEN}", "Content-Type": "application/json"}
HEADERS_REST = {"PRIVATE-TOKEN": PRIVATE_TOKEN}

def parse_project_numeric_id(gid: str):
    m = re.search(r"Project/(\d+)$", gid or "")
    return int(m.group(1)) if m else None

GQL_QUERY = """
query ($fullPath: ID!, $after: String, $ca: Time, $cb: Time) {
  group(fullPath: $fullPath) {
    mergeRequests(
      first: 100,
      after: $after,
      includeSubgroups: true,
      createdAfter: $ca,
      createdBefore: $cb
    ) {
      pageInfo { endCursor hasNextPage }
      nodes {
        iid
        title
        state
        createdAt
        mergedAt
        webUrl
        author { username }
        project { id fullPath }
      }
    }
  }
}
"""

@st.cache_data(show_spinner=False, ttl=300)
def fetch_mrs(created_after: str, created_before: str):
    all_nodes = []
    session = requests.Session()
    for full_path in GROUP_FULL_PATHS:
        after_cursor = None
        while True:
            payload = {
                "query": GQL_QUERY,
                "variables": {"fullPath": full_path, "after": after_cursor, "ca": created_after, "cb": created_before}
            }
            r = session.post(GRAPHQL_ENDPOINT, json=payload, headers=HEADERS_GQL, timeout=30)
            if not r.ok:
                break
            try:
                j = r.json()
            except Exception:
                break
            if "errors" in j and j["errors"]:
                break
            data = j.get("data", {})
            group = data.get("group")
            if not group:
                break
            mrs = group["mergeRequests"]
            nodes = mrs.get("nodes", [])
            all_nodes.extend(nodes)
            pg = mrs.get("pageInfo", {})
            if pg.get("hasNextPage"):
                after_cursor = pg.get("endCursor")
            else:
                break
    return all_nodes

def rest_list_commits(session: requests.Session, project_id: int, iid: int):
    out = []; page = 1; per_page = 100
    while True:
        url = f"{REST_API_BASE}/projects/{project_id}/merge_requests/{iid}/commits"
        resp = session.get(url, headers=HEADERS_REST, params={"page": page, "per_page": per_page}, timeout=30)
        if not resp.ok: break
        batch = resp.json()
        if not batch: break
        out.extend(batch)
        if len(batch) < per_page: break
        page += 1
    return out

def rest_list_notes(session: requests.Session, project_id: int, iid: int):
    out = []; page = 1; per_page = 100
    while True:
        url = f"{REST_API_BASE}/projects/{project_id}/merge_requests/{iid}/notes"
        resp = session.get(url, headers=HEADERS_REST, params={"page": page, "per_page": per_page}, timeout=30)
        if not resp.ok: break
        batch = resp.json()
        if not batch: break
        out.extend(batch)
        if len(batch) < per_page: break
        page += 1
    # filtra comentários de pessoas (não system)
    out = [n for n in out if not n.get("system")]
    return out

# ========= NOVO: /changes → tamanho do MR (linhas + / -) =========
def rest_mr_changes(session: requests.Session, project_id: int, iid: int):
    url = f"{REST_API_BASE}/projects/{project_id}/merge_requests/{iid}/changes"
    resp = session.get(url, headers=HEADERS_REST, timeout=60)
    if not resp.ok:
        return {}
    try:
        return resp.json()
    except Exception:
        return {}

def _count_added_removed_from_diff_text(diff_text: str):
    added = removed = 0
    if not isinstance(diff_text, str):
        return 0, 0
    for line in diff_text.splitlines():
        if not line:
            continue
        # ignora cabeçalhos do patch
        if line.startswith("diff --git") or line.startswith("index ") or line.startswith("@@ "):
            continue
        if line.startswith("+++ ") or line.startswith("--- "):
            continue
        # conta adições/remoções reais
        if line.startswith("+"):
            added += 1
        elif line.startswith("-"):
            removed += 1
    return added, removed

@st.cache_data(show_spinner=False, ttl=300)
def compute_mr_size_stats(pairs_key):
    """Para cada MR, calcula linhas adicionadas/remoções e total (a partir de /changes)."""
    if not pairs_key:
        return pd.DataFrame(columns=["project_id","iid","lines_added","lines_removed","lines_total"])
    session = requests.Session()
    rows = []
    max_workers = min(16, max(4, len(pairs_key)//2))
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(rest_mr_changes, session, pid, iid): (pid, iid) for (pid, iid) in pairs_key}
        for fut in as_completed(futures):
            pid, iid = futures[fut]
            lines_added = lines_removed = 0
            try:
                data = fut.result() or {}
                # GitLab retorna "changes": [ { diff: "..."} , ... ]
                changes = data.get("changes") or data.get("diffs") or []
                for ch in changes:
                    diff_text = ch.get("diff")
                    a, r = _count_added_removed_from_diff_text(diff_text)
                    lines_added += a
                    lines_removed += r
            except Exception:
                pass
            rows.append({
                "project_id": int(pid), "iid": int(iid),
                "lines_added": int(lines_added),
                "lines_removed": int(lines_removed),
                "lines_total": int(lines_added + lines_removed)
            })
    return pd.DataFrame(rows)

# ✅ NOVO: tempos de dev por MR (commits)
@st.cache_data(show_spinner=False, ttl=300)
def compute_dev_times(pairs_key, created_map):
    """Calcula, por MR, a janela de código (first->last commit), tempos pré/pós-abertura e quantidade de commits."""
    if not pairs_key:
        return pd.DataFrame(columns=[
            "project_id","iid","first_commit","last_commit","commits_count",
            "dev_pre_open_days","post_open_coding_days","coding_window_days"
        ])
    session = requests.Session()
    rows = []
    max_workers = min(16, max(4, len(pairs_key)//2))
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(rest_list_commits, session, pid, iid): (pid, iid) for (pid, iid) in pairs_key}
        for fut in as_completed(futures):
            pid, iid = futures[fut]
            created_at = created_map.get((pid, iid))
            try:
                commits = fut.result() or []
            except Exception:
                commits = []
            ts_list = []
            for c in commits:
                t = c.get("authored_date") or c.get("committed_date") or c.get("created_at")
                ts = pd.to_datetime(t, utc=True, errors="coerce")
                if pd.notna(ts):
                    ts_list.append(ts)
            first_ts = min(ts_list) if ts_list else pd.NaT
            last_ts  = max(ts_list) if ts_list else pd.NaT

            window_days = (last_ts - first_ts).total_seconds()/86400.0 if (pd.notna(first_ts) and pd.notna(last_ts)) else None
            pre_open    = (created_at - first_ts).total_seconds()/86400.0 if (pd.notna(first_ts) and pd.notna(created_at)) else None
            post_open   = (last_ts - created_at).total_seconds()/86400.0 if (pd.notna(last_ts) and pd.notna(created_at)) else None

            pre_open  = max(0.0, pre_open)  if pre_open  is not None else None
            post_open = max(0.0, post_open) if post_open is not None else None

            rows.append({
                "project_id": int(pid), "iid": int(iid),
                "first_commit": first_ts, "last_commit": last_ts,
                "commits_count": int(len(commits)),
                "dev_pre_open_days": round(pre_open, 2) if pre_open is not None else None,
                "post_open_coding_days": round(post_open, 2) if post_open is not None else None,
                "coding_window_days": round(window_days, 2) if window_days is not None else None,
            })
    return pd.DataFrame(rows)

@st.cache_data(show_spinner=False, ttl=300)
def compute_commit_ranking(pairs_key):
    if not pairs_key:
        return pd.DataFrame(columns=["Autor", "Qtd Commits"])
    session = requests.Session()
    counts = {}
    max_workers = min(16, max(4, len(pairs_key)//2))
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(rest_list_commits, session, pid, iid): (pid, iid) for (pid, iid) in pairs_key}
        for fut in as_completed(futures):
            try:
                commits = fut.result()
                for c in commits:
                    name = c.get("author_name") or "desconhecido"
                    counts[name] = counts.get(name, 0) + 1
            except Exception:
                continue
    if not counts:
        return pd.DataFrame(columns=["Autor", "Qtd Commits"])
    return (
        pd.DataFrame([{"Autor": k, "Qtd Commits": v} for k, v in counts.items()])
        .sort_values("Qtd Commits", ascending=False, kind="mergesort")
        .reset_index(drop=True)
    )

@st.cache_data(show_spinner=False, ttl=300)
def compute_reviewers_ranking(pairs_key):
    """Conta comentários por autor em MRs (quem mais revisou)."""
    if not pairs_key:
        return pd.DataFrame(columns=["Revisor", "Qtd Comentários"])
    session = requests.Session()
    counts = {}
    max_workers = min(16, max(4, len(pairs_key)//2))
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(rest_list_notes, session, pid, iid): (pid, iid) for (pid, iid) in pairs_key}
        for fut in as_completed(futures):
            try:
                notes = fut.result()
                for n in notes:
                    user = (n.get("author") or {}).get("username", "desconhecido")
                    counts[user] = counts.get(user, 0) + 1
            except Exception:
                continue
    if not counts:
        return pd.DataFrame(columns=["Revisor", "Qtd Comentários"])
    return (
        pd.DataFrame([{"Revisor": k, "Qtd Comentários": v} for k, v in counts.items()])
        .sort_values("Qtd Comentários", ascending=False, kind="mergesort")
        .reset_index(drop=True)
    )

# ===================== FETCH & DATAFRAME =====================
created_after  = f"{start_date.isoformat()}T00:00:00Z"
created_before = f"{end_date.isoformat()}T23:59:59Z"

with st.spinner("🔄 Coletando MRs (GraphQL)..."):
    raw_nodes = fetch_mrs(created_after, created_before)

if not raw_nodes:
    st.warning("❗ Nenhum MR encontrado com esses filtros (ou sem acesso aos grupos).")
    st.stop()

records = []
for n in raw_nodes:
    proj_gid = n["project"]["id"] if n.get("project") else None
    proj_id  = parse_project_numeric_id(proj_gid)
    records.append({
        "project_full_path": n["project"]["fullPath"] if n.get("project") else "",
        "project_id": proj_id,
        "iid": int(n["iid"]),
        "title": n.get("title") or "",
        "author": (n.get("author") or {}).get("username") or "",
        "state": (n.get("state") or "").lower(),
        "created_at": n.get("createdAt"),
        "merged_at": n.get("mergedAt"),
        "web_url": n.get("webUrl"),
    })

df = pd.DataFrame(records)
df["created_at"] = pd.to_datetime(df["created_at"], utc=True, errors="coerce")
df["merged_at"]  = pd.to_datetime(df["merged_at"],  utc=True, errors="coerce")

# 🔹 micro = último segmento do caminho do projeto
df["micro"] = df["project_full_path"].fillna("").apply(lambda s: s.split("/")[-1] if s else "")

# aplica filtros
if author_filter:
    df = df[df["author"].str.contains(author_filter, case=False, na=False)]
if title_filter:
    df = df[df["title"].str.contains(title_filter, case=False, na=False)]
if states:
    df = df[df["state"].isin(states)]

if df.empty:
    st.warning("❗ Nenhum MR encontrado após aplicar os filtros.")
    st.stop()

# ===================== KPIs / SLA =====================
k_total   = len(df)
k_opened  = int((df["state"] == "opened").sum())
k_merged  = int((df["state"] == "merged").sum())
k_closed  = int((df["state"] == "closed").sum())

colA, colB, colC, colD = st.columns(4)
with colA: st.markdown(f'<div class="card"><h3>Total</h3><div class="num">{k_total}</div></div>', unsafe_allow_html=True)
with colB: st.markdown(f'<div class="card opened"><h3>Abertos</h3><div class="num">{k_opened}</div></div>', unsafe_allow_html=True)
with colC: st.markdown(f'<div class="card merged"><h3>Mergeados</h3><div class="num">{k_merged}</div></div>', unsafe_allow_html=True)
with colD: st.markdown(f'<div class="card closed"><h3>Fechados</h3><div class="num">{k_closed}</div></div>', unsafe_allow_html=True)

st.caption(
    f"Período: **{start_date.strftime('%d/%m/%Y')}** → **{end_date.strftime('%d/%m/%Y')}**"
    + (f" • Autor contém: **{author_filter}**" if author_filter else "")
    + (f" • Título contém: **{title_filter}**" if title_filter else "")
    + (f" • Estados: **{', '.join(states)}**" if states else " • Estados: todos")
)

st.markdown("---")

# ===================== EXPORTAÇÕES & RELATÓRIOS =====================
st.subheader("⬇️ Exportações & Relatórios")
exp1, exp2 = st.columns([1,1])

# ---- CSV (período/filtros atuais) ----
with exp1:
    st.markdown("**CSV dos MRs (filtros atuais)**")
    df_export = df.copy()
    df_export["created_at"] = df_export["created_at"].dt.tz_convert("UTC").dt.strftime("%Y-%m-%d %H:%M:%S").fillna("")
    df_export["merged_at"]  = df_export["merged_at"].dt.tz_convert("UTC").dt.strftime("%Y-%m-%d %H:%M:%S").fillna("")
    csv_cols = ["project_full_path","project_id","iid","title","author","state","created_at","merged_at","web_url"]
    csv_bytes = df_export[csv_cols].to_csv(index=False).encode("utf-8-sig")
    st.download_button(
        "Exportar CSV",
        data=csv_bytes,
        file_name=f"mrs_{date.today().strftime('%Y%m%d')}.csv",
        mime="text/csv",
        use_container_width=True
    )

# ---- PDF (Resumo para apresentação) ----
with exp2:
    st.markdown("**PDF — Resumo (Dark)**")
    pdf_period = st.selectbox("Período do PDF", ["Últimos 7 dias", "Período atual"], index=0)
    pdf_logo = st.file_uploader("Logotipo (PNG/JPG, opcional)", type=["png","jpg","jpeg"])
    include_highlights = st.checkbox("Incluir destaques", value=True)  # (não usado; mantém compat)
    submit_pdf = st.button("Gerar PDF", use_container_width=True)

    def build_pdf_summary(df_src: pd.DataFrame, label_periodo: str, logo_bytes: bytes | None, _unused: dict | None):
        # --- cores dark ---
        BG = colors.HexColor("#0b1220")
        TXT = colors.HexColor("#e5e7eb")
        MUTED = colors.HexColor("#94a3b8")
        BORDER = colors.HexColor("#243045")
        HEAD_BG = colors.HexColor("#111827")
        ACCENT_HEX = "#22d3ee"
        ACCENT = colors.HexColor(ACCENT_HEX)

        if "micro" not in df_src.columns:
            df_src = df_src.copy()
            df_src["micro"] = df_src["project_full_path"].fillna("").apply(lambda s: s.split("/")[-1] if s else "")

        def draw_bg(canvas, doc):
            canvas.saveState()
            canvas.setFillColor(BG)
            canvas.rect(0, 0, doc.pagesize[0], doc.pagesize[1], stroke=0, fill=1)
            canvas.setFillColor(HEAD_BG)
            canvas.rect(0, doc.pagesize[1] - 70, doc.pagesize[0], 70, stroke=0, fill=1)
            if logo_bytes:
                try:
                    img = ImageReader(io.BytesIO(logo_bytes))
                    canvas.drawImage(img, 2 * cm, doc.pagesize[1] - 60, width=80, height=40, mask='auto',
                                     preserveAspectRatio=True, anchor='nw')
                except Exception:
                    pass
            canvas.setFillColor(TXT)
            canvas.setFont("Helvetica-Bold", 16)
            title = "Resumo Semanal de MRs" if "7 dias" in label_periodo else "Resumo de MRs"
            x_title = 2 * cm + (90 if logo_bytes else 0)
            canvas.drawString(x_title, doc.pagesize[1] - 40, title)
            canvas.setFont("Helvetica", 10)
            canvas.setFillColor(MUTED)
            canvas.drawString(x_title, doc.pagesize[1] - 55, label_periodo)
            canvas.setFillColor(MUTED)
            canvas.setFont("Helvetica", 9)
            canvas.drawCentredString(doc.pagesize[0] / 2, 1.0 * cm, f"Página {canvas.getPageNumber()}")
            canvas.restoreState()

        buf = io.BytesIO()
        doc = SimpleDocTemplate(buf, pagesize=A4, leftMargin=2 * cm, rightMargin=2 * cm, topMargin=2.5 * cm, bottomMargin=1.5 * cm)
        styles = getSampleStyleSheet()
        styles.add(ParagraphStyle(name="H2Dark", fontSize=12, leading=16, spaceAfter=6, textColor=TXT))
        styles.add(ParagraphStyle(name="BodyDark", fontSize=10, leading=14, textColor=TXT))
        styles.add(ParagraphStyle(name="SmallMuted", fontSize=9, leading=12, textColor=MUTED))

        def P(txt: str): return Paragraph(txt, styles["BodyDark"])
        def link_paragraph(url: str, label: str = "abrir"):
            return Paragraph(f'<font color="{ACCENT_HEX}"><a href="{url}">{label}</a></font>', styles["BodyDark"])

        elements = [Spacer(1, 40)]

        # KPIs
        k_total, k_opened, k_merged, k_closed = len(df_src), int((df_src["state"] == "opened").sum()), int((df_src["state"] == "merged").sum()), int((df_src["state"] == "closed").sum())
        kpi_data = [["Total", str(k_total), "Abertos", str(k_opened)], ["Mergeados", str(k_merged), "Fechados", str(k_closed)]]
        kpi_tbl = Table(kpi_data, colWidths=[3 * cm, 3 * cm, 3 * cm, 3 * cm])
        kpi_tbl.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, -1), HEAD_BG), ("TEXTCOLOR", (0, 0), (-1, -1), TXT),
            ("BOX", (0, 0), (-1, -1), 0.5, BORDER), ("INNERGRID", (0, 0), (-1, -1), 0.25, BORDER),
            ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ]))
        elements += [Paragraph("Indicadores", styles["H2Dark"]), kpi_tbl, Spacer(1, 10)]

        # Resumo geral — Autores
        elements.append(Paragraph("Resumo geral — Autores (MRs no período)", styles["H2Dark"]))
        author_counts = (
            df_src["author"].fillna("(desconhecido)")
            .value_counts(dropna=False).rename_axis("Autor").reset_index(name="Qtd MRs")
        )
        if author_counts.empty:
            elements.append(Paragraph("Sem dados para o período.", styles["SmallMuted"]))
        else:
            top_n = 20
            rows = [["#", "Autor", "Qtd MRs"]]
            for idx, r in author_counts.head(top_n).iterrows():
                rows.append([str(idx + 1), str(r["Autor"]), int(r["Qtd MRs"])])
            if len(author_counts) > top_n:
                resto_autores = len(author_counts) - top_n
                resto_mrs = int(author_counts["Qtd MRs"].iloc[top_n:].sum())
                rows.append(["—", f"(+{resto_autores} outros)", resto_mrs])
            t_auth = Table(rows, colWidths=[1.5 * cm, 12.0 * cm, 3.5 * cm], repeatRows=1, splitByRow=1)
            t_auth.setStyle(TableStyle([
                ("BACKGROUND", (0, 0), (-1, 0), HEAD_BG), ("TEXTCOLOR", (0, 0), (-1, 0), TXT),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("ALIGN", (0, 0), (0, -1), "CENTER"), ("ALIGN", (2, 0), (2, -1), "CENTER"),
                ("BACKGROUND", (0, 1), (-1, -1), colors.HexColor("#0b1220")),
                ("TEXTCOLOR", (0, 1), (-1, -1), TXT),
                ("BOX", (0, 0), (-1, -1), 0.5, BORDER), ("INNERGRID", (0, 0), (-1, -1), 0.25, BORDER),
                ("VALIGN", (0, 1), (-1, -1), "MIDDLE"),
            ]))
            elements += [t_auth, Spacer(1, 10)]

        # Resumo por Projetos
        elements.append(Paragraph("Resumo por Projetos", styles["H2Dark"]))
        by_micro = (
            df_src.groupby("micro", dropna=False)["iid"]
            .count().sort_values(ascending=False).reset_index().rename(columns={"iid": "Qtd MRs"})
        )
        if by_micro.empty:
            elements.append(Paragraph("Sem dados para o período.", styles["SmallMuted"]))
        else:
            data = [["Projetos", "Qtd MRs"]] + by_micro.values.tolist()
            t3 = Table(data, colWidths=[10 * cm, 3 * cm])
            t3.setStyle(TableStyle([
                ("BACKGROUND", (0, 0), (-1, 0), ACCENT), ("TEXTCOLOR", (0, 0), (-1, 0), colors.black),
                ("BACKGROUND", (0, 1), (-1, -1), HEAD_BG), ("TEXTCOLOR", (0, 1), (-1, -1), TXT),
                ("BOX", (0, 0), (-1, -1), 0.5, BORDER), ("INNERGRID", (0, 0), (-1, -1), 0.25, BORDER),
                ("ALIGN", (1, 1), (1, -1), "CENTER"),
            ]))
            elements += [t3, Spacer(1, 10)]

        # Lista de MRs (resumida com link)
        elements.append(Paragraph("Lista de Merge Requests", styles["H2Dark"]))
        df_list = df_src.sort_values(["micro", "created_at"], ascending=[True, False]).copy()
        header = ["Projeto", "Título", "Autor", "Criado", "Mesclado", "Link"]
        rows = [header]
        for _, r in df_list.iterrows():
            micro = r.get("micro", "") or ""
            title = r.get("title", "") or ""
            author = r.get("author", "") or ""
            ca = pd.to_datetime(r.get("created_at"), utc=True, errors="coerce")
            ma = pd.to_datetime(r.get("merged_at"), utc=True, errors="coerce")
            created_s = ca.tz_convert("UTC").strftime("%d/%m/%Y %H:%M") if pd.notna(ca) else "-"
            merged_s = ma.tz_convert("UTC").strftime("%d/%m/%Y %H:%M") if pd.notna(ma) else "-"
            link = r.get("web_url", "") or ""
            rows.append([P(micro), P(title), P(author), P(created_s), P(merged_s),
                         link_paragraph(link, "abrir") if link else P("-")])

        col_widths = [2.6 * cm, 6.6 * cm, 2.3 * cm, 2.2 * cm, 2.2 * cm, 1.6 * cm]  # 17.0 cm
        tbl = Table(rows, colWidths=col_widths, repeatRows=1, splitByRow=1)
        tbl.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), ACCENT), ("TEXTCOLOR", (0, 0), (-1, 0), colors.black),
            ("ALIGN", (0, 0), (-1, 0), "CENTER"), ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("BACKGROUND", (0, 1), (-1, -1), HEAD_BG), ("TEXTCOLOR", (0, 1), (-1, -1), TXT),
            ("BOX", (0, 0), (-1, -1), 0.5, BORDER), ("INNERGRID", (0, 0), (-1, -1), 0.25, BORDER),
            ("VALIGN", (0, 1), (-1, -1), "TOP"), ("ALIGN", (5, 0), (5, -1), "CENTER"),
            ("LEFTPADDING", (5, 0), (5, -1), 0), ("RIGHTPADDING", (5, 0), (5, -1), 0),
        ]))
        elements.append(tbl)

        doc.build(elements, onFirstPage=draw_bg, onLaterPages=draw_bg)
        buf.seek(0)
        return buf.read()

    if submit_pdf:
        if pdf_period == "Últimos 7 dias":
            week_end = pd.Timestamp.now(tz="UTC")
            week_start = (week_end - pd.Timedelta(days=6)).normalize()
            df_pdf = df[(df["created_at"] >= week_start) & (df["created_at"] <= week_end)].copy()
            label = f"Período: últimos 7 dias ({week_start.strftime('%d/%m/%Y')} a {week_end.strftime('%d/%m/%Y')})"
        else:
            df_pdf = df.copy()
            label = f"Período atual: {start_date.strftime('%d/%m/%Y')} a {end_date.strftime('%d/%m/%Y')}"

        if df_pdf.empty:
            st.info("Sem dados para gerar o PDF neste período.")
        else:
            if not REPORTLAB_AVAILABLE:
                st.error("Biblioteca para PDF não encontrada. Instale com: `pip install reportlab`")
            else:
                logo_bytes = pdf_logo.read() if pdf_logo is not None else None
                with st.spinner("🧾 Gerando PDF (dark)..."):
                    pdf_bytes = build_pdf_summary(df_pdf, label, logo_bytes, None)
                st.download_button(
                    "Baixar PDF",
                    data=pdf_bytes,
                    file_name=f"mr_resumo_{date.today().strftime('%Y%m%d')}.pdf",
                    mime="application/pdf",
                    use_container_width=True
                )

st.markdown("---")

# ===================== ABAS =====================
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "🎖️ Top Autores de MR",
    "🏅 Ranking de Commits",
    "📋 Lista de MRs",
    "🔍 Detalhes do MR",
    "👥 Equipe & Gargalos",
])

def dark_bar(df_plot: pd.DataFrame, x_field: str, y_field: str, height=360):
    chart = (
        alt.Chart(df_plot, background="transparent")
        .mark_bar()
        .encode(
            x=alt.X(x_field, title="", axis=alt.Axis(labelColor="#e5e7eb", tickColor="#94a3b8", gridOpacity=0.15)),
            y=alt.Y(y_field, sort="-x", title="", axis=alt.Axis(labelColor="#e5e7eb")),
            tooltip=list(df_plot.columns)
        )
        .properties(height=height)
        .configure_axis(gridColor="#22304a")
        .configure_view(stroke=None)
    )
    return chart

# ---- Top Autores de MR ----
with tab1:
    st.subheader("🎖️ Top Autores de MR (período e filtros aplicados)")

    # Base: contagem por autor
    top_mr = (
        df["author"].fillna("(desconhecido)")
        .value_counts()
        .rename_axis("Autor")
        .reset_index(name="Qtd MR")
        .sort_values("Qtd MR", ascending=False, kind="mergesort")
    )

    if top_mr.empty:
        st.info("Sem dados para o período/filtros.")
    else:
        # ----- gráfico de barras com rótulos dentro -----
        # Importante: NÃO colocar background nos charts filhos para evitar o erro do LayerChart.
        bars = (
            alt.Chart(top_mr)
            .mark_bar()
            .encode(
                x=alt.X("Qtd MR:Q", title="", axis=alt.Axis(labelColor="#e5e7eb", tickColor="#94a3b8", gridOpacity=0.15)),
                y=alt.Y("Autor:N", sort="-x", title="", axis=alt.Axis(labelColor="#e5e7eb")),
                tooltip=list(top_mr.columns)
            )
        )

        labels = (
            alt.Chart(top_mr)
            .mark_text(
                align="right",
                baseline="middle",
                dx=-4,  # desloca um pouco para dentro da barra
                color="black",  # texto preto
                fontWeight="bold",  # negrito
                fontSize=13  # maior
            )
            .encode(
                x="Qtd MR:Q",
                y=alt.Y("Autor:N", sort="-x"),
                text=alt.Text("Qtd MR:Q", format=".0f")  # sem decimais
            )
        )

        chart = (
            alt.layer(bars, labels)  # faz o layer
            .properties(height=360, background="transparent")  # background no LayerChart (final)
            .configure_axis(gridColor="#22304a")
            .configure_view(stroke=None)
        )
        st.altair_chart(chart, use_container_width=True)

        # ----- tabela com linhas alteradas (soma por autor) -----
        # Usa /changes; populamos quando o checkbox "Gerar visão de equipe..." estiver ligado
        if fetch_team:
            with st.spinner("Calculando linhas alteradas por autor..."):
                pairs = [
                    (int(r["project_id"]), int(r["iid"]))
                    for _, r in df.iterrows() if pd.notna(r["project_id"])
                ]
                pairs = tuple(sorted(set(pairs)))
                size_df = compute_mr_size_stats(pairs)

            if not size_df.empty:
                lines_by_author = (
                    size_df.merge(df[["project_id", "iid", "author"]], on=["project_id", "iid"], how="left")
                    .assign(author=lambda d: d["author"].fillna("(desconhecido)"))
                    .groupby("author", dropna=False)["lines_total"]
                    .sum()
                    .reset_index()
                    .rename(columns={"author": "Autor", "lines_total": "Linhas alteradas (soma)"})
                )
                table = top_mr.merge(lines_by_author, on="Autor", how="left")
                table["Linhas alteradas (soma)"] = table["Linhas alteradas (soma)"].fillna(0).astype(int)
            else:
                table = top_mr.copy()
                table["Linhas alteradas (soma)"] = "—"
                st.caption("Não foi possível obter o tamanho dos MRs neste período.")
        else:
            table = top_mr.copy()
            table["Linhas alteradas (soma)"] = "—"
            st.caption("Dica: ative **Gerar visão de equipe (gargalos) + tamanho do MR** na lateral para popular a coluna de linhas alteradas.")

        st.dataframe(table, use_container_width=True)


# ---- Ranking de Commits (opcional) ----
with tab2:
    st.subheader("🏅 Ranking de Autores de Commits (opcional)")
    if fetch_commits:
        with st.spinner("🐎 Calculando ranking de commits (REST paralelo)..."):
            pairs = [(int(r["project_id"]), int(r["iid"])) for _, r in df.iterrows() if pd.notna(r["project_id"])]
            pairs = tuple(sorted(set(pairs)))
            rank_df = compute_commit_ranking(pairs)
        if rank_df.empty:
            st.info("Nenhum commit encontrado neste período.")
        else:
            st.altair_chart(dark_bar(rank_df, x_field="Qtd Commits:Q", y_field="Autor:N"), use_container_width=True)
            st.dataframe(rank_df, use_container_width=True)
    else:
        st.info("Marque o checkbox na lateral **“Incluir ranking de commits”** para calcular.")

# ---- Lista de MRs ----
with tab3:
    st.subheader("📋 Lista de Merge Requests")
    df_vis = df.copy()
    s_created = df_vis["created_at"].dt.tz_convert("UTC")
    df_vis["Criado"] = s_created.dt.strftime("%d/%m/%Y %H:%M")
    s_merged = df_vis["merged_at"].dt.tz_convert("UTC")
    df_vis["Mesclado"] = s_merged.dt.strftime("%d/%m/%Y %H:%M")
    df_vis.loc[s_merged.isna(), "Mesclado"] = "-"
    df_vis["Link"] = df_vis["web_url"].apply(lambda u: f'<a href="{u}" target="_blank">Abrir MR</a>')

    show_cols = ["project_full_path", "iid", "title", "author", "state", "Criado", "Mesclado", "Link"]
    table_html = (
        df_vis[show_cols]
        .rename(columns={"project_full_path": "Projeto", "iid": "IID", "title": "Título", "author": "Autor", "state": "Estado"})
        .to_html(escape=False, index=False)
    )
    st.markdown("""
    <style>
      .mr-table table tbody tr td:nth-child(2),
      .mr-table table tbody tr td:nth-child(5),
      .mr-table table tbody tr td:nth-child(8) { text-align:center; }
    </style>
    """, unsafe_allow_html=True)
    st.markdown(f'<div class="mr-table">{table_html}</div>', unsafe_allow_html=True)

# ---- Detalhes do MR ----
with tab4:
    st.subheader("🔍 Detalhes de um Merge Request")
    options = [""] + [f"{row['project_full_path']}!{row['iid']}" for _, row in df.iterrows()]
    choice = st.selectbox("Selecione Projeto!IID", options)
    if choice:
        proj_path, iid_str = choice.split("!")
        row = df[(df["project_full_path"] == proj_path) & (df["iid"] == int(iid_str))].iloc[0]
        pid = int(row["project_id"]) if pd.notna(row["project_id"]) else None
        iid = int(row["iid"])

        if pid is None:
            st.error("Não foi possível resolver o ID numérico do projeto para este MR.")
        else:
            with st.spinner("Carregando detalhes do MR..."):
                s = requests.Session()
                mr_url = f"{REST_API_BASE}/projects/{pid}/merge_requests/{iid}"
                mr_resp = s.get(mr_url, headers=HEADERS_REST, timeout=30)
                mr_json = mr_resp.json() if mr_resp.ok else {}

                st.markdown(f"### {proj_path}!{iid} — {mr_json.get('title', row['title'])}")
                created_s = row["created_at"].tz_convert("UTC").strftime("%d/%m/%Y %H:%M") if pd.notna(row["created_at"]) else "-"
                st.markdown(f"**Status:** {row['state']} • **Criado em:** {created_s}")
                if pd.notna(row["merged_at"]):
                    merged_s = row["merged_at"].tz_convert("UTC").strftime("%d/%m/%Y %H:%M")
                    st.markdown(f"**Mesclado em:** {merged_s}")

                st.markdown("---")
                st.markdown("**Descrição:**")
                st.write(mr_json.get("description") or "_sem descrição_")

                st.markdown("---")
                st.markdown("**Commits:**")
                commits = rest_list_commits(s, pid, iid)
                if commits:
                    for c in commits:
                        st.markdown(f"- `{c.get('short_id','')}` • {c.get('title','')}")
                else:
                    st.write("_sem commits_")

                st.markdown("---")
                st.markdown("**Comentários:**")
                notes = rest_list_notes(s, pid, iid)
                if notes:
                    for n in notes:
                        dt = pd.to_datetime(n.get("created_at"), utc=True, errors="coerce")
                        dt_s = dt.tz_convert("UTC").strftime("%d/%m/%Y %H:%M") if pd.notna(dt) else "-"
                        user = (n.get("author") or {}).get("username", "desconhecido")
                        st.markdown(f"- **{dt_s} | {user}**: {n.get('body','')}")
                else:
                    st.write("_sem comentários_")

with tab5:
    st.subheader("👥 Equipe & Gargalos (visão simples)")
    st.caption(
        "Quem entrega mais, quem pode estar demorando **codando** (janela de commits) ou no **fluxo** (review/CI). "
        "Ative *Gerar visão de equipe (gargalos) + tamanho do MR* na barra lateral para enriquecer com commits e tamanho."
    )

    TOP_N = 10  # << ajuste aqui se quiser outro 'Top N'

    # --- helper para exibir horas (a partir de 'dias' numéricos) ---
    def to_hours_str(days_val):
        if days_val is None or (hasattr(pd, "isna") and pd.isna(days_val)):
            return "—"
        try:
            h = float(days_val) * 24.0
        except Exception:
            return "—"
        if h < 1:
            m = int(round(h * 60))
            return f"{m} min"
        h1 = round(h, 1)
        return f"{int(h1)} h" if abs(h1 - int(h1)) < 0.05 else f"{h1} h"

    # ---------- base (sem REST) ----------
    merged_only = df[(df["state"] == "merged") & df["created_at"].notna() & df["merged_at"].notna()].copy()
    if not merged_only.empty:
        merged_only["lead_time_days"] = (merged_only["merged_at"] - merged_only["created_at"]).dt.total_seconds() / 86400.0

    now_utc = pd.Timestamp.now(tz="UTC")
    opened_only = df[(df["state"] == "opened") & df["created_at"].notna()].copy()
    if not opened_only.empty:
        opened_only["wip_age_days"] = (now_utc - opened_only["created_at"]).dt.total_seconds() / 86400.0

    base = df.groupby("author", dropna=False).size().rename("mrs_total").reset_index()
    base["author"] = base["author"].fillna("(desconhecido)")

    merged_per_author = (
        merged_only.groupby("author")["iid"].count().rename("mrs_merged")
        if not merged_only.empty else pd.Series(dtype=int)
    )
    lead_time_p50 = (
        merged_only.groupby("author")["lead_time_days"].median().rename("lead_time_typ")
        if not merged_only.empty else pd.Series(dtype=float)
    )

    team = (
        base.merge(merged_per_author, on="author", how="left")
            .merge(lead_time_p50, on="author", how="left")
    )
    team["mrs_merged"] = team["mrs_merged"].fillna(0).astype(int)
    team["merge_rate_%"] = (team["mrs_merged"] / team["mrs_total"] * 100).round(1)

    # ---------- REST opcional (commits + tamanho) ----------
    dev_join = pd.DataFrame()
    team_commits_typ = None  # baseline de commits por MR (quando disponível)
    if fetch_team:
        with st.spinner("⏳ Coletando commits e tamanho (REST paralelo)..."):
            pairs = [(int(r["project_id"]), int(r["iid"])) for _, r in df.iterrows() if pd.notna(r["project_id"])]
            pairs = tuple(sorted(set(pairs)))
            created_map = {(int(r["project_id"]), int(r["iid"])): r["created_at"]
                           for _, r in df.iterrows() if pd.notna(r["project_id"])}

            dev_df = compute_dev_times(pairs, created_map)   # janela de código + commits
            size_df = compute_mr_size_stats(pairs)           # linhas alteradas

        dev_join = (dev_df.merge(df[["project_id", "iid", "author"]], on=["project_id", "iid"], how="left")
                    if not dev_df.empty else pd.DataFrame())
        if not size_df.empty and not dev_join.empty:
            dev_join = dev_join.merge(size_df, on=["project_id", "iid"], how="left")

        if not dev_join.empty:
            # estatísticas por autor (típicos = mediana)
            code_p50 = dev_join.groupby("author")["coding_window_days"].median().rename("coding_typ").reset_index()
            commits_ag = dev_join.groupby("author")["commits_count"].agg(
                commits_total="sum", commits_med_por_mr="median"
            ).reset_index()
            size_ag = dev_join.groupby("author")["lines_total"].agg(tam_linhas_typ="median").reset_index()

            team = (team.merge(code_p50, on="author", how="left")
                        .merge(commits_ag, on="author", how="left")
                        .merge(size_ag, on="author", how="left"))

            # baseline de commits por MR da equipe
            team_commits_typ = (float(dev_join["commits_count"].median())
                                if not dev_join["commits_count"].dropna().empty else None)

    # ---------- baselines da equipe (para classificar) ----------
    team_lt_baseline   = (float(team["lead_time_typ"].median())
                          if "lead_time_typ" in team and not team["lead_time_typ"].dropna().empty else None)
    team_code_baseline = (float(team["coding_typ"].median())
                          if "coding_typ" in team and not team["coding_typ"].dropna().empty else None)
    team_size_baseline = (int(team["tam_linhas_typ"].median())
                          if "tam_linhas_typ" in team and not team["tam_linhas_typ"].dropna().empty else None)

    def classifica(valor, baseline):
        # classificação continua em DIAS (somente a exibição vira horas)
        if valor is None or pd.isna(valor) or baseline is None or pd.isna(baseline):
            return "—"
        r = float(valor) / float(baseline) if baseline > 0 else float("inf")
        if r <= 0.75:  return "rápido"
        if r <= 1.25:  return "normal"
        return "lento"

    # ---------- Cards de baseline (exibidos em horas) ----------
    bc1, bc2, bc3, bc4 = st.columns(4)
    with bc1:
        st.markdown(
            f'<div class="card"><h3>Tempo típico codando (mediana)</h3>'
            f'<div class="num">{to_hours_str(team_code_baseline)}</div></div>',
            unsafe_allow_html=True
        )
    with bc2:
        st.markdown(
            f'<div class="card"><h3>Lead time típico (mediana)</h3>'
            f'<div class="num">{to_hours_str(team_lt_baseline)}</div></div>',
            unsafe_allow_html=True
        )
    with bc3:
        size_txt = f"{team_size_baseline} linhas" if team_size_baseline is not None else "—"
        st.markdown(
            f'<div class="card"><h3>Tamanho típico do MR</h3><div class="num">{size_txt}</div></div>',
            unsafe_allow_html=True
        )
    with bc4:
        commits_txt = (f"{int(team_commits_typ)}"
                       if team_commits_typ is not None and not pd.isna(team_commits_typ) else "—")
        st.markdown(
            f'<div class="card"><h3>Commits típicos por MR</h3><div class="num">{commits_txt}</div></div>',
            unsafe_allow_html=True
        )

    # ---------- 3 quadros simples ----------
    c1, c2 = st.columns(2)

    # 1) Quem entrega mais (Top N)
    top_entrega = team.sort_values(["mrs_merged", "mrs_total"], ascending=False, na_position="last").head(TOP_N)
    with c1:
        st.markdown(f"### 📦 Quem entrega mais (Top {TOP_N})")
        if top_entrega.empty:
            st.info("Sem dados no período.")
        else:
            lines = []
            for _, r in top_entrega.iterrows():
                lines.append(
                    f"- **{r['author']}** — {int(r['mrs_merged'])}/{int(r['mrs_total'])} mesclados "
                    f"(**{r['merge_rate_%']}%**)"
                )
            st.markdown("\n".join(lines))

    # 2) Possível gargalo codando (usa janela de código típica)
    with c2:
        st.markdown(f"### ⏱️ Possível gargalo **codando** (Top {TOP_N})")
        if not fetch_team or "coding_typ" not in team or team["coding_typ"].dropna().empty:
            st.info("Ative **Gerar visão de equipe (gargalos)** na barra lateral para medir tempo codando.")
        else:
            cod = team.copy()
            cod["status"] = cod["coding_typ"].apply(lambda v: classifica(v, team_code_baseline))
            cand = cod[cod["status"] == "lento"].sort_values("coding_typ", ascending=False).head(TOP_N)
            if cand.empty:
                st.success("Sem sinais claros de gargalo codando.")
            else:
                lines = []
                for _, r in cand.iterrows():
                    tip = to_hours_str(r["coding_typ"])  # << em horas
                    linhas = int(r.get("tam_linhas_typ")) if "tam_linhas_typ" in r and pd.notna(r.get("tam_linhas_typ")) else None
                    txt_size = f" • tam. típico: {linhas} linhas" if linhas is not None else ""
                    lines.append(f"- **{r['author']}** — tempo típico codando: **{tip}**{txt_size}")
                st.markdown("\n".join(lines))

    st.markdown("---")

    # 3) Possível gargalo no fluxo (review/CI) — lead time típico alto
    st.markdown(f"### 🧭 Possível gargalo **no fluxo** (Top {TOP_N})")
    fluxo = team.copy()
    fluxo["status"] = fluxo["lead_time_typ"].apply(lambda v: classifica(v, team_lt_baseline))
    if "coding_typ" in fluxo.columns:
        fluxo["slow_code"] = fluxo["coding_typ"].apply(lambda v: classifica(v, team_code_baseline) == "lento")
        cand_f = fluxo[(fluxo["status"] == "lento") & (fluxo["slow_code"] == False)] \
                    .sort_values("lead_time_typ", ascending=False).head(TOP_N)
        if cand_f.empty:
            cand_f = fluxo[fluxo["status"] == "lento"].sort_values("lead_time_typ", ascending=False).head(TOP_N)
    else:
        cand_f = fluxo[fluxo["status"] == "lento"].sort_values("lead_time_typ", ascending=False).head(TOP_N)

    if cand_f.empty:
        st.success("Sem sinais claros de gargalo no fluxo.")
    else:
        lines = []
        for _, r in cand_f.iterrows():
            tip = to_hours_str(r["lead_time_typ"])  # << em horas
            lines.append(f"- **{r['author']}** — lead time típico: **{tip}** (pode ser fila de review/CI)")
        st.markdown("\n".join(lines))

    # ---------- WIP mais antigo (ajuda a agir hoje) ----------
    st.markdown("---")
    st.markdown(f"### 🧱 WIP mais antigo (MRs abertos) — Top {TOP_N}")
    if opened_only.empty:
        st.info("Sem MRs abertos no período filtrado.")
    else:
        wip_sorted = opened_only.sort_values("wip_age_days", ascending=False).head(TOP_N)
        lines = []
        for _, r in wip_sorted.iterrows():
            age = to_hours_str(r["wip_age_days"])  # << em horas
            micro = (r.get("micro") or "")
            title = (r.get("title") or "")[:80]
            link = r.get("web_url", "")
            i = int(r["iid"])
            if link:
                lines.append(f"- **{r['author']}** — {age} • {micro}!{i} — {title}  [[abrir]]({link})")
            else:
                lines.append(f"- **{r['author']}** — {age} • {micro}!{i} — {title}")
        st.markdown("\n".join(lines))

    # ---------- Resumo por autor ----------
    st.markdown("---")
    st.markdown("### 📋 Resumo por autor (títulos autoexplicativos)")
    resumo = team.copy().rename(columns={
        "author": "Autor",
        "mrs_total": "MRs no período",
        "mrs_merged": "MRs mesclados",
        "merge_rate_%": "Merge rate (%)",
        "lead_time_typ": "Lead time típico (criado→mesclado, h)",
        "coding_typ": "Tempo típico codando (1º→último commit, h)",
        "tam_linhas_typ": "Tamanho típico (linhas)",
        "commits_med_por_mr": "Commits típicos por MR",
        "commits_total": "Commits no período"
    })

    # formatações amigáveis (em horas)
    if "Lead time típico (criado→mesclado, h)" in resumo.columns:
        resumo["Lead time típico (criado→mesclado, h)"] = resumo["Lead time típico (criado→mesclado, h)"].apply(to_hours_str)
    if "Tempo típico codando (1º→último commit, h)" in resumo.columns:
        resumo["Tempo típico codando (1º→último commit, h)"] = resumo["Tempo típico codando (1º→último commit, h)"].apply(to_hours_str)

    # monta colunas dinamicamente para evitar KeyError quando fetch_team=False
    cols_show = ["Autor", "MRs no período", "MRs mesclados", "Merge rate (%)"]
    for c in ["Lead time típico (criado→mesclado, h)",
              "Tempo típico codando (1º→último commit, h)",
              "Tamanho típico (linhas)",
              "Commits típicos por MR",
              "Commits no período"]:
        if c in resumo.columns:
            cols_show.append(c)

    st.dataframe(resumo[cols_show], use_container_width=True)

    # ---------- Ajuda rápida ----------
    with st.expander("ℹ️ Como interpretar rapidamente"):
        st.markdown(
            "- **Tempo típico codando**: mediana do tempo entre o primeiro e o último commit de cada MR (agora mostrado em **horas**). "
            "Se estiver alto vs. baseline, a pessoa está gastando mais tempo escrevendo código.\n"
            "- **Lead time típico**: mediana do tempo do *criado → mesclado* (em **horas**). Alto com **tempo codando normal** sugere gargalo de review/CI.\n"
            "- **Tamanho típico (linhas)**: mediana de linhas (+/−) por MR; MRs grandes tendem a demorar mais.\n"
            "- **WIP mais antigo**: MRs abertos que estão há mais tempo em progresso — bons candidatos para destravar agora."
        )