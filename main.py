import os
import re
import json
import requests
import pandas as pd
from datetime import datetime, date
from urllib.parse import urlparse
import streamlit as st

# =========================
# Config de Página / Tema
# =========================
st.set_page_config(
    page_title="ClickUp → GitLab MR",
    page_icon="🔗",
    layout="wide",
)

# ================
# Paleta & Estilos
# ================
def inject_css(dark: bool):
    primary = "#7c3aed"   # roxo moderno
    primary_soft = "rgba(124,58,237,0.12)"
    success = "#22c55e"
    warning = "#f59e0b"
    danger = "#ef4444"
    surface = "#0b1220" if dark else "#ffffff"
    on_surface = "#e5e7eb" if dark else "#111827"
    subtle = "#9ca3af" if dark else "#6b7280"
    card = "#111827" if dark else "#f8fafc"
    border = "#1f2937" if dark else "#e5e7eb"

    st.markdown(f"""
    <style>
      @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap');
      html, body, [class*="css"]  {{
        font-family: 'Inter', system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif !important;
      }}
      .app-title {{
        font-weight: 800;
        letter-spacing: -0.02em;
        font-size: 28px;
        margin: 4px 0 12px 0;
      }}
      .toolbar {{
        position: sticky; top: 0; z-index: 50;
        background: {surface};
        padding: 12px 8px 8px 8px;
        border-bottom: 1px solid {border};
        margin-bottom: 8px;
      }}
      .kpi {{
        background: {card};
        border: 1px solid {border};
        border-radius: 16px;
        padding: 16px;
      }}
      .kpi .label {{ color: {subtle}; font-size: 12px; }}
      .kpi .value {{ color: {on_surface}; font-size: 24px; font-weight: 800; }}
      .kpi .trend {{ font-size: 12px; color: {subtle}; }}

      .chip {{
        display: inline-block;
        padding: 4px 10px;
        border-radius: 999px;
        font-size: 12px;
        font-weight: 600;
        border: 1px solid {border};
        background: {primary_soft};
        color: {primary};
        margin-right: 8px;
      }}
      .chip-neutral {{ background: transparent; color: {subtle}; }}
      .tag {{
        display: inline-block; font-size: 11px; padding: 4px 8px; border-radius: 8px;
        background: {border}; color: {on_surface}; margin: 0 6px 6px 0;
      }}

      .section-card {{
        background: {card};
        border: 1px solid {border};
        border-radius: 16px;
        padding: 16px;
      }}

      .expander-header {{
        display:flex; gap:8px; align-items:center; flex-wrap:wrap;
      }}

      .bubble {{
        background-color: {surface};
        padding: 14px;
        border-radius: 14px;
        margin-bottom: 12px;
        border: 1px solid {border};
      }}
      .bubble .meta {{
        display:flex; gap:8px; align-items:center; margin-bottom:6px;
        color: {subtle}; font-size: 12px;
      }}
      .bubble .author {{ color: {primary}; font-weight: 700; }}
      .bubble a {{ color: {primary}; text-decoration: none; }}
      .bubble a:hover {{ text-decoration: underline; }}

      .status-header {{
        background: {primary_soft};
        border: 1px solid {border};
        color: {on_surface};
        padding: 10px 12px;
        border-radius: 12px;
        margin: 12px 0;
        font-weight: 700;
      }}

      /* tabela */
      .stDataFrame, .stTable {{ border-radius: 12px; overflow: hidden; }}
    </style>
    """, unsafe_allow_html=True)

# Toggle de aparência (aplica CSS variável)
with st.sidebar:
    st.caption("Aparência")
    dark_mode = st.toggle("🌙 Tema escuro", value=True, key="toggle_dark")
inject_css(dark_mode)

# ===============================
# Carregamento de Config / Secrets
# ===============================
def load_config():
    try:
        if hasattr(st, 'secrets') and st.secrets:
            return {
                "CLICKUP_TOKEN": st.secrets.get("CLICKUP_TOKEN", ""),
                "LIST_ID": st.secrets.get("LIST_ID", "900701188534"),
            }
    except Exception:
        pass
    # fallback env (sem obrigar python-dotenv)
    return {
        "CLICKUP_TOKEN": os.getenv("CLICKUP_TOKEN", ""),
        "LIST_ID": os.getenv("LIST_ID", "900701188534"),
    }

config = load_config()
BASE_URL = "https://api.clickup.com/api/v2"

# ================
# Sidebar / Filtros
# ================
with st.sidebar:
    st.header("Filtros")
    API_TOKEN = st.text_input("🔑 Token ClickUp", type="password", value=config["CLICKUP_TOKEN"], key="token")
    LIST_ID = st.text_input("🗂️ List ID", value=config["LIST_ID"], key="listid")
    STATUSES = st.multiselect(
        "📌 Status",
        options=["code review", "homologacao", "testado"],
        default=["code review", "homologacao", "testado"],
        key="statuses"
    )
    today = date.today()
    first_day = today.replace(day=1)
    c1, c2 = st.columns(2)
    with c1:
        start_date = st.date_input("Início", value=first_day, format="DD/MM/YYYY", key="start")
    with c2:
        end_date = st.date_input("Fim", value=today, format="DD/MM/YYYY", key="end")
    author_filter = st.text_input("👤 Autor contém", value="", key="author")
    term_filter = st.text_input("🔎 Buscar termo (link/tarefa)", value="", key="term")
    tag_filter = st.text_input("🏷️ Tag contém", value="", key="tag")
    debug_mode = st.toggle("🧪 Modo Debug (ignora filtros)", value=False, key="debug")
    focus_mode = st.toggle("🧷 Modo Foco (expandir tudo)", value=False, key="focus")

HEADERS = {"Authorization": API_TOKEN}

# ============
# Utilitários
# ============
def validate_token():
    if not API_TOKEN:
        st.error("Informe o token.")
        return False
    try:
        r = requests.get(f"{BASE_URL}/list/{LIST_ID}/task", headers=HEADERS, timeout=15)
        if r.status_code != 200:
            st.error("Token inválido ou LIST_ID sem permissão.")
            return False
        return True
    except requests.RequestException as e:
        st.error(f"Erro de conexão: {e}")
        return False

@st.cache_data(show_spinner=False)
def fetch_tasks(list_id: str, statuses: list[str]) -> list[dict]:
    tasks, page = [], 0
    params = [("statuses[]", s) for s in statuses] + [("page", page)]
    while True:
        r = requests.get(f"{BASE_URL}/list/{list_id}/task", headers=HEADERS, params=params, timeout=30)
        if r.status_code != 200:
            break
        data = r.json()
        tasks += data.get("tasks", [])
        if not data.get("next_page"):
            break
        page = data["next_page"]["page"]
        params[-1] = ("page", page)
    return tasks

@st.cache_data(show_spinner=False)
def fetch_comments(task_id: str) -> list[dict]:
    comments, page = [], 0
    while True:
        r = requests.get(f"{BASE_URL}/task/{task_id}/comment", headers=HEADERS, params={"page": page}, timeout=30)
        if r.status_code != 200:
            break
        d = r.json()
        comments += d.get("comments", [])
        if not d.get("next_page"):
            break
        page = d["next_page"]
    return comments

def find_urls_in_text(text: str) -> list[str]:
    url_pattern = r'https?://(?:[-\w.]|(?:%[\da-fA-F]{{2}}))+[/\w\.-]*\??[/\w\.-=&%]*'
    urls = re.findall(url_pattern, text or "")
    gitlab_pattern = r'(?:gitlab\.com/[^\s\)\]\"]+)'
    gitlab_urls = [f"https://{u}" for u in re.findall(gitlab_pattern, text or "")]
    return urls + gitlab_urls

def is_gitlab_url(url: str) -> bool:
    parsed = urlparse(url)
    netloc = parsed.netloc.lower()
    path = parsed.path.lower()
    if 'gitlab' in netloc:
        return True
    if '/merge_requests/' in path or '/-/merge_requests/' in path:
        return True
    if re.search(r'/[\w\-]+/[\w\-]+(?:\.git)?$', path) and not any(x in path for x in ['/blob/', '/tree/', '/commit/']):
        return True
    return False

def normalize_gitlab_url(url: str) -> str:
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    parsed = urlparse(url)
    if '/merge_requests/' in parsed.path or '/-/merge_requests/' in parsed.path:
        return f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
    return url

def extract_links(comment: dict) -> list[str]:
    links = []
    if "comment" in comment and isinstance(comment["comment"], list):
        for part in comment["comment"]:
            if isinstance(part, dict):
                if part.get("type") == "link_mention":
                    link_url = part.get("link_mention", {}).get("url", "")
                    if link_url and is_gitlab_url(link_url):
                        links.append(normalize_gitlab_url(link_url))
                elif part.get("text"):
                    links += [normalize_gitlab_url(u) for u in find_urls_in_text(part["text"]) if is_gitlab_url(u)]
    text = comment.get("comment_text", "")
    if text:
        links += [normalize_gitlab_url(u) for u in find_urls_in_text(text) if is_gitlab_url(u)]
    # uniq
    return sorted(set(links))

def parse_date(raw) -> datetime | None:
    if not raw:
        return None
    if isinstance(raw, datetime):
        return raw
    if isinstance(raw, str) and raw.isdigit():
        raw = int(raw)
    if isinstance(raw, (int, float)):
        if raw > 1000000000000:  # ms
            return datetime.fromtimestamp(raw / 1000)
        if raw > 1000000000:     # s
            return datetime.fromtimestamp(raw)
    try:
        return datetime.fromisoformat(str(raw).replace('Z', '+00:00'))
    except Exception:
        pass
    for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ","%Y-%m-%dT%H:%M:%SZ","%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(str(raw), fmt)
        except Exception:
            continue
    return None

# =========
# Header UI
# =========
st.markdown('<div class="toolbar">', unsafe_allow_html=True)
st.markdown('<div class="app-title">ClickUp → GitLab MR Links</div>', unsafe_allow_html=True)
st.caption("Filtre à esquerda; veja resumo, comentários com links e dados tabulares.")
st.markdown('</div>', unsafe_allow_html=True)

# =========
# Execução
# =========
if not validate_token():
    st.stop()

with st.spinner("Carregando tarefas…"):
    tasks = fetch_tasks(LIST_ID, STATUSES)

comments_cache = {}
if tasks:
    with st.spinner("Coletando comentários…"):
        prog = st.progress(0)
        for i, t in enumerate(tasks):
            comments_cache[t["id"]] = fetch_comments(t["id"])
            prog.progress((i + 1) / max(1, len(tasks)))
        prog.empty()

# =====================================
# Processamento: links por autor / linhas
# =====================================
links_by_author: dict[str, list[dict]] = {}
rows = []
comments_with_links = 0
total_links = 0

for t in tasks:
    status = t.get("status", {}).get("status", "")
    if status not in STATUSES:
        continue

    # filtro por tag opcional
    if not debug_mode and tag_filter:
        tag_names = [tag.get("name","") for tag in t.get("tags", [])]
        if not any(tag_filter.lower() in n.lower() for n in tag_names):
            continue

    for c in comments_cache.get(t["id"], []):
        links = extract_links(c)
        if not links:
            continue

        dt = parse_date(c.get("date") or c.get("date_created") or c.get("created_at"))
        if not dt:
            continue

        if not debug_mode and not (start_date <= dt.date() <= end_date):
            continue

        author = c.get("user", {}).get("username", "—")
        comment_text = c.get("comment_text", "") or ""
        for link in links:
            if not debug_mode and term_filter:
                if term_filter.lower() not in link.lower() and term_filter.lower() not in t["name"].lower():
                    continue
            if not debug_mode and author_filter and author_filter.lower() not in author.lower():
                continue

            entry = {"Autor": author, "Link": link, "Tarefa": t["name"], "Status": status, "Data": dt, "Tags": ", ".join([tg["name"] for tg in t.get("tags",[])])}
            rows.append(entry)
            links_by_author.setdefault(author, []).append(entry)
            total_links += 1
        comments_with_links += 1

df = pd.DataFrame(rows).sort_values(by="Data", ascending=False) if rows else pd.DataFrame(columns=["Autor","Link","Tarefa","Status","Data","Tags"])

# ============
# KPIs (cards)
# ============
k1, k2, k3, k4, k5 = st.columns(5)
with k1: st.markdown(f'<div class="kpi"><div class="label">Tarefas</div><div class="value">{len(tasks)}</div></div>', unsafe_allow_html=True)
with k2: st.markdown(f'<div class="kpi"><div class="label">Comentários c/ Links</div><div class="value">{comments_with_links}</div></div>', unsafe_allow_html=True)
with k3: st.markdown(f'<div class="kpi"><div class="label">Links Totais</div><div class="value">{total_links}</div></div>', unsafe_allow_html=True)
with k4: st.markdown(f'<div class="kpi"><div class="label">Autores</div><div class="value">{len(links_by_author)}</div></div>', unsafe_allow_html=True)
with k5:
    dr = f"{start_date.strftime('%d/%m/%Y')} → {end_date.strftime('%d/%m/%Y')}"
    st.markdown(f'<div class="kpi"><div class="label">Período</div><div class="value" style="font-size:16px">{dr}</div></div>', unsafe_allow_html=True)

st.divider()

# =====
# Abas
# =====
tab_resumo, tab_coment, tab_dados = st.tabs(["📊 Resumo", "💬 Comentários", "📑 Dados"])

with tab_resumo:
    # seletor de autor
    autores = ["Todos"] + sorted(links_by_author.keys())
    autor_sel = st.selectbox("Filtrar por autor", autores, key="autor_sel")

    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.subheader("🔗 Links por Autor")
    if df.empty:
        st.info("Nenhum link encontrado com os filtros atuais.")
    else:
        if autor_sel != "Todos":
            df_view = df[df["Autor"] == autor_sel]
        else:
            df_view = df
        # lista enxuta
        for autor, grp in df_view.groupby("Autor"):
            st.markdown(f'<span class="chip">{autor}</span> <span class="chip chip-neutral">{len(grp)} links</span>', unsafe_allow_html=True)
            for _, row in grp.head(15).iterrows():
                st.markdown(f"- [{row['Link']}]({row['Link']}) · *{row['Tarefa']}* · {row['Data'].strftime('%d/%m/%Y %H:%M')}")
            if len(grp) > 15:
                st.caption(f"… e mais {len(grp)-15} links")
    st.markdown('</div>', unsafe_allow_html=True)

with tab_coment:
    st.subheader("Comentários com Links do GitLab")
    if not tasks or df.empty:
        st.info("Nada para exibir ainda.")
    else:
        # Agrupar tarefas por status
        tasks_by_status = {}
        for t in tasks:
            stt = t.get("status", {}).get("status", "")
            tasks_by_status.setdefault(stt, []).append(t)

        for status in [s for s in STATUSES if s in tasks_by_status]:
            st.markdown(f'<div class="status-header">{status.upper()}</div>', unsafe_allow_html=True)
            for task in tasks_by_status[status]:
                # Filtrar por tag e termo task, se aplicável
                if not debug_mode and term_filter and term_filter.lower() not in task["name"].lower():
                    continue
                if not debug_mode and tag_filter:
                    tnames = [tg.get("name","") for tg in task.get("tags",[])]
                    if not any(tag_filter.lower() in n.lower() for n in tnames):
                        continue

                bubble_items = []
                for c in comments_cache.get(task["id"], []):
                    links = extract_links(c)
                    if not links:
                        continue
                    dt = parse_date(c.get("date") or c.get("date_created") or c.get("created_at"))
                    if not dt:
                        continue
                    if not debug_mode and not (start_date <= dt.date() <= end_date):
                        continue
                    author = c.get("user", {}).get("username", "—")
                    if not debug_mode and author_filter and author_filter.lower() not in author.lower():
                        continue
                    ts = dt.strftime("%d/%m/%Y %H:%M")
                    comment_text = c.get("comment_text", "") or ""
                    bubble_items.append((ts, author, comment_text, links))

                if not bubble_items:
                    continue

                tags_html = " ".join([f'<span class="tag">{tg["name"]}</span>' for tg in
                                      task.get("tags", [])]) or '<span class="chip chip-neutral">sem tags</span>'
                label_text = f"{status.upper()} · {task['name']} · {len(bubble_items)} comentário(s)"  # use clip(task['name']) se quiser encurtar

                with st.expander(label_text, expanded=focus_mode):
                    st.markdown(f"""
                      <div class="expander-header">
                        <span class="chip">{status}</span>
                        <strong>{task['name']}</strong>
                        {tags_html}
                        <span class="chip chip-neutral">{len(bubble_items)} comentários</span>
                      </div>
                    """, unsafe_allow_html=True)

                    for ts, author, txt, links in bubble_items:
                        links_html = "<br>".join([f'<a href="{lk}" target="_blank">{lk}</a>' for lk in links])
                        st.markdown(f"""
                          <div class="bubble">
                            <div class="meta"><span>{ts}</span> · <span class="author">{author}</span></div>
                            <div style="margin-bottom:8px">{txt}</div>
                            <div><strong>Links</strong><br>{links_html}</div>
                          </div>
                        """, unsafe_allow_html=True)

with tab_dados:
    st.subheader("Tabela de Links (exportável)")
    if df.empty:
        st.info("Sem dados.")
    else:
        c1, c2, c3 = st.columns([3,1,1])
        with c2:
            ordenar_por = st.selectbox("Ordenar por", ["Data","Autor","Tarefa","Status"], index=0, key="ordpor")
        with c3:
            asc = st.toggle("Ascendente", value=False, key="asc")
        df_sorted = df.sort_values(by=ordenar_por, ascending=asc)
        st.dataframe(df_sorted, use_container_width=True, hide_index=True)
        csv = df_sorted.to_csv(index=False).encode("utf-8")
        st.download_button("⬇️ Baixar CSV", data=csv, file_name="links_gitlab.csv", mime="text/csv")

st.caption("✅ Atualizado com filtros e visual renovado.")