import requests
import re
from datetime import datetime, date
import streamlit as st

# ——— CSS PARA BOLHAS WHATSAPP DARK MODE ———
st.markdown("""
    <style>
    .bubble {
        background-color: #2c2c2e;
        padding: 14px;
        border-radius: 16px;
        margin-bottom: 16px;
        max-width: 80%;
        box-shadow: 0 2px 6px rgba(0,0,0,0.8);
    }
    .author {
        font-weight: bold;
        color: #58a6ff;
    }
    .timestamp {
        font-size: 0.75em;
        color: #8b8b8b;
        margin-right: 8px;
    }
    </style>
""", unsafe_allow_html=True)

# ——— SIDEBAR COM FILTROS ———
st.sidebar.header("Filtros de Comentário")
API_TOKEN = st.sidebar.text_input(
    "Token ClickUp",
    value="pk_170645352_SBNT41M3TE5XCB413PQNQGZ9Z5R60LNT",
    type="password"
)
LIST_ID = st.sidebar.text_input("List ID", value="900701188534")
STATUSES = st.sidebar.multiselect(
    "Status",
    options=["code review", "homologacao", "testado"],
    default=["code review", "homologacao", "testado"]
)

# intervalo de data
today = date.today()
first_day = today.replace(day=1)
start_date = st.sidebar.date_input(
    "Data inicial (DD/MM/AAAA)",
    value=first_day,
    format="DD/MM/YYYY"
)
end_date = st.sidebar.date_input(
    "Data final (DD/MM/AAAA)",
    value=today,
    format="DD/MM/YYYY"
)

# filtros de autor
author_filter = st.sidebar.text_input("Autor (bubbles)", value="")
# este selectbox será preenchido logo abaixo:
selected_link_author = st.sidebar.selectbox("🔗 Autor para resumo de links", options=["Todos"])

BASE_URL = "https://api.clickup.com/api/v2"
HEADERS = {"Authorization": API_TOKEN}

@st.cache_data
def fetch_tasks(list_id, statuses):
    tasks, page = [], 0
    params = [("statuses[]", s) for s in statuses] + [("page", page)]
    while True:
        r = requests.get(f"{BASE_URL}/list/{list_id}/task", headers=HEADERS, params=params)
        r.raise_for_status()
        data = r.json()
        tasks += data.get("tasks", [])
        if not data.get("next_page"):
            break
        page = data["next_page"]["page"]
        params[-1] = ("page", page)
    return tasks

@st.cache_data
def fetch_comments(task_id):
    comments, page = [], 0
    while True:
        r = requests.get(f"{BASE_URL}/task/{task_id}/comment", headers=HEADERS, params={"page": page})
        r.raise_for_status()
        d = r.json()
        comments += d.get("comments", [])
        if not d.get("next_page"):
            break
        page = d["next_page"]
    return comments

def extract_links(text):
    return re.findall(r"https://gitlab\.com/[^\s\)]+", text)

def parse_date(raw):
    if isinstance(raw, (int, float)):
        return datetime.fromtimestamp(raw / 1000)
    if isinstance(raw, str) and raw.isdigit():
        return datetime.fromtimestamp(int(raw) / 1000)
    if isinstance(raw, str):
        for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"):
            try: return datetime.strptime(raw, fmt)
            except ValueError: pass
    return None

# ——— MAIN ———
st.set_page_config(page_title="ClickUp → GitLab MR", layout="wide")
st.title("🔍 ClickUp → GitLab MR Links")
st.markdown("Ajuste filtros à esquerda; veja progresso e resultados agrupados por autor.")

# 1) tasks
with st.spinner("Carregando tasks…"):
    tasks = fetch_tasks(LIST_ID, STATUSES)

# 2) comentários com barra de progresso
comments_cache = {}
with st.spinner("Buscando comentários…"):
    prog = st.progress(0)
    for i, t in enumerate(tasks):
        comments_cache[t["id"]] = fetch_comments(t["id"])
        prog.progress((i+1)/len(tasks))
    prog.empty()

# 3) montar dicionário de links por autor
links_by_author = {}
for t in tasks:
    if t.get("status",{}).get("status","") not in STATUSES:
        continue
    for c in comments_cache[t["id"]]:
        txt = c.get("comment_text","")
        if not extract_links(txt): continue
        dt = parse_date(c.get("date") or c.get("date_created") or c.get("created_at"))
        if not dt or not (start_date <= dt.date() <= end_date): continue
        author = c.get("user",{}).get("username","—")
        for link in extract_links(txt):
            links_by_author.setdefault(author, []).append(link)

# atualizar selectbox de autores para links
authors = sorted(links_by_author.keys())
authors_options = ["Todos"] + authors
selected_link_author = st.sidebar.selectbox("🔗 Autor para resumo de links", options=authors_options)

# 4) exibir resumo de links
st.subheader("🔗 Links GitLab por Autor")
if selected_link_author == "Todos":
    for author in authors:
        st.markdown(f"**{author}**")
        for l in links_by_author.get(author, []):
            st.markdown(f"- {l}")
else:
    st.markdown(f"**{selected_link_author}**")
    for l in links_by_author.get(selected_link_author, []):
        st.markdown(f"- {l}")

# 5) exibir bubbles por status
for status in STATUSES:
    st.header(status.upper())
    for t in tasks:
        if t.get("status",{}).get("status","") != status:
            continue
        bubble_items = []
        for c in comments_cache[t["id"]]:
            txt = c.get("comment_text","")
            if not extract_links(txt): continue
            dt = parse_date(c.get("date") or c.get("date_created") or c.get("created_at"))
            if not dt or not (start_date <= dt.date() <= end_date): continue
            author = c.get("user",{}).get("username","—")
            if author_filter and author_filter.lower() not in author.lower(): continue
            ts = dt.strftime("%d/%m/%Y %H:%M")
            bubble_items.append((ts, author, txt))
        if not bubble_items: continue
        with st.expander(f"{t['name']}  🏷️ {', '.join(tag['name'] for tag in t.get('tags',[])) or '—'}"):
            for ts, author, txt in bubble_items:
                st.markdown(f"""
                    <div class="bubble">
                        <div>
                            <span class="timestamp">{ts}</span>
                            <span class="author">{author}</span>
                        </div>
                        <div>{txt}</div>
                    </div>
                """, unsafe_allow_html=True)

st.success("✅ Busca concluída.")
