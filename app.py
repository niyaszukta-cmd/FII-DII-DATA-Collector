import streamlit as st
import requests
import pandas as pd
import json
from datetime import datetime, date, timedelta
import io
import time

# ── PAGE CONFIG ──────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="FII/DII Data Collector",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── CUSTOM CSS ───────────────────────────────────────────────────────────────

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&family=IBM+Plex+Sans:wght@300;400;500;600&display=swap');
html, body, [class*="css"] { font-family: 'IBM Plex Sans', sans-serif; }
.stApp { background-color: #0d1117; color: #e6edf3; }
section[data-testid="stSidebar"] { background-color: #161b22; border-right: 1px solid #21262d; }
h1 { font-family: 'IBM Plex Mono', monospace !important; color: #58a6ff !important; font-size: 1.6rem !important; letter-spacing: -0.02em; }
h2 { font-family: 'IBM Plex Mono', monospace !important; color: #79c0ff !important; font-size: 1.1rem !important; }
h3 { color: #8b949e !important; font-size: 0.9rem !important; font-weight: 500 !important; text-transform: uppercase; letter-spacing: 0.08em; }
[data-testid="metric-container"] { background: #161b22; border: 1px solid #21262d; border-radius: 8px; padding: 1rem; }
[data-testid="metric-container"] label { color: #8b949e !important; font-size: 0.75rem !important; text-transform: uppercase; letter-spacing: 0.06em; }
[data-testid="metric-container"] [data-testid="stMetricValue"] { color: #e6edf3 !important; font-family: 'IBM Plex Mono', monospace; }
.stTextInput > div > div > input,
.stTextArea > div > div > textarea,
.stDateInput > div > div > input { background-color: #0d1117 !important; border: 1px solid #30363d !important; color: #e6edf3 !important; border-radius: 6px !important; font-family: 'IBM Plex Mono', monospace !important; font-size: 0.8rem !important; }
.stButton > button { background: #238636 !important; color: #fff !important; border: 1px solid #2ea043 !important; border-radius: 6px !important; font-weight: 500 !important; padding: 0.5rem 1.5rem !important; }
.stButton > button:hover { background: #2ea043 !important; }
.stDownloadButton > button { background: #1f6feb !important; border-color: #388bfd !important; color: #fff !important; border-radius: 6px !important; font-weight: 500 !important; }
.stSelectbox > div > div { background-color: #161b22 !important; border: 1px solid #30363d !important; color: #e6edf3 !important; border-radius: 6px !important; }
.stAlert { border-radius: 8px !important; font-size: 0.85rem !important; }
code { font-family: 'IBM Plex Mono', monospace !important; background: #161b22 !important; color: #79c0ff !important; padding: 2px 6px !important; border-radius: 4px !important; font-size: 0.8rem !important; }
.stTabs [data-baseweb="tab-list"] { background: transparent; border-bottom: 1px solid #21262d; gap: 0; }
.stTabs [data-baseweb="tab"] { background: transparent !important; color: #8b949e !important; border-bottom: 2px solid transparent !important; font-size: 0.85rem; padding: 0.5rem 1.25rem; }
.stTabs [aria-selected="true"] { color: #e6edf3 !important; border-bottom-color: #f78166 !important; }
.streamlit-expanderHeader { background: #161b22 !important; border: 1px solid #21262d !important; border-radius: 6px !important; color: #8b949e !important; font-size: 0.85rem !important; }
hr { border-color: #21262d !important; margin: 1.5rem 0; }
::-webkit-scrollbar { width: 6px; height: 6px; }
::-webkit-scrollbar-track { background: #0d1117; }
::-webkit-scrollbar-thumb { background: #30363d; border-radius: 3px; }
.chunk-info { font-size: 0.78rem; color: #484f58; font-family: 'IBM Plex Mono', monospace; padding: 4px 0; }
</style>
""", unsafe_allow_html=True)

# ── CONSTANTS ────────────────────────────────────────────────────────────────

API_URL       = "https://static-scanx.dhan.co/staticscanx/fiidiidata"
MAX_HOURS     = 990          # API limit is 1000 hrs; use 990 for safety
MAX_DAYS      = MAX_HOURS // 24   # = 41 days per chunk

SEGMENT_OPTIONS = {
    "Equity":             "equity",
    "Futures & Options":  "fno",
    "Debt":               "debt",
    "Hybrid":             "hybrid",
}

TIMEFRAME_OPTIONS = {
    "Daily":   "D",
    "Weekly":  "W",
    "Monthly": "M",
}

# ── SESSION STATE ─────────────────────────────────────────────────────────────

if "fetch_history" not in st.session_state:
    st.session_state.fetch_history = []
if "last_df" not in st.session_state:
    st.session_state.last_df = None
if "last_raw" not in st.session_state:
    st.session_state.last_raw = None

# ── HELPERS ──────────────────────────────────────────────────────────────────

def date_chunks(from_dt: date, to_dt: date, chunk_days: int = MAX_DAYS):
    """
    Split a wide date range into consecutive chunks of at most chunk_days.
    Yields (chunk_start, chunk_end) pairs as date objects.
    """
    current = from_dt
    while current <= to_dt:
        end = min(current + timedelta(days=chunk_days - 1), to_dt)
        yield current, end
        current = end + timedelta(days=1)


def build_payload(segment: str, from_dt: date, to_dt: date, timeframe: str) -> dict:
    """
    Exact structure confirmed from DevTools:
    { "data": { "startdate": "DD-MM-YYYY", "enddate": "DD-MM-YYYY",
                "defaultpage": "N", "segment": "equity", "TimeFrame": "D" } }
    """
    return {
        "data": {
            "startdate":   from_dt.strftime("%d-%m-%Y"),
            "enddate":     to_dt.strftime("%d-%m-%Y"),
            "defaultpage": "N",
            "segment":     segment,
            "TimeFrame":   timeframe,
        }
    }


def fetch_single(token: str, payload: dict):
    hdrs = {
        "Auth":           token.strip(),
        "Content-Type":   "application/json",
        "Accept":         "application/json, text/plain, */*",
        "Origin":         "https://scanx.trade",
        "Referer":        "https://scanx.trade/insight/fii-dii-data",
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/146.0.0.0 Safari/537.36"
        ),
    }
    return requests.post(API_URL, headers=hdrs, json=payload, timeout=15)


def parse_records(raw) -> list:
    """Extract a list of record dicts from whatever shape the API returns."""
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict):
        # check known wrapper keys
        for key in ["data", "result", "records", "fiidii", "response"]:
            if key in raw and isinstance(raw[key], list):
                return raw[key]
        # fallback: first list value
        for v in raw.values():
            if isinstance(v, list):
                return v
    return []


def color_net(val):
    try:
        v = float(val)
        if v > 0:
            return "color: #3fb950; font-weight: 500"
        elif v < 0:
            return "color: #f85149; font-weight: 500"
    except Exception:
        pass
    return ""


def df_to_csv(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8")


def df_to_excel(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="FII_DII_Data")
    return buf.getvalue()


# ── SIDEBAR ──────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("## ⚙️ Configuration")
    st.markdown("---")

    st.markdown("### 🔑 Auth Token")
    st.markdown(
        "Get from **DevTools → Network → fiidiidata → Headers → Auth**",
        help=(
            "Open Chrome DevTools (F12) → Network tab → Fetch/XHR filter → "
            "reload the ScanX FII/DII page → click 'fiidiidata' → "
            "Headers tab → copy the full Auth value (starts with eyJ...)"
        ),
    )
    token = st.text_area(
        "JWT token",
        height=120,
        placeholder="eyJ0eXAiOiJKV1QiLCJhbGciOiJ...",
        label_visibility="collapsed",
    )

    st.markdown("---")
    st.markdown("### 📅 Date Range")

    # Preset shortcuts
    preset = st.radio(
        "Quick range",
        ["Last 30 days", "Last 90 days", "Last 6 months", "Last 1 year", "Custom"],
        index=0,
        horizontal=False,
        label_visibility="collapsed",
    )

    today = date.today()
    if preset == "Last 30 days":
        default_from, default_to = today - timedelta(days=30), today
    elif preset == "Last 90 days":
        default_from, default_to = today - timedelta(days=90), today
    elif preset == "Last 6 months":
        default_from, default_to = today - timedelta(days=182), today
    elif preset == "Last 1 year":
        default_from, default_to = today - timedelta(days=365), today
    else:
        default_from, default_to = today - timedelta(days=30), today

    col1, col2 = st.columns(2)
    with col1:
        from_date = st.date_input("From", value=default_from)
    with col2:
        to_date = st.date_input("To", value=default_to)

    # Show chunk count warning
    total_days = (to_date - from_date).days + 1
    n_chunks = -(-total_days // MAX_DAYS)   # ceiling division
    if n_chunks > 1:
        st.info(
            f"⚡ Range is **{total_days} days** — will be fetched in "
            f"**{n_chunks} chunks** of ≤{MAX_DAYS} days each.",
            icon="ℹ️",
        )

    st.markdown("---")
    st.markdown("### 📂 Segment")
    segment_label   = st.selectbox("Market segment", list(SEGMENT_OPTIONS.keys()))
    segment_val     = SEGMENT_OPTIONS[segment_label]

    st.markdown("### 🕐 Time Frame")
    timeframe_label = st.selectbox("Frequency", list(TIMEFRAME_OPTIONS.keys()))
    timeframe_val   = TIMEFRAME_OPTIONS[timeframe_label]

    st.markdown("---")
    st.markdown("### 🔧 Advanced")
    with st.expander("Custom Payload (JSON override)"):
        st.markdown("Overrides auto-built payload. Chunking is **disabled** when using custom payload.")
        custom_payload_str = st.text_area(
            "Custom JSON",
            height=160,
            placeholder=(
                '{\n'
                '  "data": {\n'
                '    "startdate": "28-03-2026",\n'
                '    "enddate": "28-04-2026",\n'
                '    "segment": "equity",\n'
                '    "TimeFrame": "D",\n'
                '    "defaultpage": "N"\n'
                '  }\n'
                '}'
            ),
            label_visibility="collapsed",
        )

    st.markdown("---")
    fetch_btn = st.button("🚀 Fetch Data", use_container_width=True)

    st.markdown("---")
    st.markdown(
        "<div style='font-size:0.7rem; color:#484f58; text-align:center;'>"
        "Source: static-scanx.dhan.co<br>"
        "API limit: 1000 hrs (~41 days) per request<br>"
        "JWT token expires with each session"
        "</div>",
        unsafe_allow_html=True,
    )

# ── MAIN PANEL ───────────────────────────────────────────────────────────────

st.markdown("# 📊 FII / DII Data Collector")
st.markdown(
    "<p style='color:#8b949e; font-size:0.9rem; margin-top:-0.5rem;'>"
    "Pull Foreign & Domestic Institutional Investor activity from ScanX / Dhan</p>",
    unsafe_allow_html=True,
)

# Live payload preview
with st.expander("🔍 Current Request Payload (preview)", expanded=False):
    st.code(
        json.dumps(build_payload(segment_val, from_date, to_date, timeframe_val), indent=2),
        language="json",
    )

# How-to guide
with st.expander("📖 How to get your Auth token", expanded=False):
    st.markdown("""
1. Go to **[scanx.trade/insight/fii-dii-data](https://scanx.trade/insight/fii-dii-data)** and log in
2. Open **Chrome DevTools** → press `F12`
3. Click the **Network** tab → select **Fetch/XHR** filter
4. **Reload** the page (`Ctrl+R` / `Cmd+R`)
5. Click the `fiidiidata` request → **Headers** tab
6. Scroll to **Request Headers** → copy the full **`Auth`** value (starts with `eyJ...`)
7. Paste it in the sidebar → set your date range → click **Fetch Data**

> ⚠️ The token **expires** when you log out or after some time — grab a fresh one as needed.  
> 📅 Dates are sent as **DD-MM-YYYY** — handled automatically.  
> 🔢 API limit is **1000 hours (~41 days)** per call — larger ranges are auto-split.
""")

st.markdown("---")

# ── FETCH LOGIC ──────────────────────────────────────────────────────────────

if fetch_btn:
    if not token or len(token.strip()) < 20:
        st.error("⚠️ Please paste a valid Auth token in the sidebar.")
    elif from_date > to_date:
        st.error("⚠️ 'From' date must be earlier than 'To' date.")
    else:
        # Custom payload path (single call, no chunking)
        if custom_payload_str.strip():
            try:
                custom_payload = json.loads(custom_payload_str)
            except json.JSONDecodeError as e:
                st.error(f"Invalid JSON in custom payload: {e}")
                st.stop()

            with st.spinner("Fetching with custom payload..."):
                try:
                    resp = fetch_single(token, custom_payload)
                    if resp.status_code == 200:
                        raw = resp.json()
                        records = parse_records(raw)
                        df = pd.DataFrame(records)
                        st.session_state.last_df = df
                        st.session_state.last_raw = raw
                        st.success(f"✅ Fetched **{len(df)} records**")
                    elif resp.status_code == 401:
                        st.error("🔐 401 Unauthorized — token expired.")
                    else:
                        st.error(f"❌ HTTP {resp.status_code}: {resp.text[:300]}")
                        st.json(resp.json())
                except Exception as e:
                    st.error(f"Error: {e}")

        else:
            # Auto-chunked path
            chunks = list(date_chunks(from_date, to_date))
            total  = len(chunks)
            ts     = datetime.now().strftime("%H:%M:%S")

            if total == 1:
                status_msg = st.empty()
            else:
                status_msg = st.empty()
                prog_bar   = st.progress(0, text=f"Fetching chunk 1 / {total}...")

            all_records = []
            all_raws    = []
            error_flag  = False

            for i, (chunk_start, chunk_end) in enumerate(chunks):
                label = (
                    f"Fetching {segment_label} ({timeframe_label}): "
                    f"{chunk_start.strftime('%d-%m-%Y')} → {chunk_end.strftime('%d-%m-%Y')} "
                    f"[{i+1}/{total}]"
                )
                if total > 1:
                    prog_bar.progress((i) / total, text=label)
                else:
                    status_msg.info(f"⏳ {label}")

                payload = build_payload(segment_val, chunk_start, chunk_end, timeframe_val)

                try:
                    resp = fetch_single(token, payload)

                    if resp.status_code == 401:
                        st.error("🔐 **401 Unauthorized** — Token has expired. Grab a fresh one from DevTools.")
                        error_flag = True
                        break
                    elif resp.status_code == 403:
                        st.error("🚫 **403 Forbidden** — Make sure you're logged in to ScanX.")
                        error_flag = True
                        break
                    elif resp.status_code != 200:
                        st.error(f"❌ HTTP {resp.status_code} on chunk {i+1}: {resp.text[:200]}")
                        error_flag = True
                        break

                    raw = resp.json()

                    # Check for API-level errors in response body
                    if isinstance(raw, dict) and raw.get("code") == -1:
                        remark = raw.get("remarks", "Unknown API error")
                        st.error(f"❌ API error on chunk {i+1}: `{remark}`")
                        error_flag = True
                        break

                    records = parse_records(raw)
                    all_records.extend(records)
                    all_raws.append(raw)

                except requests.exceptions.Timeout:
                    st.error(f"⏱️ Timeout on chunk {i+1}. Try a smaller date range.")
                    error_flag = True
                    break
                except Exception as e:
                    st.error(f"Error on chunk {i+1}: {e}")
                    error_flag = True
                    break

                # Polite delay between chunks
                if i < total - 1:
                    time.sleep(0.5)

            if total > 1:
                prog_bar.progress(1.0, text="Done!")

            if not error_flag:
                df = pd.DataFrame(all_records)
                st.session_state.last_df = df
                st.session_state.last_raw = all_raws if total > 1 else all_raws[0] if all_raws else {}

                st.session_state.fetch_history.append({
                    "Time":      ts,
                    "Segment":   segment_label,
                    "TimeFrame": timeframe_label,
                    "From":      from_date.strftime("%d-%m-%Y"),
                    "To":        to_date.strftime("%d-%m-%Y"),
                    "Chunks":    total,
                    "Rows":      len(df),
                    "Status":    "✅ OK",
                })

                status_msg.empty()
                st.success(
                    f"✅ Fetched **{len(df)} records** "
                    f"({total} API call{'s' if total > 1 else ''}) at {ts}"
                )

# ── RESULTS ──────────────────────────────────────────────────────────────────

df  = st.session_state.last_df
raw = st.session_state.last_raw

if df is not None and not df.empty:

    st.markdown("### Summary")
    m1, m2, m3, m4 = st.columns(4)

    net_cols  = [c for c in df.columns if "net"  in c.lower()]
    buy_cols  = [c for c in df.columns if "buy"  in c.lower()]
    sell_cols = [c for c in df.columns if "sell" in c.lower()]

    with m1:
        st.metric("Total Records", len(df))
    with m2:
        if buy_cols:
            v = pd.to_numeric(df[buy_cols[0]], errors="coerce").sum()
            st.metric("Total Buy", f"₹{v:,.0f} Cr")
    with m3:
        if sell_cols:
            v = pd.to_numeric(df[sell_cols[0]], errors="coerce").sum()
            st.metric("Total Sell", f"₹{v:,.0f} Cr")
    with m4:
        if net_cols:
            v = pd.to_numeric(df[net_cols[0]], errors="coerce").sum()
            st.metric("Net Flow", f"₹{v:,.0f} Cr",
                      delta="Inflow" if v >= 0 else "Outflow")

    st.markdown("---")

    tab1, tab2, tab3 = st.tabs(["📋 Data Table", "🔍 Raw JSON", "📜 Fetch History"])
    ts_str = datetime.now().strftime("%Y%m%d_%H%M")

    with tab1:
        st.markdown("### Data Table")
        styled = df.style
        for nc in net_cols:
            styled = styled.applymap(color_net, subset=[nc])
        st.dataframe(styled, use_container_width=True, height=420)

        st.markdown("### Export")
        c1, c2, _ = st.columns([1, 1, 2])
        with c1:
            st.download_button(
                "⬇️ CSV",
                data=df_to_csv(df),
                file_name=f"fii_dii_{ts_str}.csv",
                mime="text/csv",
                use_container_width=True,
            )
        with c2:
            st.download_button(
                "⬇️ Excel",
                data=df_to_excel(df),
                file_name=f"fii_dii_{ts_str}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )

    with tab2:
        st.markdown("### Raw API Response")
        st.json(raw, expanded=2)
        st.download_button(
            "⬇️ Raw JSON",
            data=json.dumps(raw, indent=2).encode(),
            file_name=f"fii_dii_raw_{ts_str}.json",
            mime="application/json",
        )

    with tab3:
        st.markdown("### Session Fetch Log")
        if st.session_state.fetch_history:
            st.dataframe(
                pd.DataFrame(st.session_state.fetch_history),
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.markdown(
                "<p style='color:#484f58;'>No fetches yet this session.</p>",
                unsafe_allow_html=True,
            )

elif df is not None and df.empty:
    st.warning(
        "⚠️ API responded but returned no rows. "
        "Try reducing the date range or check the segment selection."
    )
    if raw:
        st.markdown("**Raw response (for debugging):**")
        st.json(raw)

else:
    st.markdown("""
<div style="text-align:center;padding:4rem 2rem;border:1px dashed #21262d;
            border-radius:12px;margin-top:2rem;">
    <div style="font-size:3rem;margin-bottom:1rem;">📡</div>
    <div style="color:#8b949e;font-size:1rem;font-family:'IBM Plex Mono',monospace;">
        Awaiting data fetch
    </div>
    <div style="color:#484f58;font-size:0.8rem;margin-top:0.5rem;">
        Paste your Auth token in the sidebar and click Fetch Data
    </div>
</div>
""", unsafe_allow_html=True)
