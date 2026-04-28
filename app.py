import streamlit as st
import requests
import pandas as pd
import json
from datetime import datetime, date, timedelta
import io

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
</style>
""", unsafe_allow_html=True)

# ── CONSTANTS ────────────────────────────────────────────────────────────────

API_URL = "https://static-scanx.dhan.co/staticscanx/fiidiidata"

SEGMENT_OPTIONS = {
    "Equity": "equity",
    "Futures & Options": "fno",
    "Debt": "debt",
    "Hybrid": "hybrid",
}

TIMEFRAME_OPTIONS = {
    "Daily": "D",
    "Weekly": "W",
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

def build_payload(segment, from_dt, to_dt, timeframe):
    """
    Exact structure confirmed from DevTools Payload tab:
    {
      "data": {
        "startdate":   "28-03-2026",   <- DD-MM-YYYY
        "enddate":     "28-04-2026",
        "defaultpage": "N",
        "segment":     "equity",
        "TimeFrame":   "D"
      }
    }
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


def fetch_fiidii(token, payload):
    hdrs = {
        "Auth": token.strip(),
        "Content-Type": "application/json",
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://scanx.trade",
        "Referer": "https://scanx.trade/insight/fii-dii-data",
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/146.0.0.0 Safari/537.36"
        ),
    }
    return requests.post(API_URL, headers=hdrs, json=payload, timeout=15)


def parse_response(data):
    if isinstance(data, list):
        return pd.DataFrame(data)
    if isinstance(data, dict):
        for key in ["data", "result", "records", "fiidii", "response"]:
            if key in data and isinstance(data[key], list):
                return pd.DataFrame(data[key])
        for v in data.values():
            if isinstance(v, list) and len(v) > 0:
                return pd.DataFrame(v)
    return pd.DataFrame()


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


def df_to_csv(df):
    return df.to_csv(index=False).encode("utf-8")


def df_to_excel(df):
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
    col1, col2 = st.columns(2)
    with col1:
        from_date = st.date_input("From", value=date.today() - timedelta(days=30))
    with col2:
        to_date = st.date_input("To", value=date.today())

    st.markdown("---")
    st.markdown("### 📂 Segment")
    segment_label = st.selectbox("Market segment", list(SEGMENT_OPTIONS.keys()))
    segment_val = SEGMENT_OPTIONS[segment_label]

    st.markdown("### 🕐 Time Frame")
    timeframe_label = st.selectbox("Frequency", list(TIMEFRAME_OPTIONS.keys()))
    timeframe_val = TIMEFRAME_OPTIONS[timeframe_label]

    st.markdown("---")
    st.markdown("### 🔧 Advanced")
    with st.expander("Custom Payload (JSON override)"):
        st.markdown("Overrides the auto-built payload. Leave empty to use defaults above.")
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
> 📅 Dates are sent as **DD-MM-YYYY** (e.g. `28-04-2026`) — this is handled automatically.
""")

st.markdown("---")

# ── FETCH LOGIC ──────────────────────────────────────────────────────────────

if fetch_btn:
    if not token or len(token.strip()) < 20:
        st.error("⚠️ Please paste a valid Auth token in the sidebar.")
    elif from_date > to_date:
        st.error("⚠️ 'From' date must be earlier than 'To' date.")
    else:
        if custom_payload_str.strip():
            try:
                payload = json.loads(custom_payload_str)
            except json.JSONDecodeError as e:
                st.error(f"Invalid JSON in custom payload: {e}")
                st.stop()
        else:
            payload = build_payload(segment_val, from_date, to_date, timeframe_val)

        with st.spinner(f"Fetching {segment_label} ({timeframe_label}) data..."):
            try:
                resp = fetch_fiidii(token, payload)
                ts = datetime.now().strftime("%H:%M:%S")

                if resp.status_code == 200:
                    raw = resp.json()
                    df = parse_response(raw)
                    st.session_state.last_df = df
                    st.session_state.last_raw = raw
                    st.session_state.fetch_history.append({
                        "Time": ts,
                        "Segment": segment_label,
                        "TimeFrame": timeframe_label,
                        "From": from_date.strftime("%d-%m-%Y"),
                        "To": to_date.strftime("%d-%m-%Y"),
                        "Rows": len(df),
                        "Status": "✅ OK",
                    })
                    st.success(f"✅ Fetched **{len(df)} records** at {ts}")

                elif resp.status_code == 401:
                    st.error("🔐 **401 Unauthorized** — Token has expired. Grab a fresh one from DevTools.")
                    st.session_state.fetch_history.append({
                        "Time": ts, "Segment": segment_label, "TimeFrame": timeframe_label,
                        "From": "", "To": "", "Rows": 0, "Status": "❌ 401",
                    })
                elif resp.status_code == 403:
                    st.error("🚫 **403 Forbidden** — Make sure you're logged in to ScanX.")
                else:
                    st.error(f"❌ HTTP {resp.status_code}: {resp.text[:400]}")

            except requests.exceptions.ConnectionError:
                st.error("🌐 Connection error — check your internet connection.")
            except requests.exceptions.Timeout:
                st.error("⏱️ Request timed out (15s). Please try again.")
            except Exception as e:
                st.error(f"Unexpected error: {e}")

# ── RESULTS ──────────────────────────────────────────────────────────────────

df = st.session_state.last_df
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
            st.markdown("<p style='color:#484f58;'>No fetches yet this session.</p>",
                        unsafe_allow_html=True)

elif df is not None and df.empty:
    st.warning(
        "⚠️ API responded but returned no rows. "
        "Try the **Custom Payload** override in the sidebar to adjust fields."
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
