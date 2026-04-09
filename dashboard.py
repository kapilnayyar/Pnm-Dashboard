import streamlit as st
import requests
import gspread
from google.oauth2.service_account import Credentials
from collections import Counter
from datetime import datetime
from streamlit_autorefresh import st_autorefresh
import json
import os

st.set_page_config(page_title="PNM Activation Funnel", layout="centered")

# Hide GitHub icon, toolbar, and footer
st.markdown("""
<style>
[data-testid="stToolbar"] {visibility: hidden !important;}
[data-testid="stDecoration"] {display: none !important;}
footer {visibility: hidden !important;}
#MainMenu {visibility: hidden !important;}
</style>
""", unsafe_allow_html=True)

# ── Login page ────────────────────────────────────────────────────────────────
if "authenticated" not in st.session_state:
    st.session_state.authenticated = False

if not st.session_state.authenticated:
    st.markdown("""
    <style>
    .login-title {
        background: #1F3864;
        color: white;
        padding: 16px;
        border-radius: 8px;
        text-align: center;
        font-size: 18px;
        font-weight: bold;
        margin-bottom: 8px;
    }
    .login-sub {
        text-align: center;
        color: #555;
        font-size: 13px;
        margin-bottom: 24px;
    }
    </style>
    <div class="login-title">PNM ACTIVATION FUNNEL</div>
    <div class="login-sub">Wiom Internal Dashboard — Restricted Access</div>
    """, unsafe_allow_html=True)

    col1, col2, col3 = st.columns([1, 3, 1])
    with col2:
        email    = st.text_input("Wiom Email", placeholder="name@wiom.in")
        password = st.text_input("Password", type="password")

        if st.button("Login", use_container_width=True):
            try:
                correct_pw = st.secrets["APP_PASSWORD"]
            except Exception:
                from dotenv import load_dotenv
                load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))
                correct_pw = os.getenv("APP_PASSWORD", "")

            clean_email = email.strip().lower()
            valid_domain = clean_email.endswith("@wiom.in") or clean_email.endswith("@i2e1.com")

            if not valid_domain:
                st.error("Access restricted to @wiom.in and @i2e1.com emails only.")
            elif password.strip() != correct_pw.strip():
                st.error("Incorrect password. Please try again.")
            else:
                st.session_state.authenticated = True
                st.session_state.user_email = clean_email
                st.rerun()
    st.stop()

# ── Top bar: title + logout ───────────────────────────────────────────────────
col_title, col_logout = st.columns([5, 1])
with col_title:
    st.markdown(
        "<div style='font-size:20px;font-weight:bold;color:#1F3864;padding-top:6px'>"
        "PNM Activation Funnel</div>",
        unsafe_allow_html=True
    )
with col_logout:
    if st.button("Logout", use_container_width=True):
        st.session_state.authenticated = False
        st.session_state.user_email = ""
        st.rerun()

# Auto-refresh every 30 seconds
st_autorefresh(interval=30000)

st.markdown("""
<style>
.funnel-table {
    width: 100%;
    border-collapse: collapse;
    font-family: Arial, sans-serif;
    font-size: 13px;
}
.funnel-table td {
    padding: 8px 12px;
    border: 1px solid #cccccc;
    color: #000000;
}
.funnel-table .count-col {
    text-align: right;
    width: 80px;
    font-weight: bold;
    color: #000000;
}
.funnel-table .count-col-white {
    text-align: right;
    width: 80px;
    font-weight: bold;
    color: #ffffff;
}
.updated { font-size: 11px; color: #666; text-align: right; margin-bottom: 6px; }
</style>
""", unsafe_allow_html=True)

# ── Credentials ───────────────────────────────────────────────────────────────
def get_secrets():
    try:
        return {
            "railway_url":   st.secrets["RAILWAY_APP_URL"],
            "railway_email": st.secrets["RAILWAY_EMAIL"],
            "railway_pass":  st.secrets["RAILWAY_PASSWORD"],
            "sheet_id":      st.secrets["GOOGLE_SHEET_ID"],
            "gcp_creds":     dict(st.secrets["gcp_service_account"])
        }
    except Exception:
        from dotenv import load_dotenv
        BASE = os.path.dirname(os.path.abspath(__file__))
        load_dotenv(os.path.join(BASE, ".env"))
        with open(os.path.join(BASE, "google_credentials.json")) as f:
            gcp = json.load(f)
        return {
            "railway_url":   os.getenv("RAILWAY_APP_URL"),
            "railway_email": os.getenv("RAILWAY_EMAIL"),
            "railway_pass":  os.getenv("RAILWAY_PASSWORD"),
            "sheet_id":      os.getenv("GOOGLE_SHEET_ID"),
            "gcp_creds":     gcp
        }

# ── Google Sheet fetch ────────────────────────────────────────────────────────
@st.cache_data(ttl=30)
def fetch_sheet(sheet_id, gcp_creds):
    creds = Credentials.from_service_account_info(
        gcp_creds,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets.readonly",
            "https://www.googleapis.com/auth/drive.readonly"
        ]
    )
    client = gspread.authorize(creds)
    sheet  = client.open_by_key(sheet_id).sheet1

    # Column N = Remarks Dropdown (calling status)
    col_n = sheet.col_values(14)[1:]
    col_n = ["Appointment Scheduled" if v == "Appointment Confirmed" else v for v in col_n]

    # Column P = PSH Remark (activation status)
    col_p = sheet.col_values(16)[1:]

    return Counter(col_n), Counter(col_p)

# ── Railway fetch ─────────────────────────────────────────────────────────────
@st.cache_data(ttl=30)
def fetch_railway(url, email, password):
    try:
        session = requests.Session()
        r = session.post(
            f"{url}/api/auth/login",
            json={"email": email, "password": password},
            timeout=15
        )
        if r.status_code != 200:
            return None, f"Login failed ({r.status_code})"
        partners = session.get(f"{url}/api/partners", timeout=30).json()
        if isinstance(partners, dict):
            partners = next((v for v in partners.values() if isinstance(v, list)), [])

        # Per partner — use latest case status
        counts = {"activation_done": 0, "rescheduled": 0, "denied": 0, "not_available": 0}
        for p in partners:
            cases = p.get("cases", [])
            if not cases:
                continue
            latest = sorted(cases, key=lambda c: c.get("assigned_at") or "", reverse=True)[0]
            status = latest.get("status", "")
            if status in counts:
                counts[status] += 1

        return counts, None
    except Exception as e:
        return None, str(e)

# ── Build funnel numbers ──────────────────────────────────────────────────────
def build(calling, railway):
    ELIGIBLE = 1201

    CONNECTED_S = {
        "Appointment Scheduled", "Call Back Later", "Denied",
        "Out of Town", "Px Asking Details on Mail", "Wrong Number",
        "Shifted to Other Partner"
    }
    NOT_CONN_S = {"DNP", "Not Contactable"}

    connected     = sum(calling[k] for k in CONNECTED_S)
    not_connected = sum(calling[k] for k in NOT_CONN_S)
    calls_made    = connected + not_connected
    not_called    = ELIGIBLE - calls_made

    appt_sched    = calling["Appointment Scheduled"]
    not_sched     = connected - appt_sched

    pnm_activated = railway.get("activation_done", 0)
    rescheduled   = railway.get("rescheduled", 0)
    denied_pnm    = railway.get("denied", 0)
    not_available = railway.get("not_available", 0)
    yet_to_visit  = max(appt_sched - pnm_activated - rescheduled - denied_pnm - not_available, 0)

    return {
        "eligible":       ELIGIBLE,
        "calls_made":     calls_made,
        "not_called":     not_called,
        "connected":      connected,
        "not_connected":  not_connected,
        "dnp":            calling["DNP"],
        "not_contactable":calling["Not Contactable"],
        "appt_sched":     appt_sched,
        "not_sched":      not_sched,
        "ns_cbl":         calling["Call Back Later"],
        "ns_denied":      calling["Denied"],
        "ns_oot":         calling["Out of Town"],
        "ns_shifted":     calling.get("Shifted to Other Partner", 0),
        "ns_mail":        calling["Px Asking Details on Mail"],
        "ns_wrong":       calling["Wrong Number"],
        "pnm_activated":  pnm_activated,
        "not_activated":  max(appt_sched - pnm_activated, 0),
        "yet_to_visit":   yet_to_visit,
        "rescheduled":    rescheduled,
        "denied_pnm":     denied_pnm,
        "not_available":  not_available,
    }

# ── HTML rows ─────────────────────────────────────────────────────────────────
def title_r(label, count, bg, white_text=False):
    fc  = "#ffffff" if white_text else "#000000"
    ffw = "bold"
    return (
        f'<tr>'
        f'<td style="background:{bg};color:{fc};font-weight:{ffw}">{label}</td>'
        f'<td style="background:{bg};color:{fc};font-weight:{ffw};text-align:right;width:80px">{count:,}</td>'
        f'</tr>'
    )

def plain_r(label, count, bg="#ffffff"):
    return (
        f'<tr>'
        f'<td style="background:{bg};color:#000000">{label}</td>'
        f'<td style="background:{bg};color:#000000;text-align:right;width:80px;font-weight:bold">{count:,}</td>'
        f'</tr>'
    )

def sub_r(label, count, bg="#ffffff"):
    return (
        f'<tr>'
        f'<td style="background:{bg};color:#333333;padding-left:26px">'
        f'&#x21B3; {label}</td>'
        f'<td style="background:{bg};color:#333333;text-align:right;width:80px">{count:,}</td>'
        f'</tr>'
    )

def gap_r():
    return '<tr><td colspan="2" style="padding:2px;border:none;background:#f0f2f6"></td></tr>'

# ── Render ────────────────────────────────────────────────────────────────────
def render():
    secrets = get_secrets()

    with st.spinner("Fetching live data..."):
        calling, _      = fetch_sheet(secrets["sheet_id"], secrets["gcp_creds"])
        railway, err    = fetch_railway(
            secrets["railway_url"], secrets["railway_email"], secrets["railway_pass"]
        )

    if err:
        st.warning(f"⚠️ Railway: {err}")
        railway = {}

    f = build(calling, railway)

    updated = datetime.now().strftime("%d-%b-%Y %H:%M")

    # Title
    st.markdown(
        f'<div style="background:#1F3864;color:#ffffff;padding:12px;border-radius:6px;'
        f'font-size:16px;font-weight:bold;text-align:center;margin-bottom:4px">'
        f'PNM ACTIVATION FUNNEL &nbsp;|&nbsp; As of {updated}</div>',
        unsafe_allow_html=True
    )
    st.markdown(f'<div class="updated">Live — auto-refreshes every 30 seconds</div>', unsafe_allow_html=True)

    # Table header
    html = (
        '<table class="funnel-table">'
        '<tr>'
        '<td style="background:#2F5496;color:#ffffff;font-weight:bold">METRIC</td>'
        '<td style="background:#2F5496;color:#ffffff;font-weight:bold;text-align:right;width:80px">COUNT</td>'
        '</tr>'
    )

    # Call section
    html += plain_r("Eligible CSP",                      f["eligible"],      "#D6E4F0")
    html += title_r("Total Calls Made",                  f["calls_made"],    "#2E75B6", white_text=True)
    html += plain_r("Not Yet Called",                    f["not_called"],    "#D6E4F0")
    html += gap_r()
    html += plain_r("Connected",                         f["connected"],     "#D6E4F0")
    html += plain_r("Not Connected / DNP",               f["not_connected"], "#D6E4F0")
    html += sub_r  ("DNP (Did Not Pick)",                f["dnp"],           "#EBF3FB")
    html += sub_r  ("Not Contactable",                   f["not_contactable"],"#EBF3FB")
    html += gap_r()

    # Appointment section
    html += title_r("Appointment Scheduled  (from Connected)",     f["appt_sched"], "#ED7D31", white_text=True)
    html += plain_r("Appointment Not Scheduled  (from Connected)", f["not_sched"],  "#FCE4D6")
    html += sub_r  ("Call Back Later",                             f["ns_cbl"],     "#FEF4EE")
    html += sub_r  ("Denied",                                      f["ns_denied"],  "#FEF4EE")
    html += sub_r  ("Out of Town",                                 f["ns_oot"],     "#FEF4EE")
    html += sub_r  ("Shifted to Other Partner",                    f["ns_shifted"], "#FEF4EE")
    html += sub_r  ("Asking Details on Mail",                      f["ns_mail"],    "#FEF4EE")
    html += sub_r  ("Wrong Number",                                f["ns_wrong"],   "#FEF4EE")
    html += gap_r()

    # Activation section
    html += title_r("PNM Activated  (from Appointment Scheduled)", f["pnm_activated"], "#375623", white_text=True)
    html += plain_r("Not Activated  (from Appointment Scheduled)", f["not_activated"], "#FFC7CE")
    html += sub_r  ("Visit Yet to Happen",                         f["yet_to_visit"],  "#FFE2E2")
    html += sub_r  ("Rescheduled",                                 f["rescheduled"],   "#FFE2E2")
    html += sub_r  ("Denied",                                      f["denied_pnm"],    "#FFE2E2")
    html += sub_r  ("Not Available",                               f["not_available"], "#FFE2E2")

    html += "</table>"
    st.markdown(html, unsafe_allow_html=True)

render()
