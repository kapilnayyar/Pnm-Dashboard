import streamlit as st
import requests
import gspread
from google.oauth2.service_account import Credentials
from collections import Counter
from datetime import datetime, timedelta
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
.updated { font-size: 11px; color: #666; text-align: right; margin-bottom: 6px; }
</style>
""", unsafe_allow_html=True)

# ── Credentials ───────────────────────────────────────────────────────────────
def get_secrets():
    try:
        return {
            "railway_url":    st.secrets["RAILWAY_APP_URL"],
            "railway_email":  st.secrets["RAILWAY_EMAIL"],
            "railway_pass":   st.secrets["RAILWAY_PASSWORD"],
            "sheet_id":       st.secrets["GOOGLE_SHEET_ID"],
            "gcp_creds":      dict(st.secrets["gcp_service_account"]),
            "metabase_url":   st.secrets["METABASE_URL"],
            "metabase_key":   st.secrets["METABASE_API_KEY"],
        }
    except Exception:
        from dotenv import load_dotenv
        BASE = os.path.dirname(os.path.abspath(__file__))
        load_dotenv(os.path.join(BASE, ".env"))
        # Also load global .env for Metabase key
        home_env = os.path.join(os.path.expanduser("~"), ".env")
        if os.path.exists(home_env):
            load_dotenv(home_env)
        with open(os.path.join(BASE, "google_credentials.json")) as f:
            gcp = json.load(f)
        return {
            "railway_url":   os.getenv("RAILWAY_APP_URL"),
            "railway_email": os.getenv("RAILWAY_EMAIL"),
            "railway_pass":  os.getenv("RAILWAY_PASSWORD"),
            "sheet_id":      os.getenv("GOOGLE_SHEET_ID"),
            "gcp_creds":     gcp,
            "metabase_url":  "https://metabase.wiom.in",
            "metabase_key":  os.getenv("METABASE_API_KEY", ""),
        }

# ── Google Sheet fetch (col A = partner ID, col N = calling status) ───────────
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

    col_a = sheet.col_values(1)[1:]   # Partner Account IDs
    col_n = sheet.col_values(15)[1:]  # Calling status (Remarks Dropdown — column O)
    col_p = sheet.col_values(16)[1:]  # PSH Remark

    col_n = ["Appointment Scheduled" if v == "Appointment Confirmed" else v for v in col_n]

    # Build partner_id → calling_status mapping
    partner_calling = {}
    for pid, status in zip(col_a, col_n):
        pid = str(pid).strip()
        if pid:
            partner_calling[pid] = status

    return Counter(col_n), Counter(col_p), partner_calling

# ── Metabase userbase fetch (active customers per partner from Snowflake) ─────
@st.cache_data(ttl=300)
def fetch_userbase(metabase_url, api_key):
    try:
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        query = (
            f"select partner_account_id, sum(ACTIVE_R15_CUSTOMERS) as active_customers "
            f"from customer_base where DATE = '{yesterday}' group by all"
        )
        resp = requests.post(
            f"{metabase_url}/api/dataset",
            headers={"x-api-key": api_key, "Content-Type": "application/json"},
            json={"database": 113, "type": "native", "native": {"query": query}},
            timeout=30
        )
        data = resp.json()
        rows = data.get("data", {}).get("rows", [])
        # Returns {partner_account_id: active_customers}
        return {str(r[0]).strip(): int(r[1] or 0) for r in rows if r[0]}
    except Exception:
        return {}

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
            return None, {}, f"Login failed ({r.status_code})"
        partners = session.get(f"{url}/api/partners", timeout=30).json()
        if isinstance(partners, dict):
            partners = next((v for v in partners.values() if isinstance(v, list)), [])

        counts = {"activation_done": 0, "rescheduled": 0, "denied": 0, "not_available": 0}
        # Map partner_account_id → activation_status
        partner_activation = {}
        for p in partners:
            cases = p.get("cases", [])
            if not cases:
                continue
            latest = sorted(cases, key=lambda c: c.get("assigned_at") or "", reverse=True)[0]
            status = latest.get("status", "")
            if status in counts:
                counts[status] += 1
            # Try common partner ID field names
            pid = (
                str(p.get("partner_account_id") or
                    p.get("account_id") or
                    p.get("partnerId") or
                    p.get("id") or "").strip()
            )
            if pid and status in counts:
                partner_activation[pid] = status

        return counts, partner_activation, None
    except Exception as e:
        return None, {}, str(e)

# ── Sum userbase for a set of partner IDs ────────────────────────────────────
def ub(partner_ids, userbase_map):
    total = sum(userbase_map.get(pid, 0) for pid in partner_ids)
    return f"{total:,}" if total > 0 else "—"

# ── Build funnel numbers ──────────────────────────────────────────────────────
def build(calling, railway, partner_calling, partner_activation, userbase_map):
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

    # Partner ID sets by calling status
    all_pids      = set(partner_calling.keys())
    called_pids   = {pid for pid, s in partner_calling.items() if s in (CONNECTED_S | NOT_CONN_S)}
    not_called_pids = all_pids - called_pids
    conn_pids     = {pid for pid, s in partner_calling.items() if s in CONNECTED_S}
    not_conn_pids = {pid for pid, s in partner_calling.items() if s in NOT_CONN_S}
    appt_pids     = {pid for pid, s in partner_calling.items() if s == "Appointment Scheduled"}
    not_sched_pids= conn_pids - appt_pids
    cbl_pids      = {pid for pid, s in partner_calling.items() if s == "Call Back Later"}
    denied_c_pids = {pid for pid, s in partner_calling.items() if s == "Denied"}
    oot_pids      = {pid for pid, s in partner_calling.items() if s == "Out of Town"}
    shifted_pids  = {pid for pid, s in partner_calling.items() if s == "Shifted to Other Partner"}
    mail_pids     = {pid for pid, s in partner_calling.items() if s == "Px Asking Details on Mail"}
    wrong_pids    = {pid for pid, s in partner_calling.items() if s == "Wrong Number"}
    dnp_pids      = {pid for pid, s in partner_calling.items() if s == "DNP"}
    nc_pids       = {pid for pid, s in partner_calling.items() if s == "Not Contactable"}

    # Activation partner sets (from Railway)
    act_pids      = {pid for pid, s in partner_activation.items() if s == "activation_done"}
    resch_pids    = {pid for pid, s in partner_activation.items() if s == "rescheduled"}
    denied_p_pids = {pid for pid, s in partner_activation.items() if s == "denied"}
    na_pids       = {pid for pid, s in partner_activation.items() if s == "not_available"}
    ytv_pids      = appt_pids - act_pids - resch_pids - denied_p_pids - na_pids

    return {
        "eligible":        (ELIGIBLE,      ub(all_pids,       userbase_map)),
        "calls_made":      (calls_made,    ub(called_pids,    userbase_map)),
        "not_called":      (not_called,    ub(not_called_pids,userbase_map)),
        "connected":       (connected,     ub(conn_pids,      userbase_map)),
        "not_connected":   (not_connected, ub(not_conn_pids,  userbase_map)),
        "dnp":             (calling["DNP"],ub(dnp_pids,       userbase_map)),
        "not_contactable": (calling["Not Contactable"], ub(nc_pids, userbase_map)),
        "appt_sched":      (appt_sched,    ub(appt_pids,      userbase_map)),
        "not_sched":       (not_sched,     ub(not_sched_pids, userbase_map)),
        "ns_cbl":          (calling["Call Back Later"],          ub(cbl_pids,     userbase_map)),
        "ns_denied":       (calling["Denied"],                   ub(denied_c_pids,userbase_map)),
        "ns_oot":          (calling["Out of Town"],              ub(oot_pids,     userbase_map)),
        "ns_shifted":      (calling.get("Shifted to Other Partner", 0), ub(shifted_pids, userbase_map)),
        "ns_mail":         (calling["Px Asking Details on Mail"],ub(mail_pids,    userbase_map)),
        "ns_wrong":        (calling["Wrong Number"],             ub(wrong_pids,   userbase_map)),
        "pnm_activated":   (pnm_activated, ub(act_pids,       userbase_map)),
        "not_activated":   (max(appt_sched - pnm_activated, 0), ub(appt_pids - act_pids, userbase_map)),
        "yet_to_visit":    (yet_to_visit,  ub(ytv_pids,       userbase_map)),
        "rescheduled":     (rescheduled,   ub(resch_pids,     userbase_map)),
        "denied_pnm":      (denied_pnm,    ub(denied_p_pids,  userbase_map)),
        "not_available":   (not_available, ub(na_pids,        userbase_map)),
    }

# ── HTML rows (3 columns: METRIC | COUNT | USERBASE) ─────────────────────────
def title_r(label, count, userbase, bg, white_text=False):
    fc = "#ffffff" if white_text else "#000000"
    return (
        f'<tr>'
        f'<td style="background:{bg};color:{fc};font-weight:bold">{label}</td>'
        f'<td style="background:{bg};color:{fc};font-weight:bold;text-align:right;width:80px">{count:,}</td>'
        f'<td style="background:{bg};color:{fc};font-weight:bold;text-align:right;width:90px">{userbase}</td>'
        f'</tr>'
    )

def plain_r(label, count, userbase, bg="#ffffff"):
    return (
        f'<tr>'
        f'<td style="background:{bg};color:#000000">{label}</td>'
        f'<td style="background:{bg};color:#000000;text-align:right;width:80px;font-weight:bold">{count:,}</td>'
        f'<td style="background:{bg};color:#000000;text-align:right;width:90px">{userbase}</td>'
        f'</tr>'
    )

def sub_r(label, count, userbase, bg="#ffffff"):
    return (
        f'<tr>'
        f'<td style="background:{bg};color:#333333;padding-left:26px">&#x21B3; {label}</td>'
        f'<td style="background:{bg};color:#333333;text-align:right;width:80px">{count:,}</td>'
        f'<td style="background:{bg};color:#333333;text-align:right;width:90px">{userbase}</td>'
        f'</tr>'
    )

def gap_r():
    return '<tr><td colspan="3" style="padding:2px;border:none;background:#f0f2f6"></td></tr>'

# ── Render ────────────────────────────────────────────────────────────────────
def render():
    secrets = get_secrets()

    with st.spinner("Fetching live data..."):
        calling, _, partner_calling = fetch_sheet(secrets["sheet_id"], secrets["gcp_creds"])
        railway, partner_activation, err = fetch_railway(
            secrets["railway_url"], secrets["railway_email"], secrets["railway_pass"]
        )
        userbase_map = fetch_userbase(secrets["metabase_url"], secrets["metabase_key"])

    if err:
        st.warning(f"⚠️ Railway: {err}")
        railway = {}

    f = build(calling, railway or {}, partner_calling, partner_activation, userbase_map)

    updated = datetime.now().strftime("%d-%b-%Y %H:%M")

    st.markdown(
        f'<div style="background:#1F3864;color:#ffffff;padding:12px;border-radius:6px;'
        f'font-size:16px;font-weight:bold;text-align:center;margin-bottom:4px">'
        f'PNM ACTIVATION FUNNEL &nbsp;|&nbsp; As of {updated}</div>',
        unsafe_allow_html=True
    )
    st.markdown('<div class="updated">Live — auto-refreshes every 30 seconds</div>', unsafe_allow_html=True)

    html = (
        '<table class="funnel-table">'
        '<tr>'
        '<td style="background:#2F5496;color:#ffffff;font-weight:bold">METRIC</td>'
        '<td style="background:#2F5496;color:#ffffff;font-weight:bold;text-align:right;width:80px">PARTNERS</td>'
        '<td style="background:#2F5496;color:#ffffff;font-weight:bold;text-align:right;width:90px">USERBASE</td>'
        '</tr>'
    )

    c, u = lambda k: f[k][0], lambda k: f[k][1]

    # Call section
    html += plain_r("Eligible CSP",       c("eligible"),      u("eligible"),      "#D6E4F0")
    html += title_r("Total Calls Made",   c("calls_made"),    u("calls_made"),    "#2E75B6", white_text=True)
    html += plain_r("Not Yet Called",     c("not_called"),    u("not_called"),    "#D6E4F0")
    html += gap_r()
    html += plain_r("Connected",          c("connected"),     u("connected"),     "#D6E4F0")
    html += plain_r("Not Connected / DNP",c("not_connected"), u("not_connected"), "#D6E4F0")
    html += sub_r  ("DNP (Did Not Pick)", c("dnp"),           u("dnp"),           "#EBF3FB")
    html += sub_r  ("Not Contactable",    c("not_contactable"),u("not_contactable"),"#EBF3FB")
    html += gap_r()

    # Appointment section
    html += title_r("Appointment Scheduled  (from Connected)",     c("appt_sched"), u("appt_sched"), "#ED7D31", white_text=True)
    html += plain_r("Appointment Not Scheduled  (from Connected)", c("not_sched"),  u("not_sched"),  "#FCE4D6")
    html += sub_r  ("Call Back Later",          c("ns_cbl"),     u("ns_cbl"),     "#FEF4EE")
    html += sub_r  ("Denied",                   c("ns_denied"),  u("ns_denied"),  "#FEF4EE")
    html += sub_r  ("Out of Town",              c("ns_oot"),     u("ns_oot"),     "#FEF4EE")
    html += sub_r  ("Shifted to Other Partner", c("ns_shifted"), u("ns_shifted"), "#FEF4EE")
    html += sub_r  ("Asking Details on Mail",   c("ns_mail"),    u("ns_mail"),    "#FEF4EE")
    html += sub_r  ("Wrong Number",             c("ns_wrong"),   u("ns_wrong"),   "#FEF4EE")
    html += gap_r()

    # Activation section
    html += title_r("PNM Activated  (from Appointment Scheduled)", c("pnm_activated"), u("pnm_activated"), "#375623", white_text=True)
    html += plain_r("Not Activated  (from Appointment Scheduled)", c("not_activated"), u("not_activated"), "#FFC7CE")
    html += sub_r  ("Visit Yet to Happen", c("yet_to_visit"),  u("yet_to_visit"),  "#FFE2E2")
    html += sub_r  ("Rescheduled",         c("rescheduled"),   u("rescheduled"),   "#FFE2E2")
    html += sub_r  ("Denied",              c("denied_pnm"),    u("denied_pnm"),    "#FFE2E2")
    html += sub_r  ("Not Available",       c("not_available"), u("not_available"), "#FFE2E2")

    html += "</table>"
    st.markdown(html, unsafe_allow_html=True)

render()
