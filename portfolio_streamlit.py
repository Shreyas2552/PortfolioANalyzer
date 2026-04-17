"""
Portfolio Analyzer — Streamlit Web Edition
==========================================
Run locally:  streamlit run portfolio_streamlit.py
Deploy:       push to GitHub → connect at share.streamlit.io
Mobile:       open the Streamlit Cloud URL on any device
"""

import time, warnings, requests
import streamlit as st

warnings.filterwarnings("ignore")

try:
    import yfinance as yf
    YF_AVAILABLE = True
except ImportError:
    YF_AVAILABLE = False

# ── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Portfolio Analyzer",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Mobile-friendly dark theme ───────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
html, body, [class*="css"] { font-family: 'Inter', sans-serif; }
.block-container { padding: 1rem 1rem 2rem; max-width: 900px; }

/* Score card */
.score-card {
    background: #161b22; border: 1px solid #30363d;
    border-radius: 14px; padding: 1.1rem 1.2rem;
    margin-bottom: 0.75rem;
}
.score-card:hover { border-color: #58a6ff; }
.ticker-label { font-size: 1.2rem; font-weight: 700; color: #e6edf3; }
.name-label   { font-size: 0.8rem; color: #8b949e; margin-top: 1px; }
.score-big    { font-size: 2.4rem; font-weight: 800; font-family: monospace; line-height: 1; }
.score-buy    { color: #3fb950; }
.score-hold   { color: #d29922; }
.score-sell   { color: #ff7b72; }
.verdict-pill {
    display: inline-block; padding: 3px 12px; border-radius: 20px;
    font-size: 0.78rem; font-weight: 700; margin-top: 4px;
}
.pill-buy  { background: #0d3321; color: #3fb950; border: 1px solid #1a4731; }
.pill-hold { background: #2d1f00; color: #d29922; border: 1px solid #453001; }
.pill-sell { background: #2d0f0f; color: #ff7b72; border: 1px solid #4a1515; }

/* Criteria bar */
.crit-row { display: flex; align-items: center; gap: 8px; margin: 4px 0; font-size: 0.82rem; }
.crit-name { color: #8b949e; width: 160px; flex-shrink: 0; }
.crit-bar-bg { flex: 1; height: 7px; background: #21262d; border-radius: 4px; overflow: hidden; }
.crit-bar-fill { height: 100%; border-radius: 4px; }
.bar-high { background: #3fb950; }
.bar-mid  { background: #d29922; }
.bar-low  { background: #ff7b72; }
.crit-score { color: #c9d1d9; font-weight: 600; width: 36px; text-align: right; font-family: monospace; }
.crit-note  { color: #6e7681; font-size: 0.75rem; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; max-width: 200px; }

/* Warning / caution */
.warn-box {
    background: #2d1f00; border: 1px solid #bb8009;
    border-radius: 8px; padding: 0.6rem 0.9rem;
    font-size: 0.82rem; color: #d29922; margin-top: 8px;
}
.danger-box {
    background: #2d0f0f; border: 1px solid #da3633;
    border-radius: 8px; padding: 0.6rem 0.9rem;
    font-size: 0.82rem; color: #ff7b72; margin-top: 8px;
}
.upside-box {
    background: #0d3321; border: 1px solid #238636;
    border-radius: 8px; padding: 0.6rem 0.9rem;
    font-size: 0.82rem; color: #3fb950; margin-top: 8px;
}

/* Metric chip */
.metric-chip {
    display: inline-block; background: #0d1117; border: 1px solid #21262d;
    border-radius: 6px; padding: 3px 9px; margin: 3px 3px 3px 0;
    font-size: 0.75rem; color: #8b949e;
}
.metric-chip b { color: #c9d1d9; }

/* Data quality badge */
.dq-high   { background:#0d3321; color:#3fb950; border:1px solid #1a4731; padding:2px 8px; border-radius:10px; font-size:0.72rem; font-weight:600; }
.dq-medium { background:#2d1f00; color:#d29922; border:1px solid #453001; padding:2px 8px; border-radius:10px; font-size:0.72rem; font-weight:600; }
.dq-low    { background:#2d0f0f; color:#ff7b72; border:1px solid #4a1515; padding:2px 8px; border-radius:10px; font-size:0.72rem; font-weight:600; }

div[data-testid="stExpander"] { border: 1px solid #21262d !important; border-radius: 10px !important; }
</style>
""", unsafe_allow_html=True)

# ── API Keys — st.secrets in cloud, hardcoded fallback for local dev ─────────
try:
    FINNHUB_KEY = st.secrets["FINNHUB_KEY"]
    AV_KEY      = st.secrets["AV_KEY"]
except Exception:
    FINNHUB_KEY = "d787fi1r01qsamsj83h0d787fi1r01qsamsj83hg"
    AV_KEY      = "DRFXF3KZF59XEVA4"

# ── API / timing config ──────────────────────────────────────────────────────
FH_BASE        = "https://finnhub.io/api/v1"
AV_BASE        = "https://www.alphavantage.co/query"
TIMEOUT        = 15
FH_DELAY       = 0.22
AV_DELAY       = 13.0
YF_DELAY       = 0.5
RETRY_ATTEMPTS = 3
RETRY_BACKOFF  = 2.0

# ── Default portfolio ────────────────────────────────────────────────────────
DEFAULT_TICKERS = ["NVDA", "AMZN", "GOOGL", "GEV", "CAT", "F", "PCAR", "JEPQ", "VUG"]

# ── ETF database ─────────────────────────────────────────────────────────────
ETF_DB = {
    "SPY":  {"expense":0.0945,"num_holdings":503,  "category":"S&P 500"},
    "IVV":  {"expense":0.03,  "num_holdings":503,  "category":"S&P 500"},
    "VOO":  {"expense":0.03,  "num_holdings":503,  "category":"S&P 500"},
    "VTI":  {"expense":0.03,  "num_holdings":3700, "category":"Total US Market"},
    "VUG":  {"expense":0.04,  "num_holdings":150,  "category":"Large Cap Growth"},
    "QQQ":  {"expense":0.20,  "num_holdings":101,  "category":"Nasdaq-100"},
    "QQQM": {"expense":0.15,  "num_holdings":101,  "category":"Nasdaq-100"},
    "IWF":  {"expense":0.19,  "num_holdings":330,  "category":"Large Cap Growth"},
    "SPYG": {"expense":0.04,  "num_holdings":240,  "category":"Large Cap Growth"},
    "MGK":  {"expense":0.07,  "num_holdings":69,   "category":"Mega Cap Growth"},
    "VTV":  {"expense":0.04,  "num_holdings":340,  "category":"Large Cap Value"},
    "SCHD": {"expense":0.06,  "num_holdings":103,  "category":"Dividend Growth"},
    "VIG":  {"expense":0.06,  "num_holdings":338,  "category":"Dividend Growth"},
    "DGRO": {"expense":0.08,  "num_holdings":420,  "category":"Dividend Growth"},
    "DVY":  {"expense":0.38,  "num_holdings":100,  "category":"High Dividend"},
    "VYM":  {"expense":0.06,  "num_holdings":555,  "category":"High Dividend Yield"},
    "JEPI": {"expense":0.35,  "num_holdings":101,  "category":"Covered Call / S&P 500 Income"},
    "JEPQ": {"expense":0.35,  "num_holdings":93,   "category":"Covered Call / Nasdaq Income"},
    "XYLD": {"expense":0.60,  "num_holdings":503,  "category":"Covered Call / S&P 500"},
    "QYLD": {"expense":0.60,  "num_holdings":101,  "category":"Covered Call / Nasdaq"},
    "XLK":  {"expense":0.10,  "num_holdings":67,   "category":"Technology Sector"},
    "VGT":  {"expense":0.10,  "num_holdings":316,  "category":"Technology Sector"},
    "IYW":  {"expense":0.38,  "num_holdings":144,  "category":"U.S. Technology"},
    "SMH":  {"expense":0.35,  "num_holdings":26,   "category":"Semiconductors"},
    "SOXX": {"expense":0.35,  "num_holdings":30,   "category":"Semiconductors"},
    "XLF":  {"expense":0.10,  "num_holdings":74,   "category":"Financials"},
    "XLV":  {"expense":0.10,  "num_holdings":63,   "category":"Healthcare"},
    "XLE":  {"expense":0.10,  "num_holdings":23,   "category":"Energy"},
    "XLI":  {"expense":0.10,  "num_holdings":79,   "category":"Industrials"},
    "VEA":  {"expense":0.05,  "num_holdings":3900, "category":"Developed Markets ex-US"},
    "VWO":  {"expense":0.08,  "num_holdings":5800, "category":"Emerging Markets"},
    "BND":  {"expense":0.03,  "num_holdings":10000,"category":"Total US Bond Market"},
    "AGG":  {"expense":0.03,  "num_holdings":10000,"category":"Total US Bond Market"},
    "TLT":  {"expense":0.15,  "num_holdings":30,   "category":"Long-Term Treasury"},
    "GLD":  {"expense":0.40,  "num_holdings":1,    "category":"Gold"},
    "IAU":  {"expense":0.25,  "num_holdings":1,    "category":"Gold"},
    "VNQ":  {"expense":0.13,  "num_holdings":160,  "category":"US REITs"},
    "ARKK": {"expense":0.75,  "num_holdings":30,   "category":"Disruptive Innovation"},
}


# ════════════════════════════════════════════════════════════════════════════
#  CORE FUNCTIONS  (identical to portfolio_analyzer_5.py — no GUI imports)
# ════════════════════════════════════════════════════════════════════════════

def sf(d, key, default=None):
    if not isinstance(d, dict): return default
    v = d.get(key)
    if v is None or str(v).strip() in ("None", "-", "N/A", "", "nan"): return default
    try:
        f = float(str(v).replace(",", ""))
        return f if f == f else default
    except (TypeError, ValueError):
        return default


def score_range(val, good, bad, higher=True):
    if val is None: return 5
    if higher:
        if val >= good: return 10
        if val <= bad:  return 1
        return max(1, min(10, round(1 + (val - bad) / (good - bad) * 9)))
    else:
        if val <= good: return 10
        if val >= bad:  return 1
        return max(1, min(10, round(1 + (bad - val) / (bad - good) * 9)))


def fh_get(path, params):
    for attempt in range(RETRY_ATTEMPTS):
        time.sleep(FH_DELAY)
        try:
            r = requests.get(f"{FH_BASE}{path}", params=params, timeout=TIMEOUT)
            if r.status_code == 200:
                return r.json(), None
            if r.status_code in (429, 503):
                time.sleep(RETRY_BACKOFF * (attempt + 1))
                continue
            return None, f"HTTP {r.status_code}"
        except requests.exceptions.Timeout:
            if attempt < RETRY_ATTEMPTS - 1:
                time.sleep(RETRY_BACKOFF)
                continue
            return None, "Timeout after 3 attempts"
        except Exception as e:
            return None, str(e)[:80]
    return None, "Max retries exceeded"


def av_get(params):
    time.sleep(AV_DELAY)
    try:
        r = requests.get(AV_BASE, params=params, timeout=TIMEOUT)
        if r.status_code != 200:
            return None, f"HTTP {r.status_code}"
        data = r.json()
        if "Note" in data:        return None, "AV rate limit — wait 1 min"
        if "Information" in data: return None, "AV key error: " + data["Information"][:60]
        if not data:              return None, "AV returned empty response"
        return data, None
    except Exception as e:
        return None, str(e)[:80]


def yf_get_etf(ticker):
    if not YF_AVAILABLE:
        return {}, "yfinance not installed"
    time.sleep(YF_DELAY)
    try:
        info = yf.Ticker(ticker).info
        if not info or info.get("regularMarketPrice") is None:
            return {}, f"yfinance returned empty info for {ticker}"
        raw_div = info.get("dividendYield")
        raw_ytd = info.get("ytdReturn")
        return {
            "trailingPE":   info.get("trailingPE"),
            "totalAssets":  info.get("totalAssets"),
            "dividendYield": raw_div / 100 if raw_div is not None else None,
            "beta":         info.get("beta"),
            "52WeekHigh":   info.get("fiftyTwoWeekHigh"),
            "52WeekLow":    info.get("fiftyTwoWeekLow"),
            "ytdReturn":    raw_ytd / 100 if raw_ytd is not None else None,
        }, None
    except Exception as e:
        return {}, str(e)[:80]


def yf_get_stock(ticker):
    if not YF_AVAILABLE:
        return {}, "yfinance not installed"
    time.sleep(YF_DELAY)
    try:
        info = yf.Ticker(ticker).info
        if not info or info.get("regularMarketPrice") is None:
            return {}, f"yfinance returned empty info for {ticker}"
        d = {}
        if info.get("trailingPE")       is not None: d["TrailingPE"]                  = info["trailingPE"]
        if info.get("forwardPE")        is not None: d["ForwardPE"]                   = info["forwardPE"]
        if info.get("priceToBook")      is not None: d["PriceToBookRatio"]            = info["priceToBook"]
        if info.get("priceToSalesTrailing12Months") is not None:
            d["PriceToSalesRatioTTM"] = info["priceToSalesTrailing12Months"]
        if info.get("enterpriseToEbitda") is not None: d["EVToEBITDA"]               = info["enterpriseToEbitda"]
        if info.get("returnOnEquity")   is not None: d["ReturnOnEquityTTM"]           = info["returnOnEquity"]
        if info.get("profitMargins")    is not None: d["ProfitMargin"]                = info["profitMargins"]
        if info.get("operatingMargins") is not None: d["OperatingMarginTTM"]          = info["operatingMargins"]
        if info.get("grossMargins")     is not None: d["_grossMargins"]               = info["grossMargins"]
        if info.get("earningsQuarterlyGrowth") is not None:
            d["QuarterlyEarningsGrowthYOY"] = info["earningsQuarterlyGrowth"]
        if info.get("revenueGrowth")    is not None: d["QuarterlyRevenueGrowthYOY"]   = info["revenueGrowth"]
        if info.get("dividendYield")    is not None: d["DividendYield"]               = info["dividendYield"] / 100
        if info.get("payoutRatio")      is not None: d["PayoutRatio"]                 = info["payoutRatio"]
        if info.get("fiftyTwoWeekHigh") is not None: d["52WeekHigh"]                  = info["fiftyTwoWeekHigh"]
        if info.get("fiftyTwoWeekLow")  is not None: d["52WeekLow"]                   = info["fiftyTwoWeekLow"]
        return d, None
    except Exception as e:
        return {}, str(e)[:80]


def fh_price_target(ticker, fh_key):
    if YF_AVAILABLE:
        try:
            time.sleep(YF_DELAY)
            info = yf.Ticker(ticker).info
            mean = info.get("targetMeanPrice")
            high = info.get("targetHighPrice")
            low  = info.get("targetLowPrice")
            if mean and float(mean) > 0:
                return {"mean": float(mean),
                        "high": float(high) if high else None,
                        "low":  float(low)  if low  else None}, None
        except Exception:
            pass
    data, err = fh_get("/stock/price-target", {"symbol": ticker, "token": fh_key})
    if err or not data:
        return None, err or "no analyst target available"
    mean = data.get("targetMean") or data.get("targetMedian")
    high = data.get("targetHigh")
    low  = data.get("targetLow")
    if not mean:
        return None, "no analyst price target available"
    return {"mean": float(mean),
            "high": float(high) if high else None,
            "low":  float(low)  if low  else None}, None


class DataQuality:
    def __init__(self):
        self.sources_ok   = []
        self.sources_fail = []
        self.notes        = []
    def ok(self, src):         self.sources_ok.append(src)
    def fail(self, src, why):
        self.sources_fail.append(src)
        self.notes.append(f"{src}: {why}")
    @property
    def quality(self):
        n = len(self.sources_ok)
        if n >= 3: return "HIGH"
        if n == 2: return "MEDIUM"
        return "LOW"
    @property
    def summary(self):
        if not self.sources_fail: return ""
        return " | ".join(self.notes)


def score_etf(ticker, price, wk52_hi_fh, wk52_lo_fh, db_entry, av_data, yf_data, dq):
    pe_h = (sf(yf_data, "trailingPE") or sf(av_data, "TrailingPE"))
    pe_source = "live yfinance" if sf(yf_data, "trailingPE") else (
                "live AV" if sf(av_data, "TrailingPE") else "N/A")
    yf_aum = sf(yf_data, "totalAssets")
    aum_b  = (yf_aum / 1e9) if yf_aum else db_entry.get("aum_b_fallback")
    yf_div = sf(yf_data, "dividendYield")
    av_div = sf(av_data, "DividendYield")
    div_raw = yf_div or av_div or 0
    div_pct = div_raw * 100
    av_beta = sf(av_data, "Beta")
    yf_beta = sf(yf_data, "beta")
    beta    = av_beta if av_beta is not None else yf_beta
    av_52hi = sf(av_data, "52WeekHigh")
    av_52lo = sf(av_data, "52WeekLow")
    yf_52hi = sf(yf_data, "52WeekHigh")
    yf_52lo = sf(yf_data, "52WeekLow")
    hi = av_52hi or yf_52hi or wk52_hi_fh
    lo = av_52lo or yf_52lo or wk52_lo_fh
    vs_high = (price / hi * 100) if hi else None
    yr_ret  = (price / lo - 1) * 100 if lo else None
    ytd_raw = sf(yf_data, "ytdReturn")
    ytd_pct = ytd_raw * 100 if ytd_raw is not None else None
    expense = db_entry.get("expense")
    num_h   = db_entry.get("num_holdings", 0)
    category= db_entry.get("category", "ETF")
    if   num_h >= 1000: div_sc = 10
    elif num_h >= 400:  div_sc = 9
    elif num_h >= 200:  div_sc = 8
    elif num_h >= 100:  div_sc = 6
    elif num_h >= 50:   div_sc = 4
    else:               div_sc = 2
    is_bond = (pe_h == 0 or
               any(x in category.lower() for x in
                   ("bond","treasury","gold","silver","commodity","covered call","income")))
    criteria = [
        {"name": "Expense Ratio",
         "score": score_range(expense, 0.05, 1.0, False),
         "note":  f"{expense:.2f}%" if expense is not None else "N/A"},
        {"name": "AUM & Liquidity",
         "score": score_range(aum_b, 10, 0.5, True),
         "note":  f"${aum_b:.1f}B" if aum_b else "N/A"},
        {"name": "Dividend Yield",
         "score": score_range(div_pct, 5, 0, True),
         "note":  f"{div_pct:.2f}%" if div_pct else "None"},
        {"name": "YTD Total Return",
         "score": (score_range(ytd_pct, 10, -10, True) if ytd_pct is not None
                   else score_range(yr_ret, 20, 0, True)),
         "note":  (f"{ytd_pct:.1f}% YTD" if ytd_pct is not None
                   else (f"~{yr_ret:.0f}% (52W)" if yr_ret is not None else "N/A"))},
        {"name": "Price Momentum",
         "score": score_range(vs_high, 90, 55, True),
         "note":  f"{vs_high:.0f}% of 52W high" if vs_high is not None else "N/A"},
        {"name": "Holdings Valuation" if not is_bond else "Income Yield",
         "score": (score_range(pe_h, 15, 40, False) if not is_bond
                   else score_range(div_pct, 8, 2, True)),
         "note":  (f"P/E {pe_h:.1f}x ({pe_source})" if not is_bond
                   else f"Income yield {div_pct:.2f}%")},
        {"name": "Diversification",
         "score": div_sc,
         "note":  f"{num_h:,} holdings"},
        {"name": "Risk / Beta",
         "score": score_range(abs(beta) if beta is not None else None, 0.7, 1.5, False),
         "note":  f"Beta {beta:.2f}" if beta is not None else "N/A"},
    ]
    total   = sum(c["score"] for c in criteria)
    pct     = round(total / (len(criteria) * 10) * 100)
    verdict = "BUY" if pct >= 70 else "HOLD" if pct >= 45 else "SELL"
    metrics = {
        "Price":         f"${price:.2f}",
        "AUM":           f"${aum_b:.1f}B"       if aum_b            else "N/A",
        "Expense Ratio": f"{expense:.2f}%"       if expense is not None else "N/A",
        "Div Yield":     f"{div_pct:.2f}%"       if div_pct           else "N/A",
        "YTD Return":    (f"{ytd_pct:.1f}%"      if ytd_pct is not None
                          else f"~{yr_ret:.0f}%" if yr_ret is not None else "N/A"),
        "vs 52W High":   f"{vs_high:.0f}%"       if vs_high is not None else "N/A",
        "Holdings P/E":  f"{pe_h:.1f}x"          if pe_h              else "N/A",
        "# Holdings":    f"{num_h:,}",
        "Beta":          f"{beta:.2f}"            if beta is not None  else "N/A",
        "Category":      category,
        "Data Quality":  dq.quality,
    }
    return criteria, metrics, verdict, pct


def score_stock(ticker, price, wk52_hi, wk52_lo, av, dq, pt_data=None, sector=""):
    pe        = sf(av, "TrailingPE")
    fwd_pe    = sf(av, "ForwardPE")
    pb        = sf(av, "PriceToBookRatio")
    ps        = sf(av, "PriceToSalesRatioTTM")
    ev_ebitda = sf(av, "EVToEBITDA")
    roe       = sf(av, "ReturnOnEquityTTM")
    roe_pct   = roe * 100 if roe is not None else None
    net_m     = sf(av, "ProfitMargin")
    net_pct   = net_m * 100 if net_m is not None else None
    gp        = sf(av, "GrossProfitTTM")
    rev       = sf(av, "RevenueTTM")
    gross_pct = (gp / rev * 100) if gp and rev and rev > 0 else None
    if gross_pct is None:
        gm_raw = sf(av, "_grossMargins")
        if gm_raw is not None: gross_pct = gm_raw * 100
    eg        = sf(av, "QuarterlyEarningsGrowthYOY")
    eg_pct    = eg * 100 if eg is not None else None
    rg        = sf(av, "QuarterlyRevenueGrowthYOY")
    rg_pct    = rg * 100 if rg is not None else None
    div_yield = sf(av, "DividendYield", 0)
    div_pct   = div_yield * 100 if div_yield else 0.0
    payout    = sf(av, "PayoutRatio", 0)
    payout_pct= payout * 100 if payout else 0.0
    op_m      = sf(av, "OperatingMarginTTM")
    op_pct    = op_m * 100 if op_m is not None else None

    sec_lower = (sector or "").lower()
    _TECH = ("technology","semiconductor","software","internet","cloud","saas",
             "artificial intelligence","communication","interactive","e-commerce",
             "electronic","information technology")
    _IND  = ("capital goods","automobile","transportation","energy","industrial",
             "manufacturing","machinery","defense","aerospace","electrical",
             "power","oil","mining","construction","steel","chemical")
    if any(x in sec_lower for x in _TECH):
        pe_good, pe_bad, gm_good, gm_bad, tier_label = 30, 80, 65, 25, "Tech"
    elif any(x in sec_lower for x in _IND):
        pe_good, pe_bad, gm_good, gm_bad, tier_label = 15, 40, 35, 5, "Industrial"
    else:
        pe_good, pe_bad, gm_good, gm_bad, tier_label = 20, 60, 55, 15, "Default"

    ps_good = max(3.0, min(12.0, 3.0 + max(0.0, (gross_pct or 0) - 20.0) / 40.0 * 9.0))
    ps_bad  = ps_good * 4

    if pe and fwd_pe and fwd_pe > 0 and fwd_pe < pe:
        blended_pe = pe * 0.4 + fwd_pe * 0.6
        pe_note    = f"P/E {pe:.1f}x → Fwd {fwd_pe:.1f}x (blended {blended_pe:.1f}x) [{tier_label}]"
    elif pe:
        blended_pe = pe
        pe_note    = f"P/E {pe:.1f}x (trailing) [{tier_label}]"
    elif fwd_pe and fwd_pe > 0:
        blended_pe = fwd_pe
        pe_note    = f"Fwd P/E {fwd_pe:.1f}x [{tier_label}]"
    else:
        blended_pe = None
        pe_note    = f"N/A [{tier_label}]"

    pt_mean    = sf(pt_data, "mean") if pt_data else None
    pt_high    = sf(pt_data, "high") if pt_data else None
    pt_low     = sf(pt_data, "low")  if pt_data else None
    upside_pct = ((pt_mean - price) / price * 100) if pt_mean and price else None
    if upside_pct is not None:
        pt_note = (f"{upside_pct:+.1f}% to ${pt_mean:.0f}"
                   + (f" (${pt_low:.0f}–${pt_high:.0f})" if pt_low and pt_high else ""))
    else:
        pt_note = "No analyst target"

    if div_pct > 0 and payout_pct == 0.0 and (net_pct is None or net_pct < 0):
        div_score = 1
        div_note  = f"Yield {div_pct:.2f}%  Payout N/A (earnings negative)"
    elif div_pct > 0:
        div_score = score_range(payout_pct, 20, 85, False)
        div_note  = f"Yield {div_pct:.2f}%  Payout {payout_pct:.0f}%"
    else:
        div_score = 7
        div_note  = "No dividend"

    # Analyst target signal
    if upside_pct is not None and upside_pct >= 15:
        pt_signal = f"UPSIDE +{upside_pct:.1f}% to consensus ${pt_mean:.0f}"
    elif upside_pct is not None and upside_pct >= 0:
        pt_signal = f"FAIR +{upside_pct:.1f}% to consensus ${pt_mean:.0f}"
    elif upside_pct is not None and upside_pct >= -10:
        pt_signal = f"CAUTION {upside_pct:.1f}% — near/above analyst target ${pt_mean:.0f}"
    elif upside_pct is not None:
        pt_signal = f"WARNING {upside_pct:.1f}% — significantly above analyst target ${pt_mean:.0f}"
    else:
        pt_signal = None

    criteria = [
        {"name": "Valuation (P/E)",  "score": score_range(blended_pe, pe_good, pe_bad, False), "note": pe_note},
        {"name": "Price-to-Book",    "score": score_range(pb, 2, 50, False),                    "note": f"P/B {pb:.2f}x" if pb else "N/A"},
        {"name": "Margin-Adj P/S",   "score": score_range(ps, ps_good, ps_bad, False),          "note": f"P/S {ps:.2f}x" if ps else "N/A"},
        {"name": "EV / EBITDA",      "score": score_range(ev_ebitda, 15, 60, False),            "note": f"EV/EBITDA {ev_ebitda:.1f}x" if ev_ebitda else "N/A"},
        {"name": "Return on Equity", "score": score_range(roe_pct, 20, 0, True),                "note": f"ROE {roe_pct:.1f}%" if roe_pct is not None else "N/A"},
        {"name": "Gross Margin",     "score": score_range(gross_pct, gm_good, gm_bad, True),    "note": f"{gross_pct:.1f}%" if gross_pct is not None else "N/A"},
        {"name": "Net Margin",       "score": score_range(net_pct, 20, 3, True),                "note": f"{net_pct:.1f}%" if net_pct is not None else "N/A"},
        {"name": "Earnings Growth",  "score": score_range(eg_pct, 20, -10, True),               "note": f"{eg_pct:+.1f}% YoY" if eg_pct is not None else "N/A"},
        {"name": "Revenue Growth",   "score": score_range(rg_pct, 20, 0, True),                 "note": f"{rg_pct:+.1f}% YoY" if rg_pct is not None else "N/A"},
        {"name": "Dividend Safety",  "score": div_score,                                         "note": div_note},
        {"name": "Analyst Target",   "score": score_range(upside_pct, 15, -15, True),           "note": pt_note},
    ]
    total   = sum(c["score"] for c in criteria)
    pct     = round(total / (len(criteria) * 10) * 100)
    verdict = "BUY" if pct >= 70 else "HOLD" if pct >= 45 else "SELL"
    metrics = {
        "Price":          f"${price:.2f}",
        "Analyst Target": pt_note,
        "P/E (TTM)":      f"{pe:.1f}x"         if pe           else "N/A",
        "Fwd P/E":        f"{fwd_pe:.1f}x"      if fwd_pe       else "N/A",
        "P/B":            f"{pb:.2f}x"          if pb           else "N/A",
        "P/S (TTM)":      f"{ps:.2f}x"          if ps           else "N/A",
        "EV/EBITDA":      f"{ev_ebitda:.1f}x"   if ev_ebitda    else "N/A",
        "Gross Margin":   f"{gross_pct:.1f}%"   if gross_pct is not None else "N/A",
        "Net Margin":     f"{net_pct:.1f}%"     if net_pct   is not None else "N/A",
        "Op Margin":      f"{op_pct:.1f}%"      if op_pct    is not None else "N/A",
        "ROE":            f"{roe_pct:.1f}%"     if roe_pct   is not None else "N/A",
        "EPS Growth":     f"{eg_pct:+.1f}%"     if eg_pct    is not None else "N/A",
        "Rev Growth":     f"{rg_pct:+.1f}%"     if rg_pct    is not None else "N/A",
        "Div Yield":      f"{div_pct:.2f}%"     if div_pct       else "N/A",
        "52W Range":      (f"${wk52_lo:.0f}–${wk52_hi:.0f}" if wk52_lo and wk52_hi else "N/A"),
        "Data Quality":   dq.quality,
        "_pt_signal":     pt_signal,
    }
    return criteria, metrics, verdict, pct


def fetch_and_score(ticker, fh_key, av_key):
    ticker = ticker.strip().upper()
    dq = DataQuality()
    q, err = fh_get("/quote", {"symbol": ticker, "token": fh_key})
    if err or not q or not q.get("c") or q.get("c") == 0:
        raise ValueError(f"No price data for '{ticker}' ({err or 'empty quote'})")
    dq.ok("Finnhub")
    price      = sf(q, "c")
    change_pct = sf(q, "dp")
    pr, err = fh_get("/stock/profile2", {"symbol": ticker, "token": fh_key})
    if err: dq.fail("FH-Profile", err)
    pr = pr or {}
    name     = pr.get("name") or ticker
    industry = pr.get("finnhubIndustry") or "—"
    exchange = pr.get("exchange") or "—"
    av, err = av_get({"function": "OVERVIEW", "symbol": ticker, "apikey": av_key})
    if err:
        dq.fail("AlphaVantage", err)
        av = {}
    else:
        dq.ok("AlphaVantage")
    if not name or name == ticker: name = av.get("Name") or ticker
    if exchange == "—": exchange = av.get("Exchange") or "—"
    wk52_hi = sf(av, "52WeekHigh")
    wk52_lo = sf(av, "52WeekLow")
    av_mc   = sf(av, "MarketCapitalization")
    mkt_cap_b = (av_mc / 1e9) if av_mc else None
    db_entry = ETF_DB.get(ticker)
    av_type  = str(av.get("AssetType", "")).upper()
    is_etf   = (db_entry is not None or "ETF" in av_type or "FUND" in av_type)
    if is_etf:
        yf_data, err = yf_get_etf(ticker)
        if err:   dq.fail("yfinance", err);     yf_data = {}
        elif yf_data.get("trailingPE"): dq.ok("yfinance")
        if db_entry is None:
            db_entry = {"expense": None, "num_holdings": 0,
                        "category": av.get("Industry") or industry or "ETF"}
            dq.fail("ETF_DB", "not in built-in database")
        criteria, metrics, verdict, pct = score_etf(
            ticker, price, wk52_hi, wk52_lo, db_entry, av, yf_data, dq)
        asset_type = "ETF"
        sector     = db_entry.get("category", industry)
    else:
        if not av:
            yf_stock, yf_err = yf_get_stock(ticker)
            if yf_err: dq.fail("yfinance-stock", yf_err)
            else:
                av = yf_stock
                dq.ok("yfinance-stock")
                if not wk52_hi: wk52_hi = sf(av, "52WeekHigh")
                if not wk52_lo: wk52_lo = sf(av, "52WeekLow")
        pt_data, pt_err = fh_price_target(ticker, fh_key)
        if pt_err: dq.fail("PriceTarget", pt_err)
        else:      dq.ok("PriceTarget")
        criteria, metrics, verdict, pct = score_stock(
            ticker, price, wk52_hi, wk52_lo, av, dq, pt_data, sector=industry)
        asset_type = "Stock"
        sector     = industry
    return {
        "ticker": ticker, "name": name, "type": asset_type,
        "sector": sector, "exchange": exchange,
        "price": price, "change_pct": change_pct,
        "year_high": wk52_hi, "year_low": wk52_lo,
        "mkt_cap_b": mkt_cap_b,
        "metrics": metrics, "criteria": criteria,
        "total": sum(c["score"] for c in criteria),
        "max_total": len(criteria) * 10,
        "pct": pct, "verdict": verdict, "dq": dq,
    }


# ── Cached wrapper — 30 min TTL avoids repeated API calls on page refresh ────
@st.cache_data(ttl=1800, show_spinner=False)
def cached_fetch(ticker, fh_key, av_key):
    return fetch_and_score(ticker, fh_key, av_key)


# ════════════════════════════════════════════════════════════════════════════
#  UI COMPONENTS
# ════════════════════════════════════════════════════════════════════════════

def score_color_class(pct):
    if pct >= 70: return "score-buy"
    if pct >= 45: return "score-hold"
    return "score-sell"

def verdict_pill(v):
    cls = {"BUY": "pill-buy", "HOLD": "pill-hold", "SELL": "pill-sell"}.get(v, "pill-hold")
    return f'<span class="verdict-pill {cls}">{v}</span>'

def bar_color(score):
    if score >= 7: return "bar-high"
    if score >= 4: return "bar-mid"
    return "bar-low"

def dq_badge(q):
    cls = {"HIGH": "dq-high", "MEDIUM": "dq-medium", "LOW": "dq-low"}.get(q, "dq-low")
    return f'<span class="{cls}">{q}</span>'

def render_criteria(criteria):
    rows = ""
    for c in criteria:
        sc   = c["score"]
        pct  = sc / 10 * 100
        bc   = bar_color(sc)
        note = c["note"][:40] if c["note"] else ""
        rows += f"""
        <div class="crit-row">
            <span class="crit-name">{c['name']}</span>
            <div class="crit-bar-bg">
                <div class="crit-bar-fill {bc}" style="width:{pct}%"></div>
            </div>
            <span class="crit-score">{sc}/10</span>
            <span class="crit-note">{note}</span>
        </div>"""
    st.markdown(rows, unsafe_allow_html=True)

def render_metrics(metrics):
    skip = {"Data Quality", "_pt_signal"}
    chips = ""
    for k, v in metrics.items():
        if k in skip or v == "N/A": continue
        chips += f'<span class="metric-chip"><b>{k}</b> {v}</span>'
    st.markdown(chips, unsafe_allow_html=True)

def render_pt_signal(metrics):
    sig = metrics.get("_pt_signal")
    if not sig: return
    if sig.startswith("WARNING"):
        st.markdown(f'<div class="danger-box">⚠️ {sig}</div>', unsafe_allow_html=True)
    elif sig.startswith("CAUTION"):
        st.markdown(f'<div class="warn-box">⚡ {sig}</div>', unsafe_allow_html=True)
    elif sig.startswith("UPSIDE"):
        st.markdown(f'<div class="upside-box">↑ {sig}</div>', unsafe_allow_html=True)

def render_card(r):
    v    = r["verdict"]
    sc   = r["pct"]
    cc   = score_color_class(sc)
    chg  = r["change_pct"]
    chg_str = f"{chg:+.2f}%" if chg is not None else "—"
    chg_color = "#3fb950" if (chg or 0) >= 0 else "#ff7b72"
    dq = r.get("dq")

    st.markdown(f"""
    <div class="score-card">
        <div style="display:flex; justify-content:space-between; align-items:flex-start;">
            <div>
                <div class="ticker-label">{r['ticker']}</div>
                <div class="name-label">{r['name']} · {r['type']} · {r['exchange']}</div>
                <div style="margin-top:6px;">{verdict_pill(v)}</div>
            </div>
            <div style="text-align:right;">
                <div class="score-big {cc}">{sc}</div>
                <div style="font-size:0.72rem; color:#8b949e;">/ 100</div>
                <div style="font-size:0.78rem; color:{chg_color}; margin-top:2px;">{chg_str} today</div>
            </div>
        </div>
        <div style="margin-top:10px; font-size:0.78rem; color:#8b949e;">
            <b style="color:#c9d1d9;">${r['price']:.2f}</b>
            &nbsp;·&nbsp; Data {dq_badge(dq.quality if dq else '?')}
            {('&nbsp;·&nbsp; <span style="color:#8b949e;">' + r["sector"][:30] + '</span>') if r.get("sector") and r["sector"] != '—' else ''}
        </div>
    </div>
    """, unsafe_allow_html=True)

    with st.expander("Details — criteria & metrics"):
        render_pt_signal(r["metrics"])
        render_criteria(r["criteria"])
        st.markdown("---")
        render_metrics(r["metrics"])
        if dq and dq.summary:
            st.caption(f"⚠ {dq.summary}")


# ════════════════════════════════════════════════════════════════════════════
#  MAIN APP
# ════════════════════════════════════════════════════════════════════════════

st.markdown("## 📈 Portfolio Analyzer")
st.markdown('<p style="color:#8b949e; margin-top:-12px; font-size:0.9rem;">Live fundamental scoring · Analyst targets · Sector-aware valuation</p>', unsafe_allow_html=True)

# ── Ticker input ─────────────────────────────────────────────────────────────
with st.expander("⚙️ Portfolio settings", expanded="tickers" not in st.session_state):
    raw = st.text_area(
        "Tickers (one per line or comma-separated)",
        value="\n".join(st.session_state.get("tickers", DEFAULT_TICKERS)),
        height=180,
        help="Add or remove tickers. ETFs and stocks are detected automatically.",
    )
    col1, col2 = st.columns([1, 3])
    with col1:
        run_btn = st.button("▶ Analyze", type="primary", use_container_width=True)
    with col2:
        st.caption("Results cached 30 min — click Analyze to force a refresh")

if run_btn:
    tickers = [t.strip().upper() for t in raw.replace(",", "\n").splitlines() if t.strip()]
    st.session_state["tickers"] = tickers
    # Clear cache for fresh data
    cached_fetch.clear()

tickers = st.session_state.get("tickers", DEFAULT_TICKERS)

if not tickers:
    st.info("Enter at least one ticker above and click Analyze.")
    st.stop()

# ── Analysis ──────────────────────────────────────────────────────────────────
results  = []
errors   = []
progress = st.progress(0, text="Starting analysis…")

for i, ticker in enumerate(tickers):
    progress.progress((i) / len(tickers), text=f"Fetching {ticker}…")
    try:
        r = cached_fetch(ticker, FINNHUB_KEY, AV_KEY)
        results.append(r)
    except Exception as e:
        errors.append((ticker, str(e)))

progress.progress(1.0, text="Done.")
progress.empty()

# ── Summary bar ───────────────────────────────────────────────────────────────
if results:
    buys  = sum(1 for r in results if r["verdict"] == "BUY")
    holds = sum(1 for r in results if r["verdict"] == "HOLD")
    sells = sum(1 for r in results if r["verdict"] == "SELL")
    avg   = round(sum(r["pct"] for r in results) / len(results))
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Avg Score",  f"{avg}/100")
    c2.metric("BUY",        buys,  delta=None)
    c3.metric("HOLD",       holds, delta=None)
    c4.metric("SELL",       sells, delta=None)
    st.markdown("---")

# ── Sort: BUY first, then by score descending ─────────────────────────────────
results.sort(key=lambda r: ({"BUY": 0, "HOLD": 1, "SELL": 2}[r["verdict"]], -r["pct"]))

# ── Render cards ─────────────────────────────────────────────────────────────
for r in results:
    render_card(r)

for ticker, err in errors:
    st.error(f"**{ticker}**: {err}")

# ── Footer ────────────────────────────────────────────────────────────────────
st.markdown("---")
st.caption("Data: Finnhub (price) · Alpha Vantage (fundamentals) · yfinance (targets, ETF metrics) · Scores cached 30 min")
