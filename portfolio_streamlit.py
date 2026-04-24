"""
Portfolio Analyzer — Streamlit Web Edition
==========================================
Run locally:  streamlit run portfolio_streamlit.py
Deploy:       push to GitHub → connect at share.streamlit.io
Mobile:       open the Streamlit Cloud URL on any device

Data architecture (v3 — yfinance-first):
  PRIMARY  → yfinance  : price, fundamentals, targets, ETF data (free, no key)
  SECONDARY → Finnhub  : real-time price quote only (free, fast)
  REMOVED  → Alpha Vantage : was 13 s/ticker × N tickers = app timeout
  PARALLEL → ThreadPoolExecutor : all tickers fetched simultaneously
"""

import time, warnings, requests, html as _html, threading
from concurrent.futures import ThreadPoolExecutor, as_completed
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
.crit-score { color: #c9d1d9; font-weight: 600; width: 36px; text-align: right; font-family: monospace; white-space: nowrap; }
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
.dq-warn-inline { font-size: 0.72rem; color: #d29922; margin-top: 4px; }
</style>
""", unsafe_allow_html=True)

# ── API Keys ──────────────────────────────────────────────────────────────────
try:
    FINNHUB_KEY = st.secrets["FINNHUB_KEY"]
except Exception:
    FINNHUB_KEY = "d787fi1r01qsamsj83h0d787fi1r01qsamsj83hg"
try:
    AV_KEY = st.secrets["AV_KEY"]   # kept for forward compatibility, not actively used
except Exception:
    AV_KEY = "DRFXF3KZF59XEVA4"

# ── API / timing config ──────────────────────────────────────────────────────
FH_BASE        = "https://finnhub.io/api/v1"
TIMEOUT        = 12
FH_MIN_GAP     = 0.25   # minimum seconds between Finnhub calls (thread-safe enforced)
YF_DELAY       = 0.15   # polite pause per yfinance call
RETRY_ATTEMPTS = 2
RETRY_BACKOFF  = 1.5

# Thread-safe Finnhub rate limiter — serialises calls across parallel workers
_fh_lock   = threading.Lock()
_fh_last_t = [0.0]

# ── Default portfolio ────────────────────────────────────────────────────────
DEFAULT_TICKERS = ["NVDA", "AMZN", "GOOGL", "GEV", "CAT", "F", "PCAR", "JEPQ", "VUG"]

# ── ETF database ─────────────────────────────────────────────────────────────
ETF_DB = {
    "SPY":  {"expense":0.0945,"num_holdings":503,  "category":"S&P 500",               "beta_fallback":1.00},
    "IVV":  {"expense":0.03,  "num_holdings":503,  "category":"S&P 500",               "beta_fallback":1.00},
    "VOO":  {"expense":0.03,  "num_holdings":503,  "category":"S&P 500",               "multi_class_aum":True, "beta_fallback":1.00},
    "VTI":  {"expense":0.03,  "num_holdings":3700, "category":"Total US Market",       "multi_class_aum":True, "beta_fallback":1.00},
    "VUG":  {"expense":0.04,  "num_holdings":150,  "category":"Large Cap Growth",      "multi_class_aum":True, "beta_fallback":1.05},
    "QQQ":  {"expense":0.20,  "num_holdings":101,  "category":"Nasdaq-100",            "beta_fallback":1.15},
    "QQQM": {"expense":0.15,  "num_holdings":101,  "category":"Nasdaq-100",            "beta_fallback":1.15},
    "IWF":  {"expense":0.19,  "num_holdings":330,  "category":"Large Cap Growth",      "beta_fallback":1.10},
    "SPYG": {"expense":0.04,  "num_holdings":240,  "category":"Large Cap Growth",      "beta_fallback":1.05},
    "MGK":  {"expense":0.07,  "num_holdings":69,   "category":"Mega Cap Growth",       "beta_fallback":1.10},
    "VTV":  {"expense":0.04,  "num_holdings":340,  "category":"Large Cap Value",       "multi_class_aum":True, "beta_fallback":0.92},
    "SCHD": {"expense":0.06,  "num_holdings":103,  "category":"Dividend Growth",       "beta_fallback":0.85},
    "VIG":  {"expense":0.06,  "num_holdings":338,  "category":"Dividend Growth",       "multi_class_aum":True, "beta_fallback":0.85},
    "DGRO": {"expense":0.08,  "num_holdings":420,  "category":"Dividend Growth",       "beta_fallback":0.85},
    "DVY":  {"expense":0.38,  "num_holdings":100,  "category":"High Dividend",         "beta_fallback":0.80},
    "VYM":  {"expense":0.06,  "num_holdings":555,  "category":"High Dividend Yield",   "multi_class_aum":True, "beta_fallback":0.82},
    "JEPI": {"expense":0.35,  "num_holdings":101,  "category":"Covered Call / S&P 500 Income", "beta_fallback":0.55},
    "JEPQ": {"expense":0.35,  "num_holdings":93,   "category":"Covered Call / Nasdaq Income",  "beta_fallback":0.62},
    "XYLD": {"expense":0.60,  "num_holdings":503,  "category":"Covered Call / S&P 500",        "beta_fallback":0.60},
    "QYLD": {"expense":0.60,  "num_holdings":101,  "category":"Covered Call / Nasdaq",         "beta_fallback":0.65},
    "XLK":  {"expense":0.10,  "num_holdings":67,   "category":"Technology Sector",     "beta_fallback":1.20},
    "VGT":  {"expense":0.10,  "num_holdings":316,  "category":"Technology Sector",     "multi_class_aum":True, "beta_fallback":1.20},
    "IYW":  {"expense":0.38,  "num_holdings":144,  "category":"U.S. Technology",       "beta_fallback":1.20},
    "SMH":  {"expense":0.35,  "num_holdings":26,   "category":"Semiconductors",        "beta_fallback":1.35},
    "SOXX": {"expense":0.35,  "num_holdings":30,   "category":"Semiconductors",        "beta_fallback":1.35},
    "XLF":  {"expense":0.10,  "num_holdings":74,   "category":"Financials",            "beta_fallback":1.10},
    "XLV":  {"expense":0.10,  "num_holdings":63,   "category":"Healthcare",            "beta_fallback":0.72},
    "XLE":  {"expense":0.10,  "num_holdings":23,   "category":"Energy",                "beta_fallback":1.05},
    "XLI":  {"expense":0.10,  "num_holdings":79,   "category":"Industrials",           "beta_fallback":1.00},
    "VEA":  {"expense":0.05,  "num_holdings":3900, "category":"Developed Markets ex-US","multi_class_aum":True,"beta_fallback":0.90},
    "VWO":  {"expense":0.08,  "num_holdings":5800, "category":"Emerging Markets",      "multi_class_aum":True, "beta_fallback":0.85},
    "BND":  {"expense":0.03,  "num_holdings":10000,"category":"Total US Bond Market",  "beta_fallback":0.05},
    "AGG":  {"expense":0.03,  "num_holdings":10000,"category":"Total US Bond Market",  "beta_fallback":0.05},
    "TLT":  {"expense":0.15,  "num_holdings":30,   "category":"Long-Term Treasury",    "beta_fallback":0.20},
    "GLD":  {"expense":0.40,  "num_holdings":1,    "category":"Gold",                  "beta_fallback":0.08},
    "IAU":  {"expense":0.25,  "num_holdings":1,    "category":"Gold",                  "beta_fallback":0.08},
    "VNQ":  {"expense":0.13,  "num_holdings":160,  "category":"US REITs",              "multi_class_aum":True, "beta_fallback":0.85},
    "ARKK": {"expense":0.75,  "num_holdings":30,   "category":"Disruptive Innovation", "beta_fallback":1.55},
}


# ════════════════════════════════════════════════════════════════════════════
#  CORE FUNCTIONS
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
    """Thread-safe Finnhub GET — serialises calls with minimum gap between them."""
    with _fh_lock:
        gap = FH_MIN_GAP - (time.time() - _fh_last_t[0])
        if gap > 0:
            time.sleep(gap)
        _fh_last_t[0] = time.time()
    for attempt in range(RETRY_ATTEMPTS):
        try:
            r = requests.get(f"{FH_BASE}{path}", params=params, timeout=TIMEOUT)
            if r.status_code == 200:
                return r.json(), None
            if r.status_code in (429, 503):
                time.sleep(RETRY_BACKOFF * (attempt + 1))
                continue
            return None, f"HTTP {r.status_code}"
        except requests.exceptions.Timeout:
            return None, "Timeout"
        except Exception as e:
            return None, str(e)[:80]
    return None, "Max retries exceeded"


def yf_get_all(ticker):
    """
    Primary data source — ONE yfinance call returns everything:
    price, fundamentals, analyst targets, ETF metrics, sector info.
    Replaces av_get + yf_get_etf + yf_get_stock + fh_price_target.
    """
    if not YF_AVAILABLE:
        return {}, "yfinance not installed"
    time.sleep(YF_DELAY)
    try:
        info = yf.Ticker(ticker).info
        if not info:
            return {}, f"yfinance: empty response for {ticker}"

        price = (info.get("regularMarketPrice") or
                 info.get("currentPrice") or
                 info.get("previousClose"))
        if not price or price == 0:
            return {}, f"yfinance: no price for {ticker}"

        # Exchange code → readable name
        exc_raw = info.get("exchange", "")
        exc_map = {"NMS": "NASDAQ", "NYQ": "NYSE", "NGM": "NASDAQ",
                   "NasdaqGS": "NASDAQ", "NasdaqGM": "NASDAQ",
                   "PCX": "NYSE Arca", "NYSEArca": "NYSE Arca",
                   "BTS": "BATS", "PNK": "OTC"}
        exchange = (exc_map.get(exc_raw) or
                    info.get("fullExchangeName") or
                    exc_raw or "—")

        # yfinance returns dividendYield and ytdReturn as percentages (e.g. 11.1 = 11.1%)
        raw_div = info.get("dividendYield")
        raw_ytd = info.get("ytdReturn")
        # Prefer TTM earnings growth; fall back to quarterly
        eg = info.get("earningsGrowth") or info.get("earningsQuarterlyGrowth")

        return {
            # ── Price ────────────────────────────────────────────────────────
            "price":        price,
            "change_pct":   info.get("regularMarketChangePercent"),
            "prev_close":   (info.get("regularMarketPreviousClose") or
                             info.get("previousClose")),
            # ── Identity ─────────────────────────────────────────────────────
            "name":         (info.get("longName") or
                             info.get("shortName") or ticker),
            "exchange":     exchange,
            "sector":       info.get("sector") or "—",
            "industry":     info.get("industry") or "—",
            "asset_type":   info.get("quoteType", "EQUITY"),
            "market_cap":   info.get("marketCap"),
            # ── Valuation ────────────────────────────────────────────────────
            "TrailingPE":           info.get("trailingPE"),
            "ForwardPE":            info.get("forwardPE"),
            "PriceToBookRatio":     info.get("priceToBook"),
            "PriceToSalesRatioTTM": info.get("priceToSalesTrailing12Months"),
            "EVToEBITDA":           info.get("enterpriseToEbitda"),
            # ── Profitability ─────────────────────────────────────────────────
            "ReturnOnEquityTTM":    info.get("returnOnEquity"),
            "ProfitMargin":         info.get("profitMargins"),
            "OperatingMarginTTM":   info.get("operatingMargins"),
            "_grossMargins":        info.get("grossMargins"),      # decimal (0.55 = 55%)
            "GrossProfitTTM":       info.get("grossProfits"),
            "RevenueTTM":           info.get("totalRevenue"),
            # ── Growth ────────────────────────────────────────────────────────
            "QuarterlyEarningsGrowthYOY": eg,
            "QuarterlyRevenueGrowthYOY":  info.get("revenueGrowth"),
            # ── Dividends ─────────────────────────────────────────────────────
            "DividendYield": raw_div / 100 if raw_div is not None else None,
            "PayoutRatio":   info.get("payoutRatio"),
            # ── 52-Week range ─────────────────────────────────────────────────
            "52WeekHigh": info.get("fiftyTwoWeekHigh"),
            "52WeekLow":  info.get("fiftyTwoWeekLow"),
            # ── Analyst targets ───────────────────────────────────────────────
            "targetMeanPrice": info.get("targetMeanPrice"),
            "targetHighPrice": info.get("targetHighPrice"),
            "targetLowPrice":  info.get("targetLowPrice"),
            # ── ETF-specific ──────────────────────────────────────────────────
            "totalAssets": info.get("totalAssets"),
            "beta":        info.get("beta"),
            "trailingPE":  info.get("trailingPE"),   # for ETF Holdings P/E display
            "ytdReturn":   raw_ytd / 100 if raw_ytd is not None else None,
        }, None
    except Exception as e:
        return {}, str(e)[:80]


class DataQuality:
    def __init__(self):
        self.sources_ok   = []
        self.sources_fail = []
        self.notes        = []
    def ok(self, src):
        self.sources_ok.append(src)
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


def _etf_div_label(category, div_pct):
    cat = category.lower()
    if any(x in cat for x in ("s&p","nasdaq","total market","total us","broad")):
        label = "index ETF"
    elif any(x in cat for x in ("technology","semiconductor","financials","healthcare",
                                 "energy","materials","utilities","industrials","sector")):
        label = "sector ETF"
    elif any(x in cat for x in ("developed","emerging","international","global","world",
                                 "ex-us","ex us")):
        label = "intl ETF"
    elif any(x in cat for x in ("growth","value","blend","large cap","mid cap","small cap",
                                 "mega cap")):
        label = "equity ETF"
    elif any(x in cat for x in ("innovation","thematic","disruptive")):
        label = "thematic ETF"
    else:
        label = "non-income ETF"
    return f"{div_pct:.2f}% ({label} — not scored)"


def score_etf(ticker, price, wk52_hi_fh, wk52_lo_fh, db_entry, av_data, yf_data, dq):
    pe_h = (sf(yf_data, "trailingPE") or sf(av_data, "TrailingPE"))
    pe_source = "live yfinance" if sf(yf_data, "trailingPE") else (
                "live AV" if sf(av_data, "TrailingPE") else "N/A")
    yf_aum = sf(yf_data, "totalAssets")
    aum_b  = (yf_aum / 1e9) if yf_aum else db_entry.get("aum_b_fallback")
    _multi_class = db_entry.get("multi_class_aum", False) if db_entry else False
    yf_div = sf(yf_data, "dividendYield")
    av_div = sf(av_data, "DividendYield")
    div_raw = yf_div or av_div or 0
    div_pct = div_raw * 100
    av_beta = sf(av_data, "Beta")
    yf_beta = sf(yf_data, "beta")
    beta    = av_beta if av_beta is not None else yf_beta
    if beta is None and db_entry:
        beta = db_entry.get("beta_fallback")
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
         "note":  ((f"${aum_b:.1f}B (ETF share class)" if _multi_class else f"${aum_b:.1f}B")
                   if aum_b else "N/A")},
        {"name": "Dividend Yield",
         "score": (5 if is_bond else
                   score_range(div_pct, 5, 0, True)
                   if any(x in category.lower() for x in ("dividend","reit","high yield"))
                   else 5),
         "note":  ("See Income Yield" if is_bond else
                   (f"{div_pct:.2f}%" if div_pct else "None")
                   if any(x in category.lower() for x in ("dividend","reit","high yield"))
                   else _etf_div_label(category, div_pct))},
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
        "AUM":           ((f"${aum_b:.1f}B (ETF share class)" if _multi_class else f"${aum_b:.1f}B")
                         if aum_b else "N/A"),
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

    _TIER_OVERRIDES = {
        # E-commerce / marketplace
        "AMZN":"Tech","SHOP":"Tech","EBAY":"Tech","ETSY":"Tech","MELI":"Tech",
        "JD":"Tech","PDD":"Tech","SE":"Tech",
        # Streaming / gaming
        "NFLX":"Tech","SPOT":"Tech","RBLX":"Tech","EA":"Tech","TTWO":"Tech",
        "ATVI":"Tech","MTCH":"Tech",
        # Mobility / gig economy
        "UBER":"Tech","LYFT":"Tech","DASH":"Tech","ABNB":"Tech",
        # Online travel
        "BKNG":"Tech","EXPE":"Tech","TRIP":"Tech",
        # Fintech / payments
        "PYPL":"Tech","SQ":"Tech","COIN":"Tech","SOFI":"Tech","AFRM":"Tech",
        "UPST":"Tech","HOOD":"Tech","NU":"Tech","BILL":"Tech","FOUR":"Tech",
        # Payment networks
        "V":"Tech","MA":"Tech","FISV":"Tech","FIS":"Tech","GPN":"Tech",
        "ADP":"Tech","PAYX":"Tech",
        # Healthcare SaaS
        "VEEV":"Tech","TDOC":"Tech","HIMS":"Tech","DOCS":"Tech",
        # EVs with software premium
        "TSLA":"Tech","RIVN":"Tech","LCID":"Tech",
        # Communication services → Tech scoring
        "GOOGL":"Tech","GOOG":"Tech","META":"Tech","SNAP":"Tech","PINS":"Tech",
        "ZM":"Tech","TWLO":"Tech","DDOG":"Tech",
        # Media/Entertainment → Default (NOT pure tech, lower margins)
        "DIS":"Default","CMCSA":"Default","CHTR":"Default",
        "T":"Default","VZ":"Default","WBD":"Default","FOX":"Default","PARA":"Default",
        # Telecom → Default
        "TMUS":"Default",
        # Heavy industrials
        "PCAR":"Industrial","AGCO":"Industrial","OSK":"Industrial",
        "CMI":"Industrial","GNRC":"Industrial",
        # Aerospace / defense
        "GD":"Industrial","NOC":"Industrial","HII":"Industrial",
        "TDG":"Industrial","HEI":"Industrial","LMT":"Industrial","RTX":"Industrial",
        "BA":"Industrial","HWM":"Industrial",
        # Energy equipment
        "HAL":"Industrial","BKR":"Industrial","SLB":"Industrial",
        # Renewables manufacturing
        "FSLR":"Industrial","RUN":"Industrial",
        # Power / electrical
        "GEV":"Industrial","ETN":"Industrial","ACHR":"Industrial",
        "EMR":"Industrial","ROK":"Industrial","AME":"Industrial",
        # Automakers (yfinance sector = "Consumer Cyclical" — override to Industrial)
        "F":"Industrial","GM":"Industrial","STLA":"Industrial",
        "TM":"Industrial","HMC":"Industrial","NSANY":"Industrial",
        # Solar semiconductors scored as Tech
        "ENPH":"Tech","SEDG":"Tech",
    }
    sec_lower = (sector or "").lower()
    _override = _TIER_OVERRIDES.get(ticker.upper() if ticker else "")
    _TECH = ("technology","semiconductor","software","internet","cloud","saas",
             "artificial intelligence","communication services","interactive",
             "e-commerce","electronic","information technology")
    _IND  = ("capital goods","industrials","industrial","automobile","auto manufacturer",
             "transportation","energy","manufacturing","machinery","defense",
             "aerospace","electrical","power","oil","mining","construction",
             "steel","chemical","farm","heavy equipment","truck")

    if _override == "Tech" or (not _override and any(x in sec_lower for x in _TECH)):
        pe_good, pe_bad, gm_good, gm_bad, tier_label = 18, 60, 65, 25, "Tech" + (" (override)" if _override else "")
    elif _override == "Industrial" or (not _override and any(x in sec_lower for x in _IND)):
        pe_good, pe_bad, gm_good, gm_bad, tier_label = 10, 28, 35, 5,  "Industrial" + (" (override)" if _override else "")
    elif _override == "Default":
        pe_good, pe_bad, gm_good, gm_bad, tier_label = 13, 38, 55, 15, "Default (override)"
    else:
        pe_good, pe_bad, gm_good, gm_bad, tier_label = 13, 38, 55, 15, "Default"

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

    if div_pct >= 0.5 and payout_pct == 0.0 and (net_pct is None or net_pct < 0):
        div_score = 1
        div_note  = f"Yield {div_pct:.2f}%  Payout N/A (earnings negative)"
    elif div_pct >= 0.5:
        div_score = score_range(payout_pct, 20, 85, False)
        div_note  = f"Yield {div_pct:.2f}%  Payout {payout_pct:.0f}%"
    else:
        div_score = 5
        div_note  = "No dividend" if div_pct == 0 else f"Token yield {div_pct:.2f}% (not scored)"

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

    if blended_pe is None and net_pct is not None and net_pct < 0:
        _pe_score = 2
        pe_note   = "N/A — negative earnings [losses — penalised]"
    else:
        _pe_score = score_range(blended_pe, pe_good, pe_bad, False)

    eg_score = score_range(eg_pct, 20, -10, True)
    if eg_pct is not None and eg_pct > 200:
        eg_score = min(eg_score, 6)

    if wk52_hi and wk52_lo and wk52_hi > wk52_lo and price:
        pos_pct   = (price - wk52_lo) / (wk52_hi - wk52_lo) * 100
        pos_score = score_range(pos_pct, 30, 75, False)
        pos_note  = f"{pos_pct:.0f}% of range (${wk52_lo:.0f}–${wk52_hi:.0f})"
    else:
        pos_pct, pos_score, pos_note = None, 5, "N/A"

    criteria = [
        {"name": "Valuation (P/E)",  "score": _pe_score,                                       "note": pe_note},
        {"name": "Price-to-Book",    "score": score_range(pb, 2, 50, False),                    "note": f"P/B {pb:.2f}x" if pb else "N/A"},
        {"name": "Margin-Adj P/S",   "score": score_range(ps, ps_good, ps_bad, False),          "note": f"P/S {ps:.2f}x" if ps else "N/A"},
        {"name": "EV / EBITDA",      "score": score_range(ev_ebitda, 8, 40, False),             "note": f"EV/EBITDA {ev_ebitda:.1f}x" if ev_ebitda else "N/A"},
        {"name": "Return on Equity", "score": score_range(roe_pct, 20, 0, True),                "note": f"ROE {roe_pct:.1f}%" if roe_pct is not None else "N/A"},
        {"name": "Gross Margin",     "score": score_range(gross_pct, gm_good, gm_bad, True),    "note": f"{gross_pct:.1f}%" if gross_pct is not None else "N/A"},
        {"name": "Net Margin",       "score": score_range(net_pct, 20, 3, True),                "note": f"{net_pct:.1f}%" if net_pct is not None else "N/A"},
        {"name": "Earnings Growth",  "score": eg_score,
         "note": ((f"{eg_pct:+.1f}% TTM" + (" *one-time? (capped 6/10)" if eg_pct > 200 else ""))
                  if eg_pct is not None else "N/A")},
        {"name": "Revenue Growth",   "score": score_range(rg_pct, 20, 0, True),                 "note": f"{rg_pct:+.1f}% TTM" if rg_pct is not None else "N/A"},
        {"name": "Dividend Safety",  "score": div_score,                                         "note": div_note},
        {"name": "52W Position",     "score": pos_score,                                         "note": pos_note},
        {"name": "Analyst Target",   "score": score_range(upside_pct, 15, -15, True), "weight": 2, "note": pt_note},
    ]
    total   = sum(c["score"] * c.get("weight", 1) for c in criteria)
    max_pts = sum(10 * c.get("weight", 1) for c in criteria)
    pct     = round(total / max_pts * 100)
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

    # ── PRIMARY: yfinance — one call gets everything ──────────────────────────
    yf_info, yf_err = yf_get_all(ticker)

    if not yf_info.get("price"):
        # yfinance failed — try Finnhub for at least a price
        q, fh_err = fh_get("/quote", {"symbol": ticker, "token": fh_key})
        if fh_err or not q or not q.get("c") or q.get("c") == 0:
            raise ValueError(
                f"No price for '{ticker}' "
                f"(yfinance: {yf_err or 'no price'} | Finnhub: {fh_err or 'empty'})")
        dq.fail("yfinance", yf_err or "no price")
        dq.ok("Finnhub")
        price      = sf(q, "c")
        change_pct = sf(q, "dp")
        prev_close = sf(q, "pc")
    else:
        dq.ok("yfinance")
        price      = yf_info["price"]
        change_pct = yf_info.get("change_pct")
        prev_close = yf_info.get("prev_close")

        # SECONDARY: Finnhub quote for fresher real-time price
        q, _ = fh_get("/quote", {"symbol": ticker, "token": fh_key})
        if q and sf(q, "c") and sf(q, "c") != 0:
            dq.ok("Finnhub")
            fh_price = sf(q, "c")
            if fh_price:
                price      = fh_price
                change_pct = sf(q, "dp")
                if sf(q, "pc"): prev_close = sf(q, "pc")

    # Price sanity check
    price_warn = None
    if price and prev_close and prev_close > 0:
        dev = abs((price - prev_close) / prev_close * 100)
        if dev > 25:
            price_warn = (f"Price ${price:.2f} deviates {dev:.0f}% from prev close "
                          f"${prev_close:.2f} — possible stale data")

    # Identity from yfinance
    name      = yf_info.get("name") or ticker
    yf_sector = yf_info.get("sector") or "—"
    yf_ind    = yf_info.get("industry") or "—"
    exchange  = yf_info.get("exchange") or "—"
    mkt_cap_b = (yf_info["market_cap"] / 1e9) if yf_info.get("market_cap") else None
    wk52_hi   = sf(yf_info, "52WeekHigh")
    wk52_lo   = sf(yf_info, "52WeekLow")

    # ETF detection
    db_entry   = ETF_DB.get(ticker)
    quote_type = (yf_info.get("asset_type") or "").upper()
    is_etf     = (db_entry is not None or
                  "ETF" in quote_type or "FUND" in quote_type)

    if is_etf:
        yf_etf = {
            "trailingPE":    yf_info.get("trailingPE"),
            "totalAssets":   yf_info.get("totalAssets"),
            "dividendYield": yf_info.get("DividendYield"),
            "beta":          yf_info.get("beta"),
            "52WeekHigh":    wk52_hi,
            "52WeekLow":     wk52_lo,
            "ytdReturn":     yf_info.get("ytdReturn"),
        }
        if db_entry is None:
            db_entry = {"expense": None, "num_holdings": 0,
                        "category": yf_sector or "ETF"}
            dq.fail("ETF_DB", "not in built-in database")
        criteria, metrics, verdict, pct = score_etf(
            ticker, price, wk52_hi, wk52_lo, db_entry, {}, yf_etf, dq)
        asset_type = "ETF"
        sector_out = db_entry.get("category", yf_sector)
    else:
        dq.ok("Fundamentals")
        # Map yf_info fields to AV-compatible names expected by score_stock()
        av_compat = {k: yf_info.get(k) for k in (
            "TrailingPE", "ForwardPE", "PriceToBookRatio", "PriceToSalesRatioTTM",
            "EVToEBITDA", "ReturnOnEquityTTM", "ProfitMargin", "OperatingMarginTTM",
            "_grossMargins", "GrossProfitTTM", "RevenueTTM",
            "QuarterlyEarningsGrowthYOY", "QuarterlyRevenueGrowthYOY",
            "DividendYield", "PayoutRatio", "52WeekHigh", "52WeekLow")}

        # Analyst target embedded in yfinance info
        pt_mean = yf_info.get("targetMeanPrice")
        if pt_mean and float(pt_mean) > 0:
            pt_data = {"mean": float(pt_mean),
                       "high": yf_info.get("targetHighPrice"),
                       "low":  yf_info.get("targetLowPrice")}
            dq.ok("PriceTarget")
        else:
            pt_data = None
            dq.fail("PriceTarget", "no analyst target from yfinance")

        # Pass combined sector+industry string for better tier keyword matching
        sector_str = f"{yf_sector} {yf_ind}"
        criteria, metrics, verdict, pct = score_stock(
            ticker, price, wk52_hi, wk52_lo, av_compat, dq, pt_data,
            sector=sector_str)
        asset_type = "Stock"
        sector_out = yf_ind if yf_ind != "—" else yf_sector

    return {
        "ticker": ticker, "name": name, "type": asset_type,
        "sector": sector_out, "exchange": exchange,
        "price": price, "change_pct": change_pct, "price_warning": price_warn,
        "year_high": wk52_hi, "year_low": wk52_lo, "mkt_cap_b": mkt_cap_b,
        "metrics": metrics, "criteria": criteria,
        "total":     sum(c["score"] * c.get("weight", 1) for c in criteria),
        "max_total": sum(10 * c.get("weight", 1) for c in criteria),
        "pct": pct, "verdict": verdict, "dq": dq,
    }


# ── Simple thread-safe cache (works reliably across parallel worker threads) ──
_cache: dict = {}
_cache_lock  = threading.Lock()
_CACHE_TTL   = 1800  # 30 minutes

def cached_fetch(ticker, fh_key, av_key):
    key = (ticker, fh_key, av_key)
    with _cache_lock:
        if key in _cache:
            data, ts = _cache[key]
            if time.time() - ts < _CACHE_TTL:
                return data
    data = fetch_and_score(ticker, fh_key, av_key)   # may raise
    with _cache_lock:
        _cache[key] = (data, time.time())
    return data

def clear_cache():
    with _cache_lock:
        _cache.clear()


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
        sc     = c["score"]
        weight = c.get("weight", 1)
        pct    = sc / 10 * 100
        bc     = bar_color(sc)
        note   = _html.escape(str(c["note"])[:40]) if c["note"] else ""
        w_badge = (' <span style="font-size:0.65rem;color:#58a6ff;font-weight:700;">×2</span>'
                   if weight == 2 else "")
        rows += (f'<div class="crit-row">'
                 f'<span class="crit-name">{_html.escape(c["name"])}{w_badge}</span>'
                 f'<div class="crit-bar-bg">'
                 f'<div class="crit-bar-fill {bc}" style="width:{pct}%"></div>'
                 f'</div>'
                 f'<span class="crit-score">{sc}/10</span>'
                 f'<span class="crit-note">{note}</span>'
                 f'</div>')
    st.markdown(rows, unsafe_allow_html=True)

def render_metrics(metrics):
    skip = {"Data Quality", "_pt_signal"}
    chips = ""
    for k, v in metrics.items():
        if k in skip or not v or v == "N/A": continue
        chips += (f'<span class="metric-chip">'
                  f'<b>{_html.escape(str(k))}</b> {_html.escape(str(v))}'
                  f'</span>')
    if chips:
        st.markdown(chips, unsafe_allow_html=True)

def render_pt_signal(metrics):
    sig = metrics.get("_pt_signal")
    if not sig: return
    if sig.startswith("WARNING"):
        st.markdown(f'<div class="danger-box">⚠️ {_html.escape(sig)}</div>', unsafe_allow_html=True)
    elif sig.startswith("CAUTION"):
        st.markdown(f'<div class="warn-box">⚡ {_html.escape(sig)}</div>', unsafe_allow_html=True)
    elif sig.startswith("UPSIDE"):
        st.markdown(f'<div class="upside-box">↑ {_html.escape(sig)}</div>', unsafe_allow_html=True)

def render_card(r):
    v         = r["verdict"]
    sc        = r["pct"]
    cc        = score_color_class(sc)
    chg       = r["change_pct"]
    chg_str   = f"{chg:+.2f}%" if chg is not None else "—"
    chg_color = "#3fb950" if (chg or 0) >= 0 else "#ff7b72"
    dq        = r.get("dq")
    dq_quality  = dq.quality if dq else "?"
    price_warn  = r.get("price_warning")
    dq_warn = ""
    if price_warn:
        dq_warn += f'<div class="danger-box" style="margin-top:6px;">⚠ {_html.escape(price_warn)}</div>'
    if dq_quality in ("LOW", "MEDIUM"):
        dq_warn += (f'<div class="dq-warn-inline">⚠ Score based on partial data '
                    f'({_html.escape(dq.summary) if dq and dq.summary else "some sources unavailable"})'
                    f'</div>')
    sector_html = ""
    if r.get("sector") and r["sector"] != "—":
        sector_html = f'&nbsp;·&nbsp; <span style="color:#8b949e;">{_html.escape(r["sector"][:30])}</span>'
    st.markdown(f"""
    <div class="score-card">
        <div style="display:flex; justify-content:space-between; align-items:flex-start;">
            <div>
                <div class="ticker-label">{_html.escape(r['ticker'])}</div>
                <div class="name-label">{_html.escape(r['name'])} · {r['type']} · {_html.escape(r['exchange'])}</div>
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
            &nbsp;·&nbsp; Data {dq_badge(dq_quality)}
            {sector_html}
        </div>
        {dq_warn}
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

# ── Load saved tickers from URL on first visit ────────────────────────────────
if "tickers" not in st.session_state:
    saved = st.query_params.get("t", "")
    if saved:
        st.session_state["tickers"] = [t.strip().upper() for t in saved.split(",") if t.strip()]
    else:
        st.session_state["tickers"] = DEFAULT_TICKERS

# ── Ticker input ──────────────────────────────────────────────────────────────
with st.expander("⚙️ Portfolio settings", expanded="tickers" not in st.query_params):
    raw = st.text_area(
        "Tickers (one per line or comma-separated)",
        value="\n".join(st.session_state.get("tickers", DEFAULT_TICKERS)),
        height=180,
        help="Add or remove tickers. ETFs and stocks are detected automatically.",
    )
    col1, col2, col3 = st.columns([1, 1, 2])
    with col1:
        run_btn = st.button("▶ Analyze", type="primary", use_container_width=True)
    with col2:
        save_btn = st.button("💾 Save", use_container_width=True,
                             help="Save your ticker list to the URL — bookmark it to restore on any device")
    with col3:
        st.caption("Results cached 30 min · Parallel fetch · click Analyze to refresh")

    if save_btn:
        tickers_to_save = [t.strip().upper() for t in raw.replace(",", "\n").splitlines() if t.strip()]
        st.session_state["tickers"] = tickers_to_save
        st.query_params["t"] = ",".join(tickers_to_save)
        st.success("✓ Saved! Bookmark this page URL to restore your portfolio on any device.", icon="🔖")

if run_btn:
    tickers = [t.strip().upper() for t in raw.replace(",", "\n").splitlines() if t.strip()]
    st.session_state["tickers"] = tickers
    clear_cache()

tickers = st.session_state.get("tickers", DEFAULT_TICKERS)

if not tickers:
    st.info("Enter at least one ticker above and click Analyze.")
    st.stop()

# ── Parallel analysis — all tickers fetched simultaneously ───────────────────
results: list = []
errors:  list = []

progress    = st.progress(0, text="Starting analysis…")
_done_count = [0]                        # list so closure can mutate it
_done_lock  = threading.Lock()

def _fetch_one(t):
    return cached_fetch(t, FINNHUB_KEY, AV_KEY)

with ThreadPoolExecutor(max_workers=8) as executor:
    future_map = {executor.submit(_fetch_one, t): t for t in tickers}
    for future in as_completed(future_map):
        t = future_map[future]
        with _done_lock:
            _done_count[0] += 1
            done = _done_count[0]
        progress.progress(done / len(tickers),
                          text=f"Analyzed {done}/{len(tickers)} — {t}")
        try:
            results.append(future.result())
        except Exception as e:
            errors.append((t, str(e)))

progress.empty()

# ── Summary bar ───────────────────────────────────────────────────────────────
if results:
    buys  = sum(1 for r in results if r["verdict"] == "BUY")
    holds = sum(1 for r in results if r["verdict"] == "HOLD")
    sells = sum(1 for r in results if r["verdict"] == "SELL")
    avg   = round(sum(r["pct"] for r in results) / len(results))
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Avg Score", f"{avg}/100")
    c2.metric("BUY",   buys)
    c3.metric("HOLD",  holds)
    c4.metric("SELL",  sells)
    st.markdown("---")

# ── Split & sort: stocks BUY→score, ETFs BUY→score ───────────────────────────
_sort_key = lambda r: ({"BUY": 0, "HOLD": 1, "SELL": 2}[r["verdict"]], -r["pct"])
stocks = sorted([r for r in results if r["type"] == "Stock"], key=_sort_key)
etfs   = sorted([r for r in results if r["type"] == "ETF"],   key=_sort_key)

if stocks:
    st.markdown("### 📊 Stocks")
    for r in stocks:
        render_card(r)

if etfs:
    st.markdown("### 📦 ETFs")
    for r in etfs:
        render_card(r)

for ticker, err in errors:
    st.error(f"**{ticker}**: {err}")

# ── Footer ────────────────────────────────────────────────────────────────────
st.markdown("---")
st.caption("Data: yfinance (fundamentals · targets · ETF metrics) · Finnhub (real-time price) · Scores cached 30 min")
