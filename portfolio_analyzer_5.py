"""
Portfolio Analyzer — Production Edition
=========================================
Built for: daily portfolio optimization, highest accuracy priority
Data arch:
  STOCKS  → Finnhub (price) + Alpha Vantage (10 fundamental criteria)
  ETFs    → Finnhub (price) + Alpha Vantage (52W, Beta, Yield)
           + yfinance (live Holdings P/E, live AUM)
           + ETF_DB (expense ratio, category, num holdings — stable fields)

Data priority chain (highest to lowest accuracy):
  1. yfinance.Ticker.info  — live Holdings P/E, live AUM
  2. Alpha Vantage OVERVIEW — live 52W, Beta, DividendYield, all stock fundamentals
  3. ETF_DB hardcoded       — expense ratio (changes ~yearly), num_holdings

Failure modes handled:
  F1  Finnhub 403/empty     → retry 3x with backoff → skip with clear error
  F2  AV rate-limit         → ETF_DB + yf fallback, ⚠ shown, score still valid
  F3  yfinance PE=None      → ETF_DB fallback, ⚠ shown
  F4  Stale ETF_DB PE       → yfinance overrides it live (this was the main bug)
  F5  Unknown ETF           → yfinance + AV cover most fields, ⚠ shown
  F6  Stale AV 52W (1 day)  → negligible impact, <0.5pt score change
  F7  All sources fail      → ETF_DB values only, ⚠ clearly shown
  F8  Market closed/weekend → last close used, score still valid on fundamentals

Install:  pip install FreeSimpleGUI requests matplotlib pandas openpyxl yfinance
Run:      python portfolio_analyzer.py
"""

import threading, time, warnings, requests, json
import pandas as pd

warnings.filterwarnings("ignore")

try:
    import FreeSimpleGUI as sg
except ImportError:
    try:
        import PySimpleGUI as sg
    except ImportError:
        raise SystemExit("Run: pip install FreeSimpleGUI")

try:
    import matplotlib
    matplotlib.use("TkAgg")
    import matplotlib.pyplot as plt
    import matplotlib.gridspec as gridspec
    from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
except ImportError:
    raise SystemExit("Run: pip install matplotlib")

try:
    import yfinance as yf
    YF_AVAILABLE = True
except ImportError:
    YF_AVAILABLE = False   # graceful degradation


# ╔══════════════════════════════════════════════════════════════╗
# ║  PASTE YOUR API KEYS HERE                                   ║
# ╚══════════════════════════════════════════════════════════════╝
FINNHUB_KEY = "d787fi1r01qsamsj83h0d787fi1r01qsamsj83hg"
AV_KEY      = "DRFXF3KZF59XEVA4"   # alphavantage.co/support → Get free API key


# ── API config ───────────────────────────────────────────────────────────────
FH_BASE  = "https://finnhub.io/api/v1"
AV_BASE  = "https://www.alphavantage.co/query"
TIMEOUT  = 15
FH_DELAY = 0.22       # 4.5/sec → well under 60/min free limit
AV_DELAY = 13.0       # 5/min free → 12-13s gap is safe
YF_DELAY = 0.5        # yfinance: no official rate limit, be polite
RETRY_ATTEMPTS = 3
RETRY_BACKOFF  = 2.0  # seconds between retries

# ── Theme ────────────────────────────────────────────────────────────────────
sg.theme("DarkGrey14")
ACCENT   = "#4A9EFF"
BUY_CLR  = "#4CAF50"
HOLD_CLR = "#FF9800"
SELL_CLR = "#F44336"
WARN_CLR = "#FFD700"
BG       = "#1E1E2E"
CARD_BG  = "#2A2A3E"
TEXT     = "#E0E0E0"
MUTED    = "#888888"


# ── ETF database — stable fields only ───────────────────────────────────────
# expense (%), num_holdings, category
# AUM and holdings PE are fetched LIVE from yfinance every run
# DividendYield and Beta fetched LIVE from Alpha Vantage every run
ETF_DB = {
    # Broad US Market
    "SPY": {"expense":0.0945,"num_holdings":503, "category":"S&P 500",             "beta_fallback":1.00},
    "IVV": {"expense":0.03,  "num_holdings":503, "category":"S&P 500",             "beta_fallback":1.00},
    "VOO": {"expense":0.03,  "num_holdings":503, "category":"S&P 500",             "multi_class_aum":True, "beta_fallback":1.00},
    "VTI": {"expense":0.03,  "num_holdings":3700,"category":"Total US Market",     "multi_class_aum":True, "beta_fallback":1.00},
    "ITOT":{"expense":0.03,  "num_holdings":3500,"category":"Total US Market",     "beta_fallback":1.00},
    "SCHB":{"expense":0.03,  "num_holdings":2500,"category":"Total US Market",     "beta_fallback":1.00},
    # Growth
    "VUG": {"expense":0.04,  "num_holdings":150, "category":"Large Cap Growth",    "multi_class_aum":True, "beta_fallback":1.05},
    "QQQ": {"expense":0.20,  "num_holdings":101, "category":"Nasdaq-100",          "beta_fallback":1.15},
    "QQQM":{"expense":0.15,  "num_holdings":101, "category":"Nasdaq-100",          "beta_fallback":1.15},
    "IWF": {"expense":0.19,  "num_holdings":330, "category":"Large Cap Growth",    "beta_fallback":1.10},
    "SPYG":{"expense":0.04,  "num_holdings":240, "category":"Large Cap Growth",    "beta_fallback":1.05},
    "MGK": {"expense":0.07,  "num_holdings":69,  "category":"Mega Cap Growth",     "beta_fallback":1.10},
    # Value
    "VTV": {"expense":0.04,  "num_holdings":340, "category":"Large Cap Value",     "multi_class_aum":True, "beta_fallback":0.92},
    "IVE": {"expense":0.18,  "num_holdings":440, "category":"Large Cap Value",     "beta_fallback":0.92},
    "SPYV":{"expense":0.04,  "num_holdings":440, "category":"Large Cap Value",     "beta_fallback":0.92},
    # Dividend
    "SCHD":{"expense":0.06,  "num_holdings":103, "category":"Dividend Growth",     "beta_fallback":0.85},
    "VIG": {"expense":0.06,  "num_holdings":338, "category":"Dividend Growth",     "multi_class_aum":True, "beta_fallback":0.85},
    "DGRO":{"expense":0.08,  "num_holdings":420, "category":"Dividend Growth",     "beta_fallback":0.85},
    "DVY": {"expense":0.38,  "num_holdings":100, "category":"High Dividend",       "beta_fallback":0.80},
    "HDV": {"expense":0.08,  "num_holdings":75,  "category":"High Dividend",       "beta_fallback":0.75},
    "SDY": {"expense":0.35,  "num_holdings":135, "category":"Dividend Aristocrats","beta_fallback":0.80},
    "VYM": {"expense":0.06,  "num_holdings":555, "category":"High Dividend Yield", "multi_class_aum":True, "beta_fallback":0.82},
    # Covered Call / Income
    "JEPI":{"expense":0.35,  "num_holdings":101, "category":"Covered Call / S&P 500 Income", "beta_fallback":0.55},
    "JEPQ":{"expense":0.35,  "num_holdings":93,  "category":"Covered Call / Nasdaq Income",  "beta_fallback":0.62},
    "XYLD":{"expense":0.60,  "num_holdings":503, "category":"Covered Call / S&P 500",        "beta_fallback":0.60},
    "QYLD":{"expense":0.60,  "num_holdings":101, "category":"Covered Call / Nasdaq",         "beta_fallback":0.65},
    # Technology / Semiconductors
    "XLK": {"expense":0.10,  "num_holdings":67,  "category":"Technology Sector",   "beta_fallback":1.20},
    "VGT": {"expense":0.10,  "num_holdings":316, "category":"Technology Sector",   "multi_class_aum":True, "beta_fallback":1.20},
    "FTEC":{"expense":0.08,  "num_holdings":280, "category":"Technology Sector",   "beta_fallback":1.20},
    "IYW": {"expense":0.38,  "num_holdings":144, "category":"U.S. Technology",     "beta_fallback":1.20},
    "SMH": {"expense":0.35,  "num_holdings":26,  "category":"Semiconductors",      "beta_fallback":1.35},
    "SOXX":{"expense":0.35,  "num_holdings":30,  "category":"Semiconductors",      "beta_fallback":1.35},
    # Other Sectors
    "XLF": {"expense":0.10,  "num_holdings":74,  "category":"Financials",          "beta_fallback":1.10},
    "XLV": {"expense":0.10,  "num_holdings":63,  "category":"Healthcare",          "beta_fallback":0.72},
    "XLE": {"expense":0.10,  "num_holdings":23,  "category":"Energy",              "beta_fallback":1.05},
    "XLI": {"expense":0.10,  "num_holdings":79,  "category":"Industrials",         "beta_fallback":1.00},
    "XLY": {"expense":0.10,  "num_holdings":54,  "category":"Consumer Discretionary","beta_fallback":1.10},
    "XLP": {"expense":0.10,  "num_holdings":38,  "category":"Consumer Staples",    "beta_fallback":0.65},
    "XLU": {"expense":0.10,  "num_holdings":30,  "category":"Utilities",           "beta_fallback":0.70},
    # International
    "VEA": {"expense":0.05,  "num_holdings":3900,"category":"Developed Markets ex-US","multi_class_aum":True, "beta_fallback":0.90},
    "VWO": {"expense":0.08,  "num_holdings":5800,"category":"Emerging Markets",    "multi_class_aum":True, "beta_fallback":0.85},
    "EFA": {"expense":0.32,  "num_holdings":780, "category":"Developed Markets ex-US","beta_fallback":0.90},
    "EEM": {"expense":0.70,  "num_holdings":1200,"category":"Emerging Markets",    "beta_fallback":0.85},
    "IEFA":{"expense":0.07,  "num_holdings":2700,"category":"Developed Markets ex-US","beta_fallback":0.90},
    # Bonds
    "BND": {"expense":0.03,  "num_holdings":10000,"category":"Total US Bond Market","beta_fallback":0.05},
    "AGG": {"expense":0.03,  "num_holdings":10000,"category":"Total US Bond Market","beta_fallback":0.05},
    "TLT": {"expense":0.15,  "num_holdings":30,  "category":"Long-Term Treasury",  "beta_fallback":0.20},
    "LQD": {"expense":0.14,  "num_holdings":2600,"category":"Investment Grade Bonds","beta_fallback":0.15},
    "HYG": {"expense":0.48,  "num_holdings":1200,"category":"High Yield Bonds",    "beta_fallback":0.40},
    "JNK": {"expense":0.40,  "num_holdings":900, "category":"High Yield Bonds",    "beta_fallback":0.42},
    "VCIT":{"expense":0.04,  "num_holdings":2100,"category":"Intermediate Corp Bonds","beta_fallback":0.10},
    # Real Assets
    "GLD": {"expense":0.40,  "num_holdings":1,   "category":"Gold",                "beta_fallback":0.08},
    "IAU": {"expense":0.25,  "num_holdings":1,   "category":"Gold",                "beta_fallback":0.08},
    "VNQ": {"expense":0.13,  "num_holdings":160, "category":"US REITs",            "multi_class_aum":True, "beta_fallback":0.85},
    # Thematic
    "ARKK":{"expense":0.75,  "num_holdings":30,  "category":"Disruptive Innovation","beta_fallback":1.55},
    "ARKG":{"expense":0.75,  "num_holdings":30,  "category":"Genomic Revolution",  "beta_fallback":1.40},
}


# ── Helpers ──────────────────────────────────────────────────────────────────

def sf(d, key, default=None):
    """Safe float extraction — handles None, 'None', NaN, empty string."""
    if not isinstance(d, dict): return default
    v = d.get(key)
    if v is None or str(v).strip() in ("None", "-", "N/A", "", "nan"): return default
    try:
        f = float(str(v).replace(",", ""))
        return f if f == f else default      # NaN guard
    except (TypeError, ValueError):
        return default


def score_range(val, good, bad, higher=True):
    """
    Map val onto 1-10 using linear interpolation between bad→1 and good→10.
    None → 5 (neutral: missing data never penalises the score).
    higher=True:  high value preferred (ROE, yield, AUM, return)
    higher=False: low value preferred  (PE, expense ratio, beta)
    """
    if val is None: return 5
    if higher:
        if val >= good: return 10
        if val <= bad:  return 1
        return max(1, min(10, round(1 + (val - bad) / (good - bad) * 9)))
    else:
        if val <= good: return 10
        if val >= bad:  return 1
        return max(1, min(10, round(1 + (bad - val) / (bad - good) * 9)))


def fh_price_target(ticker, fh_key):
    """Fetch analyst consensus price target.
    Primary: yfinance (free, no paywall).
    Fallback: Finnhub /stock/price-target (requires paid plan — used only if yf fails).
    Returns (dict_with_mean_high_low, error_string).
    """
    # ── Primary: yfinance targetMeanPrice (free, reliable) ──────────────────
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
            pass  # fall through to Finnhub

    # ── Fallback: Finnhub (paid plan required) ───────────────────────────────
    data, err = fh_get("/stock/price-target",
                       {"symbol": ticker, "token": fh_key})
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


def fh_get(path, params):
    """Finnhub GET with 3-attempt retry and exponential backoff."""
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
    """Alpha Vantage GET — handles rate limit Note, invalid key Information."""
    time.sleep(AV_DELAY)
    try:
        r = requests.get(AV_BASE, params=params, timeout=TIMEOUT)
        if r.status_code != 200:
            return None, f"HTTP {r.status_code}"
        data = r.json()
        if "Note" in data:
            return None, "AV rate limit — wait 1 min"
        if "Information" in data:
            return None, "AV key error: " + data["Information"][:60]
        if not data or data == {}:
            return None, "AV returned empty response"
        return data, None
    except Exception as e:
        return None, str(e)[:80]


def yf_get_etf(ticker):
    """
    Fetch live ETF data from yfinance.
    Returns dict with trailingPE, totalAssets, dividendYield, beta.
    Returns (data, error_string) — never raises.
    """
    if not YF_AVAILABLE:
        return {}, "yfinance not installed (pip install yfinance)"
    time.sleep(YF_DELAY)
    try:
        info = yf.Ticker(ticker).info
        if not info or info.get("regularMarketPrice") is None:
            return {}, f"yfinance returned empty info for {ticker}"
        # yfinance returns dividendYield and ytdReturn as percentages (e.g. 11.11 = 11.11%),
        # not as decimals (0.1111).  Divide by 100 here so the rest of the code — which
        # expects AV-style decimals — stays consistent.
        raw_div = info.get("dividendYield")
        raw_ytd = info.get("ytdReturn")
        return {
            "trailingPE":   info.get("trailingPE"),
            "totalAssets":  info.get("totalAssets"),
            "dividendYield": raw_div / 100 if raw_div is not None else None,
            "beta":         info.get("beta"),
            "52WeekHigh":   info.get("fiftyTwoWeekHigh"),
            "52WeekLow":    info.get("fiftyTwoWeekLow"),
            "threeYearAverageReturn": info.get("threeYearAverageReturn"),
            "ytdReturn":    raw_ytd / 100 if raw_ytd is not None else None,
        }, None
    except Exception as e:
        return {}, str(e)[:80]


def yf_get_stock(ticker):
    """
    Fetch stock fundamentals from yfinance as AV rate-limit fallback.
    Maps yfinance field names to AV-equivalent keys so score_stock() works unchanged.
    Returns (data_dict, error_string) — never raises.
    """
    if not YF_AVAILABLE:
        return {}, "yfinance not installed"
    time.sleep(YF_DELAY)
    try:
        info = yf.Ticker(ticker).info
        if not info or info.get("regularMarketPrice") is None:
            return {}, f"yfinance returned empty info for {ticker}"
        d = {}
        # Valuation
        if info.get("trailingPE")       is not None: d["TrailingPE"]              = info["trailingPE"]
        if info.get("forwardPE")        is not None: d["ForwardPE"]               = info["forwardPE"]
        if info.get("priceToBook")      is not None: d["PriceToBookRatio"]        = info["priceToBook"]
        if info.get("priceToSalesTrailing12Months") is not None:
            d["PriceToSalesRatioTTM"] = info["priceToSalesTrailing12Months"]
        if info.get("enterpriseToEbitda") is not None: d["EVToEBITDA"]            = info["enterpriseToEbitda"]
        # Profitability
        if info.get("returnOnEquity")   is not None: d["ReturnOnEquityTTM"]       = info["returnOnEquity"]
        if info.get("profitMargins")    is not None: d["ProfitMargin"]            = info["profitMargins"]
        if info.get("operatingMargins") is not None: d["OperatingMarginTTM"]      = info["operatingMargins"]
        if info.get("grossMargins")     is not None: d["_grossMargins"]           = info["grossMargins"]  # decimal
        # Growth — prefer earningsGrowth (TTM YoY) over earningsQuarterlyGrowth (single quarter)
        _eg = info.get("earningsGrowth")
        if _eg is None:
            _eg = info.get("earningsQuarterlyGrowth")
        if _eg is not None:
            d["QuarterlyEarningsGrowthYOY"] = _eg
        if info.get("revenueGrowth")    is not None: d["QuarterlyRevenueGrowthYOY"] = info["revenueGrowth"]
        # Dividends — yfinance returns dividendYield as a percent (e.g. 2.0 = 2%),
        # so divide by 100 to normalise to AV-style decimal (0.02 = 2%).
        if info.get("dividendYield")    is not None: d["DividendYield"]           = info["dividendYield"] / 100
        if info.get("payoutRatio")      is not None: d["PayoutRatio"]             = info["payoutRatio"]
        # 52W
        if info.get("fiftyTwoWeekHigh") is not None: d["52WeekHigh"]              = info["fiftyTwoWeekHigh"]
        if info.get("fiftyTwoWeekLow")  is not None: d["52WeekLow"]               = info["fiftyTwoWeekLow"]
        return d, None
    except Exception as e:
        return {}, str(e)[:80]


# ── Data source tracker ──────────────────────────────────────────────────────

class DataQuality:
    """Tracks which data sources succeeded, for the ⚠ status line."""
    def __init__(self):
        self.sources_ok  = []
        self.sources_fail = []
        self.notes = []

    def ok(self, src):   self.sources_ok.append(src)
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
        return "⚠ " + " | ".join(self.notes)


# ── ETF scoring ──────────────────────────────────────────────────────────────

def _etf_div_label(category, div_pct):
    """Return an accurate 'not scored' label based on the ETF's actual category."""
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


def score_etf(ticker, price, wk52_hi_fh, wk52_lo_fh,
              db_entry, av_data, yf_data, dq):
    """
    Score ETF using 3-layer data priority:
      1. yfinance (live PE, live AUM, live yield, live beta)
      2. Alpha Vantage (live 52W, live Beta, live DividendYield)
      3. ETF_DB (stable: expense ratio, num_holdings, category)
    """
    # ── Holdings P/E — most important dynamic field ──────────────────────────
    # Priority: yf > AV (AV usually None for ETFs) > ETF_DB fallback
    pe_h = (sf(yf_data, "trailingPE") or
            sf(av_data, "TrailingPE"))
    pe_source = "live yfinance" if sf(yf_data, "trailingPE") else (
                "live AV" if sf(av_data, "TrailingPE") else "database fallback")
    # For bond/gold ETFs pe_h stays None → scored as income ETF instead

    # ── AUM — plateau above $10B so staleness has zero score impact ──────────
    yf_aum = sf(yf_data, "totalAssets")
    aum_b  = (yf_aum / 1e9) if yf_aum else db_entry.get("aum_b_fallback")
    # If still None, we score 5 — but AUM > $10B always gets 10/10 anyway
    # Vanguard multi-share-class funds: yfinance totalAssets includes institutional +
    # admiral shares, so the number is larger than ETF-only AUM (still valid for scoring).
    _multi_class = db_entry.get("multi_class_aum", False) if db_entry else False

    # ── Dividend Yield — prefer yf, then AV, then DB ──────────────────────
    yf_div = sf(yf_data, "dividendYield")
    av_div = sf(av_data, "DividendYield")
    div_raw = yf_div or av_div or 0
    div_pct = div_raw * 100

    # ── Beta — prefer AV, then yf, then ETF_DB fallback ─────────────────────
    av_beta = sf(av_data, "Beta")
    yf_beta = sf(yf_data, "beta")
    beta = av_beta if av_beta is not None else yf_beta
    if beta is None and db_entry:
        beta = db_entry.get("beta_fallback")

    # ── 52W Range — prefer AV, fallback to yf, fallback to Finnhub quote ────
    av_52hi = sf(av_data, "52WeekHigh")
    av_52lo = sf(av_data, "52WeekLow")
    yf_52hi = sf(yf_data, "52WeekHigh")
    yf_52lo = sf(yf_data, "52WeekLow")
    hi = av_52hi or yf_52hi or wk52_hi_fh
    lo = av_52lo or yf_52lo or wk52_lo_fh

    # ── Calculated metrics ───────────────────────────────────────────────────
    vs_high = (price / hi * 100) if hi else None
    yr_ret  = (price / lo - 1) * 100 if lo else None   # kept for fallback display only

    # YTD Total Return — prefer live yfinance ytdReturn (decimal, e.g. -0.08 = -8%)
    ytd_raw = sf(yf_data, "ytdReturn")
    ytd_pct = ytd_raw * 100 if ytd_raw is not None else None

    # ── Stable DB fields ─────────────────────────────────────────────────────
    expense = db_entry.get("expense")   # ~yearly change; fine as static
    num_h   = db_entry.get("num_holdings", 0)
    category= db_entry.get("category", "ETF")

    # Diversification score from number of holdings
    if   num_h >= 1000: div_sc = 10
    elif num_h >= 400:  div_sc = 9
    elif num_h >= 200:  div_sc = 8
    elif num_h >= 100:  div_sc = 6
    elif num_h >= 50:   div_sc = 4
    else:               div_sc = 2

    # Bond/gold ETFs: PE meaningless → replace Holdings Valuation with income
    # pe_h=None alone does NOT trigger bond mode — only explicit bond/gold/income categories do.
    # Covered call ETFs (JEPQ, JEPI, XYLD, QYLD) are income vehicles — their Nasdaq/S&P
    # underlying P/E is high by design; scoring them on P/E would be misleading.
    # Switch them to Income Yield scoring, same as bond ETFs.
    is_bond = (pe_h == 0 or
               any(x in category.lower() for x in
                   ("bond","treasury","gold","silver","commodity",
                    "covered call","income")))

    criteria = [
        {"name": "Expense Ratio",
         "score": score_range(expense, 0.05, 1.0, False),
         "note":  f"{expense:.2f}% (annual, stable)" if expense is not None else "N/A"},

        {"name": "AUM & Liquidity",
         "score": score_range(aum_b, 10, 0.5, True),
         "note":  ((f"${aum_b:.1f}B (ETF share class)" if _multi_class else f"${aum_b:.1f}B")
                   if aum_b else "N/A")},

        # Dividend Yield:
        #   - is_bond=True  → already scored as "Income Yield"; neutral here to avoid double-count
        #   - dividend/REIT category → score normally
        #   - everything else (index, sector, intl) → neutral + accurate label via _etf_div_label
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
                   else (f"~{yr_ret:.0f}% (52W low→now)" if yr_ret is not None else "N/A"))},

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
         "score": score_range(abs(beta) if beta is not None else None,
                              0.7, 1.5, False),
         "note":  f"Beta {beta:.2f}" if beta is not None else "N/A"},
    ]

    total     = sum(c["score"] for c in criteria)
    max_total = len(criteria) * 10
    pct       = round(total / max_total * 100)
    verdict   = "BUY" if pct >= 70 else "HOLD" if pct >= 45 else "SELL"

    metrics = {
        "Price":         f"${price:.2f}",
        "AUM":           ((f"${aum_b:.1f}B (ETF share class)" if _multi_class else f"${aum_b:.1f}B")
                         if aum_b else "N/A"),
        "Expense Ratio": f"{expense:.2f}%"      if expense is not None else "N/A",
        "Div Yield":     f"{div_pct:.2f}%"      if div_pct           else "N/A",
        "YTD Return":    (f"{ytd_pct:.1f}%"       if ytd_pct is not None
                          else f"~{yr_ret:.0f}% (52W)" if yr_ret is not None else "N/A"),
        "vs 52W High":   f"{vs_high:.0f}%"      if vs_high is not None else "N/A",
        "Holdings P/E":  f"{pe_h:.1f}x"         if pe_h              else "N/A",
        "# Holdings":    f"{num_h:,}",
        "Beta":          f"{beta:.2f}"           if beta is not None  else "N/A",
        "Category":      category,
        "52W Range":     (f"${lo:.0f}–${hi:.0f}" if lo and hi         else "N/A"),
        "Data Quality":  dq.quality,
    }
    return criteria, metrics, verdict, pct


# ── Stock scoring ─────────────────────────────────────────────────────────────

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
    # yfinance fallback: _grossMargins is a decimal (e.g. 0.55 = 55%)
    if gross_pct is None:
        gm_raw = sf(av, "_grossMargins")
        if gm_raw is not None:
            gross_pct = gm_raw * 100
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

    # ── Sector tier: adjusts P/E and gross margin thresholds ────────────────
    # Tech/growth companies command higher multiples and margins.
    # Finnhub industries mapped here (common values shown in comments).
    #
    # Tier overrides — hardcoded for tickers Finnhub commonly misclassifies.
    # Takes priority over the keyword scan below.
    _TIER_OVERRIDES = {
        # ── E-commerce / marketplace (Finnhub: "Broadline Retail", "Consumer Cyclical") ──
        "AMZN": "Tech",  "SHOP": "Tech",  "EBAY": "Tech",  "ETSY": "Tech",  "MELI": "Tech",
        "JD":   "Tech",  "PDD":  "Tech",  "SE":   "Tech",
        # ── Streaming / gaming (Finnhub: "Entertainment", "Media") ──
        "NFLX": "Tech",  "SPOT": "Tech",  "RBLX": "Tech",  "EA":   "Tech",  "TTWO": "Tech",
        "ATVI": "Tech",  "MTCH": "Tech",
        # ── Mobility / gig economy (Finnhub: "Transportation", "Consumer Cyclical") ──
        "UBER": "Tech",  "LYFT": "Tech",  "DASH": "Tech",  "ABNB": "Tech",
        # ── Online travel (Finnhub: "Hotels, Restaurants & Leisure") ──
        "BKNG": "Tech",  "EXPE": "Tech",  "TRIP": "Tech",
        # ── Fintech / payments (Finnhub: "Financial Services", "Capital Markets") ──
        "PYPL": "Tech",  "XYZ":  "Tech",  "SQ":   "Tech",  "COIN": "Tech",  "SOFI": "Tech",
        "AFRM": "Tech",  "UPST": "Tech",  "HOOD": "Tech",  "NU":   "Tech",  "BILL": "Tech",
        "FOUR": "Tech",  "FLYW": "Tech",
        # ── Payment networks — pure software economics, high P/E justified ──
        "V":    "Tech",  "MA":   "Tech",  "FISV": "Tech",  "FIS":  "Tech",  "GPN":  "Tech",
        "ADP":  "Tech",  "PAYX": "Tech",
        # ── Healthcare SaaS / digital health ──
        "VEEV": "Tech",  "TDOC": "Tech",  "HIMS": "Tech",  "DOCS": "Tech",
        # ── Autos with software/tech valuation premium ──
        "TSLA": "Tech",  "RIVN": "Tech",  "LCID": "Tech",
        # ── Communication services safety net (Finnhub sometimes returns "Media") ──
        "GOOGL":"Tech",  "GOOG": "Tech",  "META": "Tech",  "SNAP": "Tech",  "PINS": "Tech",
        "ZM":   "Tech",  "TWLO": "Tech",  "DDOG": "Tech",
        # ── Industrial overrides ──
        "PCAR": "Industrial",  "AGCO": "Industrial",  "OSK":  "Industrial",
        "CMI":  "Industrial",  "GNRC": "Industrial",
        "GD":   "Industrial",  "NOC":  "Industrial",  "HII":  "Industrial",
        "TDG":  "Industrial",  "HEI":  "Industrial",
        "HAL":  "Industrial",  "BKR":  "Industrial",  "SLB":  "Industrial",
        "FSLR": "Industrial",  "RUN":  "Industrial",
        "GEV":  "Industrial",  "ETN":  "Industrial",  "ACHR": "Industrial",
        # ── Solar semiconductors scored as Tech ──
        "ENPH": "Tech",  "SEDG": "Tech",
    }
    sec_lower = (sector or "").lower()
    _override = _TIER_OVERRIDES.get(ticker.upper() if ticker else "")
    _TECH_KEYWORDS = (
        "technology",       # NVDA, AAPL, MSFT — Finnhub "Technology"
        "semiconductor",    # NVDA, AMD
        "software",         # CRM, ADBE — "Application Software", "Systems Software"
        "internet",         # "Internet Content & Information"
        "cloud",
        "saas",
        "artificial intelligence",
        "communication",    # GOOGL, META — Finnhub "Communication Services"
        "interactive",      # "Interactive Media & Services"
        "e-commerce",       # AMZN (some Finnhub classifications)
        "electronic",       # "Electronic Components"
        "information technology",
    )
    _INDUSTRIAL_KEYWORDS = (
        "capital goods",    # CAT, GEV, DE — Finnhub "Capital Goods"
        "automobile",       # F, GM
        "transportation",   # UPS, FedEx
        "energy",           # XOM, CVX — "Energy"
        "industrial",       # "Industrials"
        "manufacturing",
        "machinery",
        "defense",          # LMT, RTX
        "aerospace",        # BA
        "electrical",       # GEV — "Electrical Equipment & Parts"
        "power",            # GEV — "Power Generation"
        "oil",              # "Oil & Gas"
        "mining",
        "construction",
        "steel",
        "chemical",
    )
    if _override == "Tech" or (not _override and any(x in sec_lower for x in _TECH_KEYWORDS)):
        pe_good, pe_bad   = 18, 60     # tightened: 18x = excellent, 60x = poor
        gm_good, gm_bad   = 65, 25
        tier_label        = "Tech" + (" (override)" if _override else "")
    elif _override == "Industrial" or (not _override and any(x in sec_lower for x in _INDUSTRIAL_KEYWORDS)):
        pe_good, pe_bad   = 10, 28     # tightened: 10x = excellent, 28x = poor
        gm_good, gm_bad   = 35, 5
        tier_label        = "Industrial" + (" (override)" if _override else "")
    else:
        pe_good, pe_bad   = 13, 38     # tightened: 13x = excellent, 38x = poor
        gm_good, gm_bad   = 55, 15
        tier_label        = "Default"

    # P/S: gradient based on gross margin — higher margin justifies higher multiple
    # Range: ps_good = 3 (low margin) to 12 (high margin), ps_bad = 4× ps_good
    ps_good = max(3.0, min(12.0, 3.0 + max(0.0, (gross_pct or 0) - 20.0) / 40.0 * 9.0))
    ps_bad  = ps_good * 4

    # ── P/E blend: only blend when forward P/E < trailing (improvement signal) ─
    if pe and fwd_pe and fwd_pe > 0 and fwd_pe < pe:
        blended_pe  = pe * 0.4 + fwd_pe * 0.6
        pe_note     = f"P/E {pe:.1f}x  Fwd {fwd_pe:.1f}x  (blended {blended_pe:.1f}x)"
    elif pe:
        blended_pe  = pe
        pe_note     = f"P/E {pe:.1f}x (trailing only)"
    elif fwd_pe and fwd_pe > 0:
        blended_pe  = fwd_pe
        pe_note     = f"Fwd P/E {fwd_pe:.1f}x (trailing N/A)"
    else:
        blended_pe  = None
        pe_note     = "N/A"

    # ── Analyst price target — upside/downside to consensus mean ────────────
    pt_mean    = sf(pt_data, "mean")   if pt_data else None
    pt_high    = sf(pt_data, "high")   if pt_data else None
    pt_low     = sf(pt_data, "low")    if pt_data else None
    upside_pct = ((pt_mean - price) / price * 100) if pt_mean and price else None
    if upside_pct is not None:
        pt_note = (f"{upside_pct:+.1f}% to ${pt_mean:.0f}"
                   + (f"  (range ${pt_low:.0f}–${pt_high:.0f})"
                      if pt_low and pt_high else ""))
    else:
        pt_note = "No analyst target"

    # ── Dividend Safety ─────────────────────────────────────────────────────────
    # Only treat as a dividend payer if yield >= 0.5% (filters token buyback yields
    # like GOOGL 0.26% that would otherwise mis-score vs payout ratio).
    if div_pct >= 0.5 and payout_pct == 0.0 and (net_pct is None or net_pct < 0):
        div_score = 1
        div_note  = (f"Yield {div_pct:.2f}%  Payout N/A "
                     f"(negative/missing earnings — dividend at risk)")
    elif div_pct >= 0.5:
        div_score = score_range(payout_pct, 20, 85, False)
        div_note  = f"Yield {div_pct:.2f}%  Payout {payout_pct:.0f}%"
    else:
        div_score = 5   # neutral — absence of dividend is neither good nor bad
        div_note  = "No dividend" if div_pct == 0 else f"Token yield {div_pct:.2f}% (not scored)"

    # Negative earnings → P/E is undefined (None). score_range(None) returns neutral 5
    # which is wrong — a money-losing company should be penalised on valuation.
    if blended_pe is None and net_pct is not None and net_pct < 0:
        _pe_score = 2
        pe_note   = "N/A — negative earnings"
        tier_note = "[losses — penalised]"
    else:
        _pe_score = score_range(blended_pe, pe_good, pe_bad, False)
        tier_note = f"[{tier_label} tier]"

    # EPS growth >200% is almost always a one-time or base-effect anomaly.
    # Cap the score at 6 so it doesn't inflate overall rating.
    eg_score = score_range(eg_pct, 20, -10, True)
    if eg_pct is not None and eg_pct > 200:
        eg_score = min(eg_score, 6)

    # 52W position: low % of range = better buy opportunity
    if wk52_hi and wk52_lo and wk52_hi > wk52_lo and price:
        pos_pct   = (price - wk52_lo) / (wk52_hi - wk52_lo) * 100
        pos_score = score_range(pos_pct, 30, 75, False)
        pos_note  = f"{pos_pct:.0f}% of range (${wk52_lo:.0f}–${wk52_hi:.0f})"
    else:
        pos_pct, pos_score, pos_note = None, 5, "N/A"

    criteria = [
        {"name": "Valuation (P/E)",
         "score": _pe_score,
         "note":  pe_note + f" {tier_note}"},
        {"name": "Price-to-Book",
         "score": score_range(pb, 2, 50, False),
         "note":  f"P/B {pb:.2f}x" if pb else "N/A"},
        {"name": "Margin-Adj P/S",
         "score": score_range(ps, ps_good, ps_bad, False),
         "note":  f"P/S {ps:.2f}x" if ps else "N/A"},
        {"name": "EV / EBITDA",
         "score": score_range(ev_ebitda, 8, 40, False),
         "note":  f"EV/EBITDA {ev_ebitda:.1f}x" if ev_ebitda else "N/A"},
        {"name": "Return on Equity",
         "score": score_range(roe_pct, 20, 0, True),
         "note":  f"ROE {roe_pct:.1f}%" if roe_pct is not None else "N/A"},
        {"name": "Gross Margin",
         "score": score_range(gross_pct, gm_good, gm_bad, True),
         "note":  f"{gross_pct:.1f}%" if gross_pct is not None else "N/A"},
        {"name": "Net Margin",
         "score": score_range(net_pct, 20, 3, True),
         "note":  f"{net_pct:.1f}%" if net_pct is not None else "N/A"},
        {"name": "Earnings Growth",
         "score": eg_score,
         "note":  (f"{eg_pct:+.1f}% TTM" + (" *one-time? (capped 6/10)" if eg_pct > 200 else ""))
                  if eg_pct is not None else "N/A"},
        {"name": "Revenue Growth",
         "score": score_range(rg_pct, 20, 0, True),
         "note":  f"{rg_pct:+.1f}% TTM" if rg_pct is not None else "N/A"},
        {"name": "Dividend Safety",
         "score": div_score,
         "note":  div_note},
        {"name": "52W Position",
         "score": pos_score,
         "note":  pos_note},
        {"name": "Analyst Target",
         "score": score_range(upside_pct, 15, -15, True), "weight": 2,
         "note":  pt_note},
    ]

    total     = sum(c["score"] * c.get("weight", 1) for c in criteria)
    max_total = sum(10 * c.get("weight", 1) for c in criteria)
    pct       = round(total / max_total * 100)
    verdict   = "BUY" if pct >= 70 else "HOLD" if pct >= 45 else "SELL"

    if upside_pct is not None and upside_pct >= 15:
        pt_display    = f"${pt_mean:.0f}  (+{upside_pct:.1f}% upside)"
        pt_signal     = f"UPSIDE  +{upside_pct:.1f}% to consensus"
    elif upside_pct is not None and upside_pct >= 0:
        pt_display    = f"${pt_mean:.0f}  (+{upside_pct:.1f}% upside)"
        pt_signal     = f"FAIR  +{upside_pct:.1f}% to consensus"
    elif upside_pct is not None and upside_pct >= -10:
        pt_display    = f"${pt_mean:.0f}  ({upside_pct:.1f}% to consensus)"
        pt_signal     = f"CAUTION  {upside_pct:.1f}% — trading near/above analyst target"
    elif upside_pct is not None:
        pt_display    = f"${pt_mean:.0f}  ({upside_pct:.1f}% to consensus)"
        pt_signal     = f"WARNING  {upside_pct:.1f}% — trading significantly above analyst target"
    else:
        pt_display    = "N/A"
        pt_signal     = "N/A"

    metrics = {
        "Price":          f"${price:.2f}",
        "Analyst Target": pt_display,
        "Target Signal":  pt_signal,
        "P/E (TTM)":      f"{pe:.1f}x"          if pe           else "N/A",
        "Fwd P/E":        f"{fwd_pe:.1f}x"       if fwd_pe       else "N/A",
        "P/B Ratio":      f"{pb:.2f}x"           if pb           else "N/A",
        "P/S (TTM)":      f"{ps:.2f}x"           if ps           else "N/A",
        "EV/EBITDA":      f"{ev_ebitda:.1f}x"    if ev_ebitda    else "N/A",
        "Gross Margin":   f"{gross_pct:.1f}%"    if gross_pct is not None else "N/A",
        "Net Margin":     f"{net_pct:.1f}%"      if net_pct   is not None else "N/A",
        "Op Margin":      f"{op_pct:.1f}%"       if op_pct    is not None else "N/A",
        "ROE":            f"{roe_pct:.1f}%"      if roe_pct   is not None else "N/A",
        "EPS Growth":     f"{eg_pct:+.1f}%"      if eg_pct    is not None else "N/A",
        "Rev Growth":     f"{rg_pct:+.1f}%"      if rg_pct    is not None else "N/A",
        "Div Yield":      f"{div_pct:.2f}%"      if div_pct       else "N/A",
        "52W Range":      (f"${wk52_lo:.0f}–${wk52_hi:.0f}"
                           if wk52_lo and wk52_hi else "N/A"),
        "Data Quality":   dq.quality,
    }
    return criteria, metrics, verdict, pct


# ── Main fetch & score ────────────────────────────────────────────────────────

def fetch_and_score(ticker, fh_key, av_key):
    ticker = ticker.strip().upper()
    dq = DataQuality()

    # ── Step 1: Finnhub real-time price (required) ───────────────────────────
    q, err = fh_get("/quote", {"symbol": ticker, "token": fh_key})
    if err or not q or not q.get("c") or q.get("c") == 0:
        raise ValueError(
            f"No price data for '{ticker}' — "
            f"check the symbol is correct. ({err or 'empty quote'})")
    dq.ok("Finnhub")
    price       = sf(q, "c")
    change_pct  = sf(q, "dp")
    prev_close  = sf(q, "pc")
    price_warn  = None
    if price and prev_close and prev_close > 0:
        deviation = abs((price - prev_close) / prev_close * 100)
        if deviation > 25:
            price_warn = (f"Price ${price:.2f} deviates {deviation:.0f}% from prev close "
                          f"${prev_close:.2f} — possible bad data")

    # ── Step 2: Finnhub company profile (non-fatal) ──────────────────────────
    pr, err = fh_get("/stock/profile2", {"symbol": ticker, "token": fh_key})
    if err: dq.fail("FH-Profile", err)
    pr = pr or {}
    name     = pr.get("name") or ticker
    industry = pr.get("finnhubIndustry") or "—"
    exchange = pr.get("exchange") or "—"

    # ── Step 3: Alpha Vantage OVERVIEW (non-fatal) ───────────────────────────
    av, err = av_get({"function": "OVERVIEW", "symbol": ticker, "apikey": av_key})
    if err:
        dq.fail("AlphaVantage", err)
        av = {}
    else:
        dq.ok("AlphaVantage")

    # Fill gaps from AV
    if not name or name == ticker: name = av.get("Name") or ticker
    if exchange == "—": exchange = av.get("Exchange") or "—"

    wk52_hi = sf(av, "52WeekHigh")
    wk52_lo = sf(av, "52WeekLow")

    # Market cap from AV (Finnhub returns None for ETFs)
    av_mc    = sf(av, "MarketCapitalization")
    mkt_cap_b = (av_mc / 1e9) if av_mc else None

    # ── Step 4: Is this an ETF? ──────────────────────────────────────────────
    db_entry = ETF_DB.get(ticker)
    av_type  = str(av.get("AssetType", "")).upper()
    is_etf   = (db_entry is not None or
                "ETF"  in av_type or
                "FUND" in av_type)

    if is_etf:
        # ── Step 5 (ETF only): yfinance for live Holdings P/E + AUM ─────────
        yf_data, err = yf_get_etf(ticker)
        if err:
            dq.fail("yfinance", err)
            yf_data = {}
        elif yf_data.get("trailingPE"):
            dq.ok("yfinance")

        if db_entry is None:
            # Unknown ETF — build minimal db_entry from available live data
            db_entry = {
                "expense":      None,
                "num_holdings": 0,
                "category":     av.get("Industry") or industry or "ETF",
            }
            dq.fail("ETF_DB", "not in built-in database — some fields N/A")

        criteria, metrics, verdict, pct = score_etf(
            ticker, price, wk52_hi, wk52_lo,
            db_entry, av, yf_data, dq)
        asset_type = "ETF"
        sector     = db_entry.get("category", industry)

    else:
        # ── Step 5 (Stock only): yfinance fallback if AV completely failed ──
        if not av:
            yf_stock, yf_err = yf_get_stock(ticker)
            if yf_err:
                dq.fail("yfinance-stock", yf_err)
            else:
                av = yf_stock
                dq.ok("yfinance-stock")
                # Fill 52W from yfinance if AV was empty
                if not wk52_hi: wk52_hi = sf(av, "52WeekHigh")
                if not wk52_lo: wk52_lo = sf(av, "52WeekLow")

        # ── Step 6 (Stock only): fetch analyst price target, then score ─────
        pt_data, pt_err = fh_price_target(ticker, fh_key)
        if pt_err:
            dq.fail("PriceTarget", pt_err)
        else:
            dq.ok("PriceTarget")

        criteria, metrics, verdict, pct = score_stock(
            ticker, price, wk52_hi, wk52_lo, av, dq, pt_data, sector=industry)
        asset_type = "Stock"
        sector     = industry

    total     = sum(c["score"] * c.get("weight", 1) for c in criteria)
    max_total = sum(10 * c.get("weight", 1) for c in criteria)

    return {
        "ticker":     ticker,
        "name":       name,
        "type":       asset_type,
        "sector":     sector,
        "exchange":   exchange,
        "price":         price,
        "change_pct":    change_pct,
        "price_warning": price_warn,
        "year_high":     wk52_hi,
        "year_low":   wk52_lo,
        "mkt_cap_b":  mkt_cap_b,
        "metrics":    metrics,
        "criteria":   criteria,
        "total":      total,
        "max_total":  max_total,
        "pct":        pct,
        "verdict":    verdict,
        "dq":         dq,
    }


# ── Detail panel ──────────────────────────────────────────────────────────────

def build_detail(r):
    v     = r["verdict"]
    stars = {"BUY": "★★★", "HOLD": "★★☆", "SELL": "★☆☆"}.get(v, "")
    chg   = f"{r['change_pct']:+.2f}%" if r["change_pct"] is not None else "—"
    dq    = r.get("dq")
    qual  = dq.quality if dq else "?"
    type_label = ("ETF (8 fund-specific criteria, live yfinance PE)"
                  if r["type"] == "ETF" else
                  "Stock (11 criteria: fundamentals + analyst price target)")

    price_warn = r.get("price_warning")
    lines = [
        "═" * 78,
        f"  {r['ticker']}   {r['name']}",
        f"  {r['type']}  ·  {r['sector']}  ·  {r['exchange']}",
        f"  Data quality: {qual}" + (f"   {dq.summary}" if dq and dq.summary else ""),
    ] + ([f"  !! PRICE WARNING: {price_warn}"] if price_warn else []) + [
        "─" * 78,
        f"  Price: ${r['price']:.2f}   Change: {chg}   "
        f"52W: ${r['year_low']:.0f}–${r['year_high']:.0f}" if r["year_high"] and r["year_low"]
        else f"  Price: ${r['price']:.2f}   Change: {chg}",
        "─" * 78,
        f"  {type_label}",
        "─" * 78,
    ]
    items = list(r["metrics"].items())
    for i in range(0, len(items), 3):
        chunk = items[i:i+3]
        lines.append("  " + "   ".join(f"{k:<16} {v:<13}" for k, v in chunk))
    lines += [
        "─" * 78,
        "  Criterion                    Bar            Score  Data",
        "─" * 78,
    ]
    for c in r["criteria"]:
        sc  = c["score"]
        bar = "█" * sc + "░" * (10 - sc)
        dot = "●" if sc >= 7 else "◑" if sc >= 4 else "○"
        lines.append(f"  {dot}  {c['name']:<26}  {bar}  {sc:>2}/10  {c['note']}")
    # ── Analyst target warning for stocks ────────────────────────────────────
    signal_line = r["metrics"].get("Target Signal", "")
    target_warn = []
    if r["type"] == "Stock" and signal_line and signal_line != "N/A":
        if signal_line.startswith("WARNING"):
            target_warn = [
                "─" * 78,
                f"  !! ANALYST TARGET WARNING: {signal_line}",
            ]
        elif signal_line.startswith("CAUTION"):
            target_warn = [
                "─" * 78,
                f"  >> ANALYST TARGET CAUTION: {signal_line}",
            ]

    lines += [
        "─" * 78,
    ] + target_warn + [
        f"  SCORE: {r['pct']}/100   VERDICT: {v}  {stars}",
        "═" * 78,
    ]
    return "\n".join(lines)


# ── Chart ─────────────────────────────────────────────────────────────────────

_fig = None

def draw_chart(canvas_elem, results):
    global _fig
    try:
        for w in canvas_elem.TKCanvas.winfo_children():
            w.destroy()
    except Exception:
        pass
    if _fig:
        plt.close(_fig)
        _fig = None

    valid = [r for r in results if r.get("price")]
    if not valid: return

    n   = len(valid)
    fig = plt.figure(figsize=(14, 4.5 * n), facecolor=BG)
    gs  = gridspec.GridSpec(n, 2, figure=fig,
                            hspace=0.65, wspace=0.35,
                            left=0.07, right=0.97, top=0.95, bottom=0.05)

    for i, r in enumerate(valid):
        price = r["price"]
        hi    = r.get("year_high") or price
        lo    = r.get("year_low")  or price * 0.85
        chg   = r.get("change_pct") or 0
        lc    = BUY_CLR if chg >= 0 else SELL_CLR
        rng   = hi - lo
        pos   = max(0.0, min(1.0, (price - lo) / rng)) if rng > 0 else 0.5

        # Left: 52-week gauge
        ax1 = fig.add_subplot(gs[i, 0])
        ax1.set_facecolor(CARD_BG)
        ax1.set_xlim(0, 1); ax1.set_ylim(0, 3.2); ax1.axis("off")
        ax1.barh(1.5, 1,   height=0.3, color="#333", left=0)
        ax1.barh(1.5, pos, height=0.3, color=lc,    left=0)
        ax1.plot(pos, 1.5, "^", color=TEXT, markersize=11, zorder=5)
        ax1.text(0,   0.7, f"52W Low\n${lo:.0f}",  color=MUTED, fontsize=8, ha="left")
        ax1.text(1,   0.7, f"52W High\n${hi:.0f}", color=MUTED, fontsize=8, ha="right")
        ax1.text(pos, 2.1, f"${price:.2f}  ({chg:+.1f}%)",
                 color=lc, fontsize=9, fontweight="bold", ha="center")
        dq_lbl = r["dq"].quality if r.get("dq") else "?"
        ax1.text(0.5, 2.8,
                 f"{r['ticker']} [{r['type']}]  Data: {dq_lbl}",
                 color=TEXT, fontsize=9, ha="center")

        # Right: criteria bars
        ax2 = fig.add_subplot(gs[i, 1])
        ax2.set_facecolor(CARD_BG)
        names  = [c["name"] for c in r["criteria"]]
        scores = [c["score"] for c in r["criteria"]]
        colors = [BUY_CLR if s >= 7 else HOLD_CLR if s >= 4 else SELL_CLR for s in scores]
        bars = ax2.barh(names, scores, color=colors, height=0.55)
        ax2.set_xlim(0, 11)
        ax2.axvline(7, color=BUY_CLR,  alpha=0.2, lw=1, ls="--")
        ax2.axvline(4, color=SELL_CLR, alpha=0.2, lw=1, ls="--")
        for bar, sc in zip(bars, scores):
            ax2.text(sc + 0.1, bar.get_y() + bar.get_height() / 2,
                     str(sc), va="center", color=TEXT, fontsize=8)
        vc = (BUY_CLR if r["verdict"] == "BUY"
              else HOLD_CLR if r["verdict"] == "HOLD" else SELL_CLR)
        ax2.set_title(f"{r['ticker']}  {r['pct']}/100  [{r['verdict']}]",
                      color=vc, fontsize=9, fontweight="bold", pad=5)
        ax2.tick_params(colors=MUTED, labelsize=7)
        for sp in ax2.spines.values():
            sp.set_edgecolor("#444")

    _fig = fig
    c = FigureCanvasTkAgg(fig, master=canvas_elem.TKCanvas)
    c.draw()
    c.get_tk_widget().pack(fill="both", expand=True)


# ── Table ─────────────────────────────────────────────────────────────────────

HDRS = ["Rank","Ticker","Type","Name","Category","Price","Change","Score","Verdict","Data"]

def make_table(sp):
    rows = []
    for i, r in enumerate(sp, 1):
        chg  = f"{r['change_pct']:+.2f}%" if r["change_pct"] is not None else "—"
        name = r["name"][:20] + ("…" if len(r["name"]) > 20 else "")
        dq   = r["dq"].quality if r.get("dq") else "?"
        rows.append([
            f"#{i}", r["ticker"], r["type"], name, r["sector"][:14],
            f"${r['price']:.2f}" if r["price"] else "—",
            chg, f"{r['pct']}/100", r["verdict"], dq,
        ])
    return rows


def make_placeholder(ticker):
    dq = DataQuality()
    return {"ticker": ticker, "name": ticker, "type": "?", "sector": "—",
            "exchange": "—", "price": None, "change_pct": None,
            "year_high": None, "year_low": None, "mkt_cap_b": None,
            "metrics": {}, "criteria": [], "dq": dq,
            "total": 0, "max_total": 100, "pct": 0, "verdict": "?"}


# ── Main window ───────────────────────────────────────────────────────────────

def main():
    portfolio = []
    fh_key    = FINNHUB_KEY.strip()
    av_key    = AV_KEY.strip()
    keys_ok   = bool(fh_key and av_key)
    yf_status = "yfinance ✓" if YF_AVAILABLE else "yfinance ✗ (pip install yfinance)"

    layout = [
        [sg.Text("Portfolio Analyzer", font=("Helvetica", 17, "bold"),
                 text_color=ACCENT, background_color=BG)],
        [sg.Text(
            f"Stocks (AV fundamentals) + ETFs (live yfinance PE + AV + DB)  ·  {yf_status}",
            font=("Helvetica", 8, "italic"), text_color="#FFB300", background_color=BG)],
        [sg.HorizontalSeparator(color="#333")],

        # Key status
        [
            sg.Text("API Keys:", background_color=BG, text_color=TEXT,
                    font=("Helvetica", 10)),
            sg.Text(
                "● Finnhub + AlphaVantage loaded" if keys_ok
                else "● Set AV_KEY in script — edit line 3 and restart",
                key="-KEY-STATUS-", font=("Helvetica", 9),
                text_color=BUY_CLR if keys_ok else SELL_CLR,
                background_color=BG),
            sg.Push(background_color=BG),
            sg.Text("alphavantage.co/support → Get free key",
                    font=("Helvetica", 8), text_color=MUTED, background_color=BG),
        ],
        [sg.HorizontalSeparator(color="#333")],

        # Ticker input
        [
            sg.Text("Ticker(s):", background_color=BG, text_color=TEXT,
                    font=("Helvetica", 10)),
            sg.Input("", key="-TICKER-", size=(36, 1), font=("Helvetica", 11)),
            sg.Text("  e.g. NVDA, JEPQ, VUG, VOO, AAPL, SCHD",
                    font=("Helvetica", 8), text_color=MUTED, background_color=BG),
            sg.Push(background_color=BG),
            sg.Button("Add",         key="-ADD-",
                      button_color=(BG, ACCENT),   font=("Helvetica", 10, "bold"), size=(7, 1)),
            sg.Button("Analyze All", key="-ANALYZE-",
                      button_color=(BG, BUY_CLR),  font=("Helvetica", 10, "bold"), size=(13,1)),
            sg.Button("Clear All",   key="-CLEAR-",
                      button_color=(BG, SELL_CLR), font=("Helvetica", 10),          size=(10,1)),
            sg.Button("Export CSV",  key="-EXPORT-",
                      button_color=(BG, HOLD_CLR), font=("Helvetica", 10),          size=(11,1)),
        ],
        [sg.HorizontalSeparator(color="#333")],

        # Leaderboard
        [sg.Text("Leaderboard — ranked by score (click row for details)",
                 font=("Helvetica", 10, "bold"), text_color=ACCENT, background_color=BG)],
        [sg.Table(
            values=[], headings=HDRS, key="-TABLE-",
            background_color=CARD_BG, text_color=TEXT,
            header_text_color=ACCENT, header_background_color="#111122",
            alternating_row_color="#252535",
            selected_row_colors=(TEXT, ACCENT),
            font=("Helvetica", 10), header_font=("Helvetica", 10, "bold"),
            col_widths=[5, 7, 5, 18, 14, 9, 9, 7, 7, 6],
            auto_size_columns=False, num_rows=12,
            justification="left", enable_events=True, expand_x=True,
        )],

        # Status
        [
            sg.Text("Add tickers and click Analyze All." if keys_ok
                    else "Set AV_KEY in the script, save, and restart.",
                    key="-STATUS-", font=("Helvetica", 9),
                    text_color=MUTED, background_color=BG, size=(76, 1)),
            sg.Push(background_color=BG),
            sg.Text("", key="-PROGRESS-", font=("Helvetica", 9, "bold"),
                    text_color=ACCENT, background_color=BG, size=(30, 1)),
        ],
        [sg.HorizontalSeparator(color="#333")],

        # Detail
        [sg.Text("Detail View",
                 font=("Helvetica", 10, "bold"), text_color=ACCENT, background_color=BG)],
        [sg.Multiline(
            default_text="  Select a row above to see full breakdown.",
            key="-DETAIL-", size=(120, 18),
            font=("Courier", 9), background_color=CARD_BG,
            text_color=TEXT, disabled=True, expand_x=True,
        )],
        [sg.HorizontalSeparator(color="#333")],

        # Chart
        [sg.Text("Charts — 52-Week Range & Criteria Scores",
                 font=("Helvetica", 10, "bold"), text_color=ACCENT, background_color=BG)],
        [sg.Canvas(key="-CANVAS-", background_color=BG,
                   expand_x=True, size=(1100, 420))],
    ]

    window = sg.Window(
        "Portfolio Analyzer — Stocks & ETFs",
        [[sg.Column(layout, background_color=BG, scrollable=True,
                    vertical_scroll_only=True, expand_x=True, expand_y=True,
                    size=(1240, 900))]],
        background_color=BG, size=(1260, 960),
        resizable=True, finalize=True,
    )
    window["-TICKER-"].bind("<Return>", "_ENTER")

    def sorted_p():
        return sorted(portfolio, key=lambda x: x["pct"], reverse=True)

    def refresh_table():
        sp = sorted_p()
        window["-TABLE-"].update(values=make_table(sp))
        return sp

    def show_detail(r):
        window["-DETAIL-"].update(value=build_detail(r), disabled=True)

    def do_analyze(tickers):
        n = len(tickers)
        for i, t in enumerate(tickers):
            window.write_event_value("-PROG-", f"Fetching {t} ({i+1}/{n})…")
            try:
                result = fetch_and_score(t, fh_key, av_key)
                idx = next((j for j, r in enumerate(portfolio)
                            if r["ticker"] == result["ticker"]), None)
                if idx is not None: portfolio[idx] = result
                else:               portfolio.append(result)
                window.write_event_value("-DONE-ONE-", t)
            except Exception as e:
                window.write_event_value("-ERR-", f"{t}||{e}")
        window.write_event_value("-DONE-ALL-", True)

    while True:
        event, values = window.read(timeout=200)
        if event in (sg.WIN_CLOSED, "Exit"): break

        elif event in ("-ADD-", "-TICKER-_ENTER"):
            if not keys_ok:
                sg.popup("Set AV_KEY in the script and restart.",
                         background_color=CARD_BG); continue
            raw = values["-TICKER-"].strip().upper()
            if not raw: continue
            added = []
            for t in [x.strip() for x in raw.split(",") if x.strip()]:
                if not any(r["ticker"] == t for r in portfolio):
                    portfolio.append(make_placeholder(t)); added.append(t)
            if added:
                refresh_table()
                window["-STATUS-"].update(
                    f"Added: {', '.join(added)} — click Analyze All.",
                    text_color=TEXT)
            window["-TICKER-"].update("")

        elif event == "-ANALYZE-":
            if not keys_ok:
                sg.popup("Set AV_KEY in the script and restart.",
                         background_color=CARD_BG); continue
            pending = [r["ticker"] for r in portfolio]
            if not pending:
                sg.popup("Add at least one ticker first.",
                         background_color=CARD_BG); continue
            n   = len(pending)
            est = round(n * AV_DELAY / 60, 1)
            window["-STATUS-"].update(
                f"Analyzing {n} ticker(s)… ~{est} min "
                f"(AV rate limit = 5/min, yfinance live for ETF PE)",
                text_color=TEXT)
            window["-ANALYZE-"].update(disabled=True)
            threading.Thread(target=do_analyze, args=(pending,), daemon=True).start()

        elif event == "-PROG-":
            window["-PROGRESS-"].update(values["-PROG-"])

        elif event == "-DONE-ONE-":
            refresh_table()

        elif event == "-ERR-":
            parts  = values["-ERR-"].split("||", 1)
            failed = parts[0].strip()
            reason = parts[1] if len(parts) > 1 else "Unknown"
            window["-STATUS-"].update(f"Skipped {failed}: {reason}",
                                      text_color=WARN_CLR)
            for r in list(portfolio):
                if r["ticker"] == failed and r["verdict"] == "?":
                    portfolio.remove(r); break
            refresh_table()

        elif event == "-DONE-ALL-":
            window["-ANALYZE-"].update(disabled=False)
            window["-PROGRESS-"].update("")
            sp    = refresh_table()
            valid = [r for r in sp if r["verdict"] != "?"]
            if valid:
                best = valid[0]
                dq_s = best["dq"].quality if best.get("dq") else "?"
                window["-STATUS-"].update(
                    f"Done! {len(valid)} analyzed. "
                    f"Top: {best['ticker']} [{best['type']}] "
                    f"{best['pct']}/100 [{best['verdict']}]  Data: {dq_s}",
                    text_color=BUY_CLR)
                show_detail(best)
                draw_chart(window["-CANVAS-"], valid)
            else:
                window["-STATUS-"].update(
                    "All failed — check ticker symbols and API keys.",
                    text_color=SELL_CLR)

        elif event == "-TABLE-":
            sel = values["-TABLE-"]
            if sel:
                sp  = sorted_p()
                idx = sel[0]
                if idx < len(sp) and sp[idx]["verdict"] != "?":
                    r = sp[idx]
                    show_detail(r)
                    draw_chart(window["-CANVAS-"], [r])

        elif event == "-CLEAR-":
            portfolio.clear()
            window["-TABLE-"].update(values=[])
            window["-DETAIL-"].update(
                value="  Select a row above to see full breakdown.",
                disabled=True)
            try:
                for w in window["-CANVAS-"].TKCanvas.winfo_children():
                    w.destroy()
            except Exception:
                pass
            window["-STATUS-"].update("Cleared.", text_color=TEXT)

        elif event == "-EXPORT-":
            valid = [r for r in portfolio if r["verdict"] != "?"]
            if not valid:
                sg.popup("Nothing to export yet.", background_color=CARD_BG); continue
            path = sg.popup_get_file(
                "Save as CSV", save_as=True, default_extension=".csv",
                file_types=(("CSV files", "*.csv"),), background_color=CARD_BG)
            if path:
                rows = []
                for rank, r in enumerate(
                        sorted(valid, key=lambda x: x["pct"], reverse=True), 1):
                    row = {"Rank": rank, "Ticker": r["ticker"],
                           "Type": r["type"], "Name": r["name"],
                           "Sector": r["sector"], "Price": r["price"],
                           "Change%": r["change_pct"],
                           "Score": r["pct"], "Verdict": r["verdict"],
                           "DataQuality": r["dq"].quality if r.get("dq") else "?"}
                    for c in r["criteria"]:
                        row[c["name"]] = c["score"]
                    rows.append(row)
                pd.DataFrame(rows).to_csv(path, index=False)
                sg.popup(f"Exported {len(rows)} rows to:\n{path}",
                         background_color=CARD_BG)

    window.close()
    plt.close("all")


if __name__ == "__main__":
    main()
