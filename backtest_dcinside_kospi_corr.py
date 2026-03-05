import os
import re
import csv
import time
import random
from dataclasses import dataclass
from datetime import datetime, date, time as dtime
from urllib.parse import urlparse, parse_qs
from zoneinfo import ZoneInfo

import requests
import pandas as pd
from bs4 import BeautifulSoup

# FinBert (선택)
try:
    from transformers import pipeline
except Exception:
    pipeline = None

# =========================
# 환경설정 (필요 시 env로 조절)
# =========================
KST = ZoneInfo("Asia/Seoul")

GALLERY_ID = os.getenv("DC_GALLERY_ID", "krstock")
LIST_URL = os.getenv("DC_LIST_URL", "https://gall.dcinside.com/mgallery/board/lists/")
VIEW_BASE = "https://gall.dcinside.com"

EVENT_CSV = os.getenv("EVENT_CSV", "kospi_3pct_days.csv")  # 이벤트 날짜 리스트
EVENT_DATE_COL = os.getenv("EVENT_DATE_COL", "date")       # 기본: date
ONLY_DATE = os.getenv("ONLY_DATE", "").strip()             # 예: "2026-03-05" (이것만 실행)

# 분석 시간창 (KST)
START_TIME = dtime(8, 50)
END_TIME = dtime(15, 40)

# 요청 제어
DELAY_SEC = float(os.getenv("DELAY_SEC", "0.8"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "5"))

# 디버그/제한
DEBUG = os.getenv("DEBUG", "0") == "1"
MAX_DATES = int(os.getenv("MAX_DATES", "0"))  # 0이면 제한 없음. 예: 10이면 10개 날짜만

# FinBert
USE_FINBERT = os.getenv("USE_FINBERT", "1") == "1"

OUT_FG = os.getenv("OUT_FG", "backtest_fear_greed.csv")
OUT_MERGED = os.getenv("OUT_MERGED", "backtest_merged.csv")
OUT_CORR = os.getenv("OUT_CORR", "backtest_corr.csv")


# =========================
# 키워드(정규표현식)
# =========================
GREED_KEYWORDS = [
    "가즈아", "떡상", "영차", "불기둥", "상한가", "상다", "풀매수", "탑승", "줍줍", "익절",
    "달달", "맛있", "신고가", "불장", "수익", "돌파", "반등", "🚀", "우상향",
]
FEAR_KEYWORDS = [
    "돔황챠", "도망", "떡락", "한강", "구조대", "손절", "물림", "물렸", "나락", "하한가",
    "파네", "떨어지는 칼날", "끝났다", "개박살", "지옥", "멸망", "파멸", "파란불",
    "반대매매", "망함", "📉", "ㅈ됨", "좆됨",
]

def _kwfrag(k: str) -> str:
    k = k.strip()
    if " " in k:
        return r"\s*".join(re.escape(p) for p in k.split())
    return re.escape(k)

GREED_PATTERN = re.compile("|".join(_kwfrag(k) for k in GREED_KEYWORDS))
FEAR_PATTERN  = re.compile("|".join(_kwfrag(k) for k in FEAR_KEYWORDS))
ANY_PATTERN   = re.compile("|".join(_kwfrag(k) for k in (GREED_KEYWORDS + FEAR_KEYWORDS)))


def clean_title(t: str) -> str:
    t = (t or "").strip()
    t = re.sub(r"\[\s*\d+\s*\]", "", t)      # 댓글수 [3] 제거
    t = re.sub(r"\s+", " ", t).strip()
    return t

def looks_like_stock_name_only(title: str) -> bool:
    t = clean_title(title)
    if ANY_PATTERN.search(t):
        return False
    t_no_space = t.replace(" ", "")
    if (" " not in t) and (1 <= len(t_no_space) <= 10) and re.fullmatch(r"[0-9A-Za-z가-힣]+", t_no_space):
        return True
    return False


# =========================
# HTTP
# =========================
def make_headers() -> dict:
    return {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "Connection": "keep-alive",
        "Referer": "https://gall.dcinside.com/",
    }

def fetch_with_retry(session: requests.Session, url: str, params: dict | None = None) -> requests.Response:
    headers = make_headers()
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = session.get(url, params=params, headers=headers, timeout=20, allow_redirects=True)
            if r.status_code == 429:
                wait = min(60, (2 ** attempt) + random.random())
                print(f"[WARN] 429 Too Many Requests. retry {attempt}/{MAX_RETRIES}, sleep {wait:.1f}s")
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r
        except requests.RequestException as e:
            wait = min(60, (2 ** attempt) + random.random())
            print(f"[WARN] request error: {e}. retry {attempt}/{MAX_RETRIES}, sleep {wait:.1f}s")
            time.sleep(wait)
    raise RuntimeError(f"Failed to fetch after retries: {url}")

def fetch_html(session: requests.Session, url: str, params: dict | None = None) -> str:
    r = fetch_with_retry(session, url, params=params)
    return r.text


# =========================
# DCInside 파싱 (리스트/뷰)
# =========================
@dataclass
class ListPost:
    no: int
    title: str
    href: str
    list_date: date  # 리스트에서 추정한 날짜(날짜 필터용)

def parse_list_date(date_text: str, now_kst: datetime) -> date | None:
    """
    리스트 작성일 표기:
    - 오늘: "HH:MM"
    - 최근: "MM.DD"
    - 이전: "YY.MM.DD" 또는 "YYYY.MM.DD"
    """
    s = (date_text or "").strip()
    s = re.sub(r"\s+", "", s)

    # HH:MM => 오늘
    if re.fullmatch(r"\d{1,2}:\d{2}", s):
        return now_kst.date()

    # YYYY.MM.DD
    m = re.fullmatch(r"(\d{4})[.\-](\d{2})[.\-](\d{2})", s)
    if m:
        return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))

    # YY.MM.DD
    m = re.fullmatch(r"(\d{2})[.\-](\d{2})[.\-](\d{2})", s)
    if m:
        return date(2000 + int(m.group(1)), int(m.group(2)), int(m.group(3)))

    # MM.DD
    m = re.fullmatch(r"(\d{2})[.\-](\d{2})", s)
    if m:
        mm = int(m.group(1))
        dd = int(m.group(2))
        cand = date(now_kst.year, mm, dd)
        # cand가 미래면 전년도라고 추정
        if cand > now_kst.date():
            cand = date(now_kst.year - 1, mm, dd)
        return cand

    return None

def extract_list_posts(html: str, now_kst: datetime) -> list[ListPost]:
    soup = BeautifulSoup(html, "lxml")
    posts: list[ListPost] = []

    for tr in soup.find_all("tr"):
        num_td = tr.find("td", class_=re.compile(r"gall_num"))
        if not num_td:
            continue
        num_text = num_td.get_text(" ", strip=True).strip()
        if not num_text.isdigit():
            continue

        no = int(num_text)

        tit_td = tr.find("td", class_=re.compile(r"gall_tit"))
        if not tit_td:
            continue

        # view 링크 찾기
        a = tit_td.find("a", href=re.compile(r"/mgallery/board/view/\?"))
        if not a:
            a = tit_td.find("a", href=re.compile(r"/board/view/\?"))
        if not a:
            continue

        title = clean_title(a.get_text(" ", strip=True))
        href = a.get("href", "").strip()
        if not href:
            continue
        if href.startswith("/"):
            href = VIEW_BASE + href

        date_td = tr.find("td", class_=re.compile(r"gall_date"))
        date_text = date_td.get_text(" ", strip=True) if date_td else ""
        d = parse_list_date(date_text, now_kst)
        if d is None:
            continue

        posts.append(ListPost(no=no, title=title, href=href, list_date=d))

    # 중복 제거
    uniq = {}
    for p in posts:
        uniq[p.no] = p
    return list(uniq.values())

def parse_view_datetime(html: str) -> datetime | None:
    """
    글 본문 페이지에서 실제 작성시각(초 단위)을 정규식으로 뽑음.
    예: "2026.03.05 14:08:38" :contentReference[oaicite:3]{index=3} 같은 형식이 실제로 노출됨.
    """
    text = re.sub(r"\s+", " ", html)
    m = re.search(r"(\d{4}\.\d{2}\.\d{2})\s+(\d{2}:\d{2}:\d{2})", text)
    if not m:
        return None
    dt = datetime.strptime(m.group(1) + " " + m.group(2), "%Y.%m.%d %H:%M:%S")
    return dt.replace(tzinfo=KST)


# =========================
# 페이지 범위/이진탐색
# =========================
def get_last_page(session: requests.Session, now_kst: datetime) -> int:
    """
    1) page=1에서 '끝' 링크 href를 찾아 요청 → 최종 redirect URL의 page=를 읽음
    2) 실패하면 텍스트에서 '페이지 N 이동'으로 추정
    """
    html = fetch_html(session, LIST_URL, params={"id": GALLERY_ID, "page": 1})
    soup = BeautifulSoup(html, "lxml")

    # 1) '끝' 링크
    end_link = None
    for a in soup.find_all("a"):
        if a.get_text(" ", strip=True) == "끝":
            href = a.get("href", "")
            if href:
                end_link = href
                break

    if end_link:
        if end_link.startswith("/"):
            end_link = VIEW_BASE + end_link
        r = fetch_with_retry(session, end_link, params=None)
        qs = parse_qs(urlparse(r.url).query)
        if "page" in qs:
            try:
                return int(qs["page"][0])
            except Exception:
                pass

    # 2) '페이지 N 이동'에서 N 추출
    txt = soup.get_text(" ", strip=True)
    m = re.search(r"페이지\s+(\d+)\s+이동", txt)
    if m:
        return int(m.group(1))

    # fallback
    return 1

def get_page_date_range(session: requests.Session, page: int, now_kst: datetime, cache: dict[int, tuple[date, date, list[ListPost]]]):
    """
    cache[page] = (newest_date, oldest_date, posts)
    """
    if page in cache:
        return cache[page]

    html = fetch_html(session, LIST_URL, params={"id": GALLERY_ID, "page": page})
    posts = extract_list_posts(html, now_kst)
    if not posts:
        cache[page] = (date.min, date.min, [])
        return cache[page]

    dates = [p.list_date for p in posts]
    newest = max(dates)
    oldest = min(dates)
    cache[page] = (newest, oldest, posts)
    return cache[page]

def page_contains_target(newest: date, oldest: date, target: date) -> bool:
    return (oldest <= target <= newest)

def find_any_page_for_date(session: requests.Session, target: date, now_kst: datetime, last_page: int,
                           cache: dict[int, tuple[date, date, list[ListPost]]]) -> int | None:
    """
    페이지 번호가 증가할수록 과거로 감(older).
    binary search로 target을 포함하는 임의의 page 하나를 찾음.
    """
    lo, hi = 1, last_page
    found = None

    while lo <= hi:
        mid = (lo + hi) // 2
        newest, oldest, _ = get_page_date_range(session, mid, now_kst, cache)

        if DEBUG:
            print(f"[DEBUG] page={mid} range {oldest} ~ {newest}")

        if target > newest:
            # target이 더 최신 => 더 앞쪽(작은 page)로
            hi = mid - 1
        elif target < oldest:
            # target이 더 과거 => 더 뒤쪽(큰 page)로
            lo = mid + 1
        else:
            found = mid
            break

        time.sleep(DELAY_SEC)

    return found

def collect_posts_for_date(session: requests.Session, target: date, now_kst: datetime, last_page: int,
                           cache: dict[int, tuple[date, date, list[ListPost]]]) -> list[ListPost]:
    mid = find_any_page_for_date(session, target, now_kst, last_page, cache)
    if mid is None:
        return []

    pages = set([mid])

    # 왼쪽(더 최신 페이지) 확장
    p = mid - 1
    while p >= 1:
        newest, oldest, _ = get_page_date_range(session, p, now_kst, cache)
        if page_contains_target(newest, oldest, target):
            pages.add(p)
            p -= 1
            time.sleep(DELAY_SEC)
        else:
            break

    # 오른쪽(더 과거 페이지) 확장
    p = mid + 1
    while p <= last_page:
        newest, oldest, _ = get_page_date_range(session, p, now_kst, cache)
        if page_contains_target(newest, oldest, target):
            pages.add(p)
            p += 1
            time.sleep(DELAY_SEC)
        else:
            break

    all_posts: dict[int, ListPost] = {}
    for p in sorted(pages):
        _, _, posts = get_page_date_range(session, p, now_kst, cache)
        for post in posts:
            if post.list_date == target:
                all_posts[post.no] = post

    return list(all_posts.values())


# =========================
# FinBert
# =========================
def load_finbert_pipe():
    if pipeline is None:
        return None
    return pipeline(
        task="text-classification",
        model="snunlp/KR-FinBert-SC",
        tokenizer="snunlp/KR-FinBert-SC",
        device=-1,
    )

def classify_titles_finbert(finbert_pipe, titles: list[str]) -> tuple[int, int]:
    """
    positive -> greed
    negative -> fear
    neutral  -> 제외
    """
    if not titles:
        return 0, 0

    if finbert_pipe is None:
        # fallback: 키워드로만 카운트
        greed = 0
        fear = 0
        for t in titles:
            g = bool(GREED_PATTERN.search(t))
            f = bool(FEAR_PATTERN.search(t))
            if g and not f:
                greed += 1
            elif f and not g:
                fear += 1
        return greed, fear

    outs = finbert_pipe(titles, truncation=True)
    greed = 0
    fear = 0
    for out in outs:
        label = (out.get("label") or "").lower()
        if label == "positive":
            greed += 1
        elif label == "negative":
            fear += 1
    return greed, fear


# =========================
# KOSPI next-day return
# =========================
def load_kospi_prices(start: str, end: str) -> pd.DataFrame:
    import yfinance as yf
    df = yf.download("^KS11", start=start, end=end, interval="1d", auto_adjust=False,
                     progress=False, threads=False)
    if df is None or len(df) == 0:
        raise RuntimeError("yfinance returned empty dataframe for ^KS11")
    df = df.copy()
    df.index = pd.to_datetime(df.index)
    df.sort_index(inplace=True)
    return df


# =========================
# GitHub Summary
# =========================
def write_summary(s: str):
    p = os.getenv("GITHUB_STEP_SUMMARY")
    if not p:
        return
    with open(p, "a", encoding="utf-8") as f:
        f.write(s + "\n")


# =========================
# Main
# =========================
def main():
    now_kst = datetime.now(tz=KST)

    # 이벤트 날짜 읽기
    if ONLY_DATE:
        target_dates = [datetime.strptime(ONLY_DATE, "%Y-%m-%d").date()]
        print(f"[INFO] ONLY_DATE mode: {target_dates[0]}")
    else:
        ev = pd.read_csv(EVENT_CSV)
        if EVENT_DATE_COL not in ev.columns:
            raise RuntimeError(f"EVENT_CSV has no column '{EVENT_DATE_COL}'")
        target_dates = sorted({datetime.strptime(d, "%Y-%m-%d").date() for d in ev[EVENT_DATE_COL].astype(str)})

    if MAX_DATES > 0:
        target_dates = target_dates[:MAX_DATES]
        print(f"[INFO] MAX_DATES applied => {len(target_dates)} dates")

    # 갤러리 개설일 자동 추정(있으면 2025-05-19처럼 표기됨) :contentReference[oaicite:4]{index=4}
    session = requests.Session()
    first_html = fetch_html(session, LIST_URL, params={"id": GALLERY_ID, "page": 1})
    created = None
    m = re.search(r"개설일\s*(\d{4})-(\d{2})-(\d{2})", first_html)
    if m:
        created = date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        print(f"[INFO] Gallery created date detected: {created}")

    if created:
        before = len(target_dates)
        target_dates = [d for d in target_dates if d >= created]
        skipped = before - len(target_dates)
        if skipped > 0:
            print(f"[INFO] Skipped {skipped} dates before gallery creation ({created})")

    if not target_dates:
        print("[WARN] No target dates to run after filtering.")
        return

    # last page 구하기
    last_page = get_last_page(session, now_kst)
    print(f"[INFO] last_page={last_page}")

    # FinBert 로드
    finbert_pipe = load_finbert_pipe() if USE_FINBERT else None

    page_cache: dict[int, tuple[date, date, list[ListPost]]] = {}
    detail_cache: dict[int, datetime] = {}

    results = []
    write_summary("## DCInside Fear & Greed Backtest\n")

    for i, d in enumerate(target_dates, 1):
        print(f"\n[INFO] ({i}/{len(target_dates)}) Processing date={d}")
        try:
            posts = collect_posts_for_date(session, d, now_kst, last_page, page_cache)
        except Exception as e:
            print(f"[ERROR] Failed to collect list posts for {d}: {e}")
            results.append({
                "date": d.strftime("%Y-%m-%d"),
                "fear_count": None,
                "greed_count": None,
                "daily_greed_score": None,
                "keyword_candidates": None,
                "keyword_in_window": None,
                "error": str(e),
            })
            continue

        # 키워드 후보만 뽑기
        candidates = []
        for p in posts:
            if looks_like_stock_name_only(p.title):
                continue
            if not ANY_PATTERN.search(p.title):
                continue
            candidates.append(p)

        # 후보 중 실제 작성시각 확인(뷰 페이지에서)
        in_window_titles = []
        for p in candidates:
            dt = detail_cache.get(p.no)
            if dt is None:
                html = fetch_html(session, p.href, params=None)
                dt = parse_view_datetime(html)
                if dt is None:
                    continue
                detail_cache[p.no] = dt
                time.sleep(DELAY_SEC)

            if dt.date() != d:
                continue
            tt = dt.time()
            if START_TIME <= tt <= END_TIME:
                in_window_titles.append(p.title)

        greed, fear = classify_titles_finbert(finbert_pipe, in_window_titles)
        total = greed + fear
        score = 50.0 if total == 0 else (greed / total) * 100.0

        row = {
            "date": d.strftime("%Y-%m-%d"),
            "fear_count": int(fear),
            "greed_count": int(greed),
            "daily_greed_score": round(score, 4),
            "keyword_candidates": len(candidates),
            "keyword_in_window": len(in_window_titles),
            "error": "",
        }
        results.append(row)

        print(f"[RESULT] {row}")
        write_summary(
            f"- **{row['date']}** | fear={row['fear_count']} greed={row['greed_count']} "
            f"score={row['daily_greed_score']} | candidates={row['keyword_candidates']} window={row['keyword_in_window']}"
        )

    # 결과 저장
    fg_df = pd.DataFrame(results)
    fg_df.to_csv(OUT_FG, index=False, encoding="utf-8-sig")
    print(f"[INFO] saved {OUT_FG}")

    # KOSPI next-day return 병합 + 상관분석
    fg_ok = fg_df.dropna(subset=["daily_greed_score"]).copy()
    fg_ok["date"] = pd.to_datetime(fg_ok["date"])
    start = fg_ok["date"].min().strftime("%Y-%m-%d")
    end = (fg_ok["date"].max() + pd.Timedelta(days=10)).strftime("%Y-%m-%d")

    kospi = load_kospi_prices(start, end)
    kospi["next_trading_date"] = kospi.index.to_series().shift(-1)
    kospi["next_day_ret_pct"] = kospi["Close"].pct_change().shift(-1) * 100.0

    kospi2 = kospi.reset_index().rename(columns={"index": "date"})
    kospi2["date"] = pd.to_datetime(kospi2["Date"]) if "Date" in kospi2.columns else pd.to_datetime(kospi2["date"])
    if "Date" in kospi2.columns:
        kospi2 = kospi2.drop(columns=["Date"])

    merged = pd.merge(
        fg_df.assign(date=pd.to_datetime(fg_df["date"])),
        kospi2[["date", "next_trading_date", "next_day_ret_pct"]],
        on="date",
        how="left",
    )
    merged["next_trading_date"] = pd.to_datetime(merged["next_trading_date"]).dt.strftime("%Y-%m-%d")
    merged["next_day_ret_pct"] = pd.to_numeric(merged["next_day_ret_pct"], errors="coerce")
    merged["next_day_abs_ret_pct"] = merged["next_day_ret_pct"].abs()

    merged.to_csv(OUT_MERGED, index=False, encoding="utf-8-sig")
    print(f"[INFO] saved {OUT_MERGED}")

    sample = merged.dropna(subset=["daily_greed_score", "next_day_ret_pct"]).copy()
    n = len(sample)

    pearson_signed = sample["daily_greed_score"].corr(sample["next_day_ret_pct"], method="pearson") if n > 1 else None
    pearson_abs = sample["daily_greed_score"].corr(sample["next_day_abs_ret_pct"], method="pearson") if n > 1 else None

    spearman_signed = sample[["daily_greed_score", "next_day_ret_pct"]].corr(method="spearman").iloc[0, 1] if n > 1 else None
    spearman_abs = sample[["daily_greed_score", "next_day_abs_ret_pct"]].corr(method="spearman").iloc[0, 1] if n > 1 else None

    corr_df = pd.DataFrame([{
        "n_samples": n,
        "pearson_signed": pearson_signed,
        "pearson_abs": pearson_abs,
        "spearman_signed": spearman_signed,
        "spearman_abs": spearman_abs,
    }])
    corr_df.to_csv(OUT_CORR, index=False, encoding="utf-8-sig")
    print(f"[INFO] saved {OUT_CORR}")

    write_summary("\n## Correlation (Daily Greed Score vs Next Trading Day KOSPI Return)\n")
    write_summary(f"- samples (date matched): **{n}**")
    write_summary(f"- Pearson corr(score, next_day_ret): **{pearson_signed}**")
    write_summary(f"- Pearson corr(score, abs(next_day_ret)): **{pearson_abs}**")
    write_summary(f"- Spearman corr(score, next_day_ret): **{spearman_signed}**")
    write_summary(f"- Spearman corr(score, abs(next_day_ret)): **{spearman_abs}**")

    print("\n===== CORRELATION =====")
    print(corr_df.to_string(index=False))


if __name__ == "__main__":
    main()
