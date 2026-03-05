import os
import re
import csv
import time
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, time as dtime
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup

# (선택) KR-FinBert 사용
try:
    from transformers import pipeline
except Exception:
    pipeline = None


# =========================
# 설정(초보자는 여기만 필요할 때 수정)
# =========================
KST = ZoneInfo("Asia/Seoul")

# 에펨코리아 주식 게시판: 모바일 페이지가 HTML 파싱이 비교적 단순한 편이라 여기를 기본으로 사용
BASE_URL = os.getenv("FMKOREA_BASE_URL", "https://m.fmkorea.com/index.php")
MID = os.getenv("FMKOREA_MID", "stock")

# 장중 시간 (KST)
MARKET_OPEN = dtime(9, 0)
MARKET_CLOSE = dtime(15, 30)

# 크롤링 페이지 수(많을수록 더 과거까지 훑음 = 429 위험 증가)
MAX_PAGES = int(os.getenv("MAX_PAGES", "20"))

# 페이지 요청 사이 딜레이(초)
DELAY_SEC = float(os.getenv("DELAY_SEC", "1.0"))

# KR-FinBert 사용 여부 (기본: 사용)
USE_FINBERT = os.getenv("USE_FINBERT", "1") == "1"

# 디버그 로그 (기본: 꺼짐)
DEBUG = os.getenv("DEBUG", "0") == "1"

CSV_PATH = "index_result.csv"


# =========================
# 키워드(정규표현식 필터)
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


def _keyword_to_regex_fragment(keyword: str) -> str:
    """
    - 공백이 있는 키워드는 공백을 \\s*로 바꿔서 '떨어지는   칼날' 같은 변형도 잡아줌
    - 그 외는 re.escape로 안전하게 처리(이모지 포함)
    """
    kw = keyword.strip()
    if " " in kw:
        parts = [re.escape(p) for p in kw.split()]
        return r"\s*".join(parts)
    return re.escape(kw)


GREED_PATTERN = re.compile("|".join(_keyword_to_regex_fragment(k) for k in GREED_KEYWORDS))
FEAR_PATTERN = re.compile("|".join(_keyword_to_regex_fragment(k) for k in FEAR_KEYWORDS))
ANY_PATTERN = re.compile(
    "|".join(_keyword_to_regex_fragment(k) for k in (GREED_KEYWORDS + FEAR_KEYWORDS))
)


# =========================
# HTTP / 파싱 유틸
# =========================
def make_headers() -> dict:
    # 너무 “봇”같은 UA는 피하고, 일반 브라우저처럼 보이게만(우회 목적 X, 기본 예의 수준)
    return {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "Connection": "keep-alive",
        "Referer": "https://www.fmkorea.com/",
    }


def fetch_html_with_retry(session: requests.Session, url: str, params: dict, max_retries: int = 5) -> str:
    """
    429(Too Many Requests) / 네트워크 오류 대비:
    - 지수 백오프 + 약간의 랜덤 지터
    """
    headers = make_headers()
    for attempt in range(1, max_retries + 1):
        try:
            resp = session.get(url, params=params, headers=headers, timeout=20)
            if resp.status_code == 429:
                wait = min(60, (2 ** attempt) + random.random())
                if DEBUG:
                    print(f"[WARN] 429 Too Many Requests. retry {attempt}/{max_retries}, sleep {wait:.1f}s")
                time.sleep(wait)
                continue

            resp.raise_for_status()
            return resp.text

        except requests.RequestException as e:
            wait = min(60, (2 ** attempt) + random.random())
            if DEBUG:
                print(f"[WARN] request error: {e}. retry {attempt}/{max_retries}, sleep {wait:.1f}s")
            time.sleep(wait)

    raise RuntimeError("Failed to fetch page after retries (maybe blocked / 429).")


@dataclass
class Post:
    title: str
    created_at: datetime  # KST


def parse_regdate_to_datetime(text: str, now_kst: datetime) -> datetime | None:
    """
    regdate 텍스트가 다음 중 무엇이든 최대한 해석:
    - "HH:MM"  -> 오늘 시각
    - "YYYY.MM.DD" / "YYYY-MM-DD"
    - "MM.DD"
    - "방금", "N분 전", "N시간 전" (공백 변형 포함)
    """
    raw = (text or "").strip()
    if not raw:
        return None

    compact = re.sub(r"\s+", "", raw)  # 공백 제거

    if compact in ("방금", "justnow", "now"):
        return now_kst

    m = re.match(r"^(\d{1,2}):(\d{2})$", compact)
    if m:
        hh = int(m.group(1))
        mm = int(m.group(2))
        return datetime(now_kst.year, now_kst.month, now_kst.day, hh, mm, tzinfo=KST)

    m = re.match(r"^(\d{4})[.\-](\d{1,2})[.\-](\d{1,2})$", compact)
    if m:
        yyyy = int(m.group(1))
        mo = int(m.group(2))
        dd = int(m.group(3))
        return datetime(yyyy, mo, dd, 0, 0, tzinfo=KST)

    m = re.match(r"^(\d{1,2})\.(\d{1,2})$", compact)
    if m:
        mo = int(m.group(1))
        dd = int(m.group(2))
        return datetime(now_kst.year, mo, dd, 0, 0, tzinfo=KST)

    # "10분전" / "10분 전"
    m = re.match(r"^(\d+)(분)전$", compact)
    if m:
        mins = int(m.group(1))
        return now_kst - timedelta(minutes=mins)

    # "2시간전"
    m = re.match(r"^(\d+)(시간)전$", compact)
    if m:
        hours = int(m.group(1))
        return now_kst - timedelta(hours=hours)

    # 영어 형태(혹시 대비): "23hoursago", "15minago"
    m = re.match(r"^(\d+)(min|mins|minute|minutes)ago$", compact, re.IGNORECASE)
    if m:
        mins = int(m.group(1))
        return now_kst - timedelta(minutes=mins)

    m = re.match(r"^(\d+)(hour|hours)ago$", compact, re.IGNORECASE)
    if m:
        hours = int(m.group(1))
        return now_kst - timedelta(hours=hours)

    return None


def clean_title(title: str) -> str:
    t = (title or "").strip()
    # 제목 끝/중간에 달리는 댓글 수 같은 "[12]" 제거
    t = re.sub(r"\[\s*\d+\s*\]", "", t)
    # 탭/줄바꿈 정리
    t = re.sub(r"\s+", " ", t).strip()
    return t


def looks_like_stock_name_only(title: str) -> bool:
    """
    예외 처리(요청사항):
    - 키워드가 없고, 제목이 '삼성전자'처럼 종목명 단독에 가까우면 분석 제외
    (정확히 종목명 DB를 만들기 어렵기 때문에 '짧은 단어 1개' 휴리스틱)
    """
    t = clean_title(title)
    t_no_space = t.replace(" ", "")
    # 키워드가 있으면 stock-name-only가 아님
    if ANY_PATTERN.search(t):
        return False
    # 한 단어(공백 없음) + 너무 길지 않음 + 한글/영문/숫자만
    if (" " not in t) and (1 <= len(t_no_space) <= 10) and re.fullmatch(r"[0-9A-Za-z가-힣]+", t_no_space):
        return True
    return False


def extract_posts_from_html(html: str, now_kst: datetime) -> list[Post]:
    soup = BeautifulSoup(html, "lxml")

    posts: list[Post] = []

    # 1) 모바일 페이지에서 흔히 보이는 패턴:
    # - li 안에 document_srl 링크
    # - 작성 시각 span.regdate
    for li in soup.find_all("li"):
        a = li.find("a", href=re.compile(r"document_srl=\d+"))
        reg = li.select_one(".regdate")
        if not a or not reg:
            continue

        href = a.get("href", "")
        # mid가 다르면 스킵(광고/다른 링크 등)
        if ("mid=" in href) and (f"mid={MID}" not in href):
            continue

        title = clean_title(a.get_text(" ", strip=True))
        reg_text = reg.get_text(" ", strip=True)

        dt = parse_regdate_to_datetime(reg_text, now_kst)
        if dt is None:
            continue

        posts.append(Post(title=title, created_at=dt))

    # 중복 제거(같은 title+time이 여러 번 잡힐 수 있어 방어)
    uniq = {}
    for p in posts:
        key = (p.title, p.created_at.isoformat())
        uniq[key] = p
    return list(uniq.values())


# =========================
# 분류 로직 (키워드 필터 + KR-FinBert)
# =========================
def load_finbert_pipe():
    """
    KR-FinBert-SC는 3분류(negative/neutral/positive) 모델.
    config의 id2label이 negative/neutral/positive임. :contentReference[oaicite:2]{index=2}
    """
    if pipeline is None:
        return None
    return pipeline(
        task="text-classification",
        model="snunlp/KR-FinBert-SC",
        tokenizer="snunlp/KR-FinBert-SC",
        device=-1,  # CPU
    )


def classify_titles(posts: list[Post], finbert_pipe):
    """
    요청사항 충족:
    1) 정규표현식 키워드로 1차 필터 (키워드 없으면 제외)
    2) 남은 후보를 KR-FinBert로 최종 판별
       - positive -> greed
       - negative -> fear
       - neutral  -> 제외
    """
    candidates: list[Post] = []
    for p in posts:
        if looks_like_stock_name_only(p.title):
            continue
        if not ANY_PATTERN.search(p.title):
            continue
        candidates.append(p)

    greed = 0
    fear = 0

    # FinBert 없이도 동작하도록(로컬에서 torch 설치가 어려운 경우 대비)
    if finbert_pipe is None:
        for p in candidates:
            g = bool(GREED_PATTERN.search(p.title))
            f = bool(FEAR_PATTERN.search(p.title))
            if g and not f:
                greed += 1
            elif f and not g:
                fear += 1
            else:
                # 둘 다 걸리거나 애매하면 제외
                pass
        return greed, fear, candidates

    # FinBert 배치 추론
    texts = [p.title for p in candidates]
    # pipeline은 리스트 입력 가능
    outputs = finbert_pipe(texts, truncation=True)

    for p, out in zip(candidates, outputs):
        label = (out.get("label") or "").lower()
        if label == "positive":
            greed += 1
        elif label == "negative":
            fear += 1
        else:
            # neutral -> 제외
            pass

    return greed, fear, candidates


# =========================
# CSV 누적 저장 (날짜 upsert)
# =========================
def upsert_csv(date_str: str, fear_count: int, greed_count: int, greed_score: float, path: str = CSV_PATH):
    fieldnames = ["date", "fear_count", "greed_count", "daily_greed_score"]

    rows = []
    if os.path.exists(path):
        with open(path, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for r in reader:
                if r.get("date"):
                    rows.append(r)

    # 같은 날짜가 있으면 제거(중복 방지)
    rows = [r for r in rows if r.get("date") != date_str]

    rows.append({
        "date": date_str,
        "fear_count": str(fear_count),
        "greed_count": str(greed_count),
        "daily_greed_score": f"{greed_score:.2f}",
    })

    # 날짜순 정렬(문자열 YYYY-MM-DD는 정렬이 잘 됨)
    rows.sort(key=lambda r: r["date"])

    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_github_summary(date_str: str, fear: int, greed: int, score: float, total_candidates: int):
    summary_path = os.getenv("GITHUB_STEP_SUMMARY")
    if not summary_path:
        return
    with open(summary_path, "a", encoding="utf-8") as f:
        f.write(f"## FMKorea Fear & Greed Index ({date_str}, KST)\n\n")
        f.write(f"- 후보(키워드 포함) 제목 수: **{total_candidates}**\n")
        f.write(f"- 공포(Fear) 카운트: **{fear}**\n")
        f.write(f"- 탐욕(Greed) 카운트: **{greed}**\n")
        f.write(f"- Daily Greed Score: **{score:.2f}**\n\n")
        f.write("> 계산식: Greed / (Fear + Greed) * 100\n")


def main():
    now_kst = datetime.now(tz=KST)
    today_str = now_kst.strftime("%Y-%m-%d")

    session = requests.Session()

    collected_posts: list[Post] = []
    reached_before_open = False

    for page in range(1, MAX_PAGES + 1):
        params = {"mid": MID, "page": page}

        if DEBUG:
            print(f"[INFO] Fetch page={page} params={params}")

        html = fetch_html_with_retry(session, BASE_URL, params=params)
        posts = extract_posts_from_html(html, now_kst)

        if DEBUG:
            print(f"[INFO] Extracted posts={len(posts)} from page={page}")

        if not posts:
            # 파싱이 하나도 안 되면(HTML 구조 변경/차단 등) 더 해봐야 의미 없을 수 있어 중단
            if page == 1:
                raise RuntimeError("No posts parsed. HTML structure may have changed or access blocked.")
            break

        # 오늘 글(시간표시형)만 대상으로 삼기 위해, created_at이 today인지 확인
        today_posts = [p for p in posts if p.created_at.date() == now_kst.date()]
        if not today_posts:
            # 오늘 글이 이 페이지에 하나도 없으면, 뒤 페이지는 더 오래된 글일 가능성이 큼 → 중단
            break

        # 장중(09:00~15:30) 글만 수집
        for p in today_posts:
            t = p.created_at.time()
            if t < MARKET_OPEN:
                reached_before_open = True
            if MARKET_OPEN <= t <= MARKET_CLOSE:
                collected_posts.append(p)

        if reached_before_open:
            break

        time.sleep(DELAY_SEC)

    # FinBert 로드 (옵션)
    finbert_pipe = None
    if USE_FINBERT:
        finbert_pipe = load_finbert_pipe()

    greed_count, fear_count, candidates = classify_titles(collected_posts, finbert_pipe)

    total = greed_count + fear_count
    if total == 0:
        # 신호가 없으면 중립 50으로 처리(0으로 나누기 방지)
        greed_score = 50.0
    else:
        greed_score = (greed_count / total) * 100.0

    upsert_csv(today_str, fear_count, greed_count, greed_score, CSV_PATH)

    print("==========================================")
    print(f"Date(KST): {today_str}")
    print(f"Collected titles (market hours): {len(collected_posts)}")
    print(f"Candidates (keyword matched): {len(candidates)}")
    print(f"Fear count:  {fear_count}")
    print(f"Greed count: {greed_count}")
    print(f"Daily Greed Score: {greed_score:.2f}")
    print(f"Saved/Updated: {CSV_PATH}")
    print("==========================================")

    write_github_summary(today_str, fear_count, greed_count, greed_score, len(candidates))


if __name__ == "__main__":
    main()
