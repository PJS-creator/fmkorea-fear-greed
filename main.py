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
# 기본 설정
# =========================
KST = ZoneInfo("Asia/Seoul")

# ✅ 디시인사이드 한국 주식 마이너 갤러리 리스트 URL(안정적)
LIST_URL = os.getenv(
    "DC_LIST_URL",
    "https://gall.dcinside.com/mgallery/board/lists/"
)
GALLERY_ID = os.getenv("DC_GALLERY_ID", "krstock")

# ✅ 수집 시간 (KST) : 08:50 ~ 15:40
START_TIME = dtime(8, 50)
END_TIME = dtime(15, 40)

# 크롤링 페이지 수 (많을수록 요청 증가)
MAX_PAGES = int(os.getenv("MAX_PAGES", "10"))
DELAY_SEC = float(os.getenv("DELAY_SEC", "1.0"))

# FinBert 사용 여부
USE_FINBERT = os.getenv("USE_FINBERT", "1") == "1"

# 디버그
DEBUG = os.getenv("DEBUG", "0") == "1"

CSV_PATH = "index_result.csv"


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


def _keyword_to_regex_fragment(keyword: str) -> str:
    kw = keyword.strip()
    if " " in kw:
        parts = [re.escape(p) for p in kw.split()]
        return r"\s*".join(parts)
    return re.escape(kw)


GREED_PATTERN = re.compile("|".join(_keyword_to_regex_fragment(k) for k in GREED_KEYWORDS))
FEAR_PATTERN = re.compile("|".join(_keyword_to_regex_fragment(k) for k in FEAR_KEYWORDS))
ANY_PATTERN = re.compile("|".join(_keyword_to_regex_fragment(k) for k in (GREED_KEYWORDS + FEAR_KEYWORDS)))


# =========================
# HTTP 유틸
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


def fetch_html_with_retry(session: requests.Session, url: str, params: dict, max_retries: int = 5) -> str:
    headers = make_headers()

    for attempt in range(1, max_retries + 1):
        try:
            resp = session.get(url, params=params, headers=headers, timeout=20)

            if resp.status_code == 429:
                wait = min(60, (2 ** attempt) + random.random())
                print(f"[WARN] 429 Too Many Requests. retry {attempt}/{max_retries}, sleep {wait:.1f}s")
                time.sleep(wait)
                continue

            if resp.status_code >= 400:
                # 디버그용: 어떤 상태코드로 막히는지 보이게
                print(f"[WARN] HTTP {resp.status_code} for {resp.url}")
                if DEBUG:
                    print(resp.text[:300])
                resp.raise_for_status()

            return resp.text

        except requests.RequestException as e:
            wait = min(60, (2 ** attempt) + random.random())
            print(f"[WARN] request error: {e}. retry {attempt}/{max_retries}, sleep {wait:.1f}s")
            time.sleep(wait)

    raise RuntimeError("Failed to fetch page after retries (blocked / network / 429).")


# =========================
# 파싱 유틸
# =========================
@dataclass
class Post:
    title: str
    created_at: datetime  # KST


def clean_title(title: str) -> str:
    t = (title or "").strip()
    # 댓글수 [12] 제거
    t = re.sub(r"\[\s*\d+\s*\]", "", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def looks_like_stock_name_only(title: str) -> bool:
    """
    요청사항: 키워드가 없고, '삼성전자' 같이 종목명만 있는 경우 제외
    (종목명 DB 없이 가능한 휴리스틱)
    """
    t = clean_title(title)
    if ANY_PATTERN.search(t):
        return False

    t_no_space = t.replace(" ", "")
    # 공백 없고 길이 짧고(<=10), 한글/영문/숫자만이면 '종목명만'으로 간주
    if (" " not in t) and (1 <= len(t_no_space) <= 10) and re.fullmatch(r"[0-9A-Za-z가-힣]+", t_no_space):
        return True
    return False


def parse_dcinside_regdate(text: str, now_kst: datetime) -> datetime | None:
    """
    디시 리스트의 작성일 표기(대표)
    - 오늘 글: "HH:MM"
    - 과거/공지: "YY.MM.DD" 또는 "YY/MM/DD" 또는 "YYYY.MM.DD"
    """
    raw = (text or "").strip()
    if not raw:
        return None

    raw = re.sub(r"\s+", "", raw)

    # HH:MM (오늘 글)
    m = re.match(r"^(\d{1,2}):(\d{2})$", raw)
    if m:
        hh = int(m.group(1))
        mm = int(m.group(2))
        return datetime(now_kst.year, now_kst.month, now_kst.day, hh, mm, tzinfo=KST)

    # YYYY.MM.DD
    m = re.match(r"^(\d{4})[.\-](\d{2})[.\-](\d{2})$", raw)
    if m:
        yyyy = int(m.group(1))
        mo = int(m.group(2))
        dd = int(m.group(3))
        return datetime(yyyy, mo, dd, 0, 0, tzinfo=KST)

    # YY.MM.DD or YY/MM/DD
    m = re.match(r"^(\d{2})[./\-](\d{2})[./\-](\d{2})$", raw)
    if m:
        yy = int(m.group(1))
        yyyy = 2000 + yy
        mo = int(m.group(2))
        dd = int(m.group(3))
        return datetime(yyyy, mo, dd, 0, 0, tzinfo=KST)

    return None


def extract_posts_from_html_dcinside(html: str, now_kst: datetime) -> list[Post]:
    """
    디시 리스트 페이지에서:
    - 일반 글(번호가 숫자인 행)만
    - 제목, 작성시간(HH:MM) 추출
    """
    soup = BeautifulSoup(html, "lxml")
    posts: list[Post] = []

    # 1) 가장 정석: tr 태그(리스트 행) 기준 파싱
    # - 디시 리스트는 보통 table / tr 로 구성됨
    for tr in soup.find_all("tr"):
        # 번호(공지/광고 제외)
        num_td = tr.find("td", class_=re.compile(r"gall_num"))
        if not num_td:
            continue
        num_text = num_td.get_text(" ", strip=True).strip()
        if not num_text.isdigit():
            continue  # 공지/설문/AD 등 제외

        # 제목
        title = None
        tit_td = tr.find("td", class_=re.compile(r"gall_tit"))
        if tit_td:
            # 제목 링크 후보들 중, "의미있는 텍스트"를 가진 가장 긴 것을 제목으로 선택
            candidates = []
            for a in tit_td.find_all("a"):
                txt = clean_title(a.get_text(" ", strip=True))
                if not txt:
                    continue
                # 댓글수 링크([1]) 같은 건 clean_title에서 빈 문자열이 되므로 자연히 제외됨
                candidates.append(txt)
            if candidates:
                title = max(candidates, key=len)

        if not title:
            continue

        # 작성일/시간
        date_text = ""
        date_td = tr.find("td", class_=re.compile(r"gall_date"))
        if date_td:
            date_text = date_td.get_text(" ", strip=True)
        else:
            # fallback: 행의 td들 중 날짜/시간 패턴을 가진 값을 찾음
            for td in tr.find_all("td"):
                v = td.get_text(" ", strip=True).strip()
                if re.fullmatch(r"\d{1,2}:\d{2}", v) or re.fullmatch(r"\d{2}[./-]\d{2}[./-]\d{2}", v) or re.fullmatch(r"\d{4}[./-]\d{2}[./-]\d{2}", v):
                    date_text = v
                    break

        dt = parse_dcinside_regdate(date_text, now_kst)
        if dt is None:
            continue

        posts.append(Post(title=title, created_at=dt))

    # 2) 중복 제거
    uniq = {}
    for p in posts:
        key = (p.title, p.created_at.isoformat())
        uniq[key] = p
    return list(uniq.values())


# =========================
# 분류 (키워드 필터 + KR-FinBert)
# =========================
def load_finbert_pipe():
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
    1) 키워드가 있는 제목만 후보로
    2) 후보를 KR-FinBert로 최종 판별:
       - positive -> Greed
       - negative -> Fear
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

    # FinBert이 없으면(설치 실패 등) 키워드로만 카운트
    if finbert_pipe is None:
        for p in candidates:
            g = bool(GREED_PATTERN.search(p.title))
            f = bool(FEAR_PATTERN.search(p.title))
            if g and not f:
                greed += 1
            elif f and not g:
                fear += 1
        return greed, fear, candidates

    # FinBert 배치 추론
    texts = [p.title for p in candidates]
    outputs = finbert_pipe(texts, truncation=True)

    for p, out in zip(candidates, outputs):
        label = (out.get("label") or "").lower()
        if label == "positive":
            greed += 1
        elif label == "negative":
            fear += 1
        else:
            pass  # neutral 제외

    return greed, fear, candidates


# =========================
# CSV 누적 저장
# =========================
def ensure_csv_exists(path: str = CSV_PATH):
    if os.path.exists(path):
        return
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["date", "fear_count", "greed_count", "daily_greed_score"],
        )
        writer.writeheader()


def upsert_csv(date_str: str, fear_count: int, greed_count: int, greed_score: float, path: str = CSV_PATH):
    fieldnames = ["date", "fear_count", "greed_count", "daily_greed_score"]

    rows = []
    if os.path.exists(path):
        with open(path, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for r in reader:
                if r.get("date"):
                    rows.append(r)

    # 같은 날짜 제거 후 추가(재실행 시 갱신)
    rows = [r for r in rows if r.get("date") != date_str]
    rows.append({
        "date": date_str,
        "fear_count": str(fear_count),
        "greed_count": str(greed_count),
        "daily_greed_score": f"{greed_score:.2f}",
    })
    rows.sort(key=lambda r: r["date"])

    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_github_summary(msg: str):
    summary_path = os.getenv("GITHUB_STEP_SUMMARY")
    if not summary_path:
        return
    with open(summary_path, "a", encoding="utf-8") as f:
        f.write(msg + "\n")


# =========================
# 메인
# =========================
def main():
    ensure_csv_exists(CSV_PATH)

    now_kst = datetime.now(tz=KST)
    today = now_kst.date()
    today_str = now_kst.strftime("%Y-%m-%d")

    session = requests.Session()

    collected_posts: list[Post] = []
    reached_before_start = False

    try:
        for page in range(1, MAX_PAGES + 1):
            params = {"id": GALLERY_ID, "page": page}

            if DEBUG:
                print(f"[INFO] Fetch page={page} params={params}")

            html = fetch_html_with_retry(session, LIST_URL, params=params)
            posts = extract_posts_from_html_dcinside(html, now_kst)

            if DEBUG:
                print(f"[INFO] Parsed posts={len(posts)} from page={page}")

            if not posts:
                # 파싱이 0이면 구조 변경/차단 가능성이 큼
                print("[WARN] No posts parsed. HTML structure may have changed or access blocked.")
                break

            # 오늘 글만
            today_posts = [p for p in posts if p.created_at.date() == today]
            if not today_posts:
                # 이 페이지부터는 오늘 글이 없는 것으로 보고 중단
                break

            # 08:50 ~ 15:40 범위만 수집
            for p in today_posts:
                t = p.created_at.time()

                if t < START_TIME:
                    reached_before_start = True

                if START_TIME <= t <= END_TIME:
                    collected_posts.append(p)

            if reached_before_start:
                break

            time.sleep(DELAY_SEC)

    except RuntimeError as e:
        # 크롤 실패해도 워크플로우를 아예 터뜨리진 않게(원하면 여기서 raise로 바꿔도 됨)
        print(f"[ERROR] Crawl failed: {e}")
        write_github_summary(f"## DCInside Fear & Greed\n\n- ❌ 크롤링 실패: `{e}`\n")
        return

    finbert_pipe = load_finbert_pipe() if USE_FINBERT else None
    greed_count, fear_count, candidates = classify_titles(collected_posts, finbert_pipe)

    total = greed_count + fear_count
    greed_score = 50.0 if total == 0 else (greed_count / total) * 100.0

    upsert_csv(today_str, fear_count, greed_count, greed_score, CSV_PATH)

    print("==========================================")
    print(f"Gallery: {GALLERY_ID}")
    print(f"Date(KST): {today_str}")
    print(f"Window(KST): {START_TIME.strftime('%H:%M')} ~ {END_TIME.strftime('%H:%M')}")
    print(f"Collected titles in window: {len(collected_posts)}")
    print(f"Candidates (keyword matched): {len(candidates)}")
    print(f"Fear count:  {fear_count}")
    print(f"Greed count: {greed_count}")
    print(f"Daily Greed Score: {greed_score:.2f}")
    print(f"Saved/Updated: {CSV_PATH}")
    print("==========================================")

    write_github_summary(
        f"## DCInside Fear & Greed ({today_str}, KST)\n\n"
        f"- 수집 대상: `gall.dcinside.com` / id=`{GALLERY_ID}`\n"
        f"- 수집 시간: **{START_TIME.strftime('%H:%M')} ~ {END_TIME.strftime('%H:%M')}**\n"
        f"- 수집된 제목 수: **{len(collected_posts)}**\n"
        f"- 후보(키워드 포함) 제목 수: **{len(candidates)}**\n"
        f"- 공포(Fear): **{fear_count}**\n"
        f"- 탐욕(Greed): **{greed_count}**\n"
        f"- Daily Greed Score: **{greed_score:.2f}**\n\n"
        f"> 계산식: Greed / (Fear + Greed) * 100\n"
    )


if __name__ == "__main__":
    main()
