import os
import pandas as pd
import FinanceDataReader as fdr

"""
KOSPI(KS11) 일간 변동폭 3% 이상 날짜 조회 (2023년 이후)
- 기본: 종가 기준 (close-to-close) 절대변동률 >= 3%
- 추가로: 장중 변동폭(High-Low)도 같이 계산해서 저장
"""

START_DATE = os.getenv("START_DATE", "2022-01-01")      # 예: "2023-01-01"
THRESHOLD_PCT = float(os.getenv("THRESHOLD_PCT", "3")) # 예: 3
OUT_CSV = os.getenv("OUT_CSV", "kospi_3pct_days.csv")   # 결과 파일명

def main():
    print(f"[INFO] Download KOSPI (KS11) from {START_DATE} ...")
    df = fdr.DataReader("KS11", START_DATE)  # KOSPI Index (KS11) :contentReference[oaicite:2]{index=2}

    if df is None or len(df) == 0:
        raise RuntimeError("No data returned from FinanceDataReader.")

    df = df.copy()
    df.sort_index(inplace=True)
    df.index = pd.to_datetime(df.index)

    # 전일 종가
    df["PrevClose"] = df["Close"].shift(1)

    # 1) 종가 기준 변동률(close-to-close)
    df["ClosePctChange"] = (df["Close"] / df["PrevClose"] - 1.0) * 100.0
    df["AbsClosePctChange"] = df["ClosePctChange"].abs()

    # 2) 장중 변동폭(High-Low)을 전일 종가로 나눈 비율
    # (원하면 이걸로 3% 이상을 볼 수도 있음)
    df["IntradayRangePct"] = ((df["High"] - df["Low"]) / df["PrevClose"]) * 100.0

    # 첫 행은 PrevClose가 없으니 제거
    df = df.dropna(subset=["PrevClose", "ClosePctChange"])

    hits = df[df["AbsClosePctChange"] >= THRESHOLD_PCT].copy()

    # 결과 정리
    hits = hits.reset_index()
    date_col = "Date" if "Date" in hits.columns else "index"
    hits = hits.rename(columns={date_col: "date"})

    hits["date"] = pd.to_datetime(hits["date"]).dt.strftime("%Y-%m-%d")
    hits["Direction"] = hits["ClosePctChange"].apply(lambda x: "UP" if x > 0 else "DOWN")

    out = hits[[
        "date",
        "Direction",
        "PrevClose",
        "Open", "High", "Low", "Close",
        "ClosePctChange",
        "IntradayRangePct",
    ]].copy()

    # 보기 좋게 소수점 정리
    out["ClosePctChange"] = out["ClosePctChange"].round(4)
    out["IntradayRangePct"] = out["IntradayRangePct"].round(4)

    # 저장
    out.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")

    print("==========================================")
    print(f"[RESULT] threshold: abs(close-to-close) >= {THRESHOLD_PCT}%")
    print(f"[RESULT] rows: {len(out)}")
    print(f"[RESULT] saved: {OUT_CSV}")
    print("==========================================")

    # 콘솔에도 출력(각각 조회 느낌으로)
    if len(out) > 0:
        for _, r in out.iterrows():
            print(f"- {r['date']} {r['Direction']}  close_change={r['ClosePctChange']}%  intraday_range={r['IntradayRangePct']}%")
    else:
        print("조건(>= threshold)에 해당하는 날짜가 없습니다.")

if __name__ == "__main__":
    main()
