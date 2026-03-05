import os
import pandas as pd
import FinanceDataReader as fdr

START_DATE = os.getenv("START_DATE", "2022-01-01")
THRESHOLD_PCT = float(os.getenv("THRESHOLD_PCT", "3"))
OUT_CSV = os.getenv("OUT_CSV", "kospi_3pct_days.csv")


def load_kospi_df(start_date: str) -> tuple[pd.DataFrame, str]:
    """
    1) FinanceDataReader(KRX: KS11) 먼저 시도
       - 최근 KRX 응답이 'LOGOUT'으로 오는 이슈가 있어 실패할 수 있음 :contentReference[oaicite:5]{index=5}
    2) 실패하면 Yahoo Finance(^KS11)로 fallback :contentReference[oaicite:6]{index=6}
    """
    # 1) FDR 시도
    try:
        print(f"[INFO] Try FinanceDataReader: KS11 from {start_date} ...")
        df = fdr.DataReader("KS11", start_date)
        if df is None or len(df) == 0:
            raise RuntimeError("FinanceDataReader returned empty dataframe.")
        return df, "FinanceDataReader(KRX:KS11)"
    except Exception as e:
        print(f"[WARN] FinanceDataReader failed: {repr(e)}")
        print("[WARN] Fallback to yfinance (^KS11) ...")

    # 2) yfinance fallback
    import yfinance as yf

    df = yf.download(
        "^KS11",
        start=start_date,
        interval="1d",
        auto_adjust=False,
        progress=False,
        threads=False,
    )

    if df is None or len(df) == 0:
        raise RuntimeError("yfinance returned empty dataframe for ^KS11.")

    # yfinance는 컬럼이 'Adj Close'일 수 있어서 정리(우리는 Close만 쓰면 됨)
    if isinstance(df.columns, pd.MultiIndex):
        # 혹시 MultiIndex로 오는 경우 방어
        df.columns = [c[0] for c in df.columns]

    df = df.rename(columns={"Adj Close": "AdjClose"})
    return df, "yfinance(Yahoo:^KS11)"


def main():
    print(f"[INFO] START_DATE={START_DATE}, THRESHOLD_PCT={THRESHOLD_PCT}, OUT_CSV={OUT_CSV}")

    df, source = load_kospi_df(START_DATE)
    print(f"[INFO] Data source: {source}")
    print(f"[INFO] Rows downloaded: {len(df)}")

    df = df.copy()
    df.sort_index(inplace=True)
    df.index = pd.to_datetime(df.index)

    # 전일 종가
    df["PrevClose"] = df["Close"].shift(1)

    # 1) 종가 기준 변동률(close-to-close)
    df["ClosePctChange"] = (df["Close"] / df["PrevClose"] - 1.0) * 100.0
    df["AbsClosePctChange"] = df["ClosePctChange"].abs()

    # 2) 장중 변동폭(High-Low)을 전일 종가로 나눈 비율
    df["IntradayRangePct"] = ((df["High"] - df["Low"]) / df["PrevClose"]) * 100.0

    # 첫 행 제거
    df = df.dropna(subset=["PrevClose", "ClosePctChange"])

    hits = df[df["AbsClosePctChange"] >= THRESHOLD_PCT].copy()

    hits = hits.reset_index()
    date_col = "Date" if "Date" in hits.columns else "index"
    hits = hits.rename(columns={date_col: "date"})

    hits["date"] = pd.to_datetime(hits["date"]).dt.strftime("%Y-%m-%d")
    hits["Direction"] = hits["ClosePctChange"].apply(lambda x: "UP" if x > 0 else "DOWN")

    out = hits[
        [
            "date",
            "Direction",
            "PrevClose",
            "Open",
            "High",
            "Low",
            "Close",
            "ClosePctChange",
            "IntradayRangePct",
        ]
    ].copy()

    out["ClosePctChange"] = out["ClosePctChange"].round(4)
    out["IntradayRangePct"] = out["IntradayRangePct"].round(4)

    out.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")

    print("==========================================")
    print(f"[RESULT] abs(close-to-close) >= {THRESHOLD_PCT}%")
    print(f"[RESULT] rows: {len(out)}")
    print(f"[RESULT] saved: {OUT_CSV}")
    print("==========================================")

    # 날짜별 출력
    for _, r in out.iterrows():
        print(
            f"- {r['date']} {r['Direction']}  "
            f"close_change={r['ClosePctChange']}%  "
            f"intraday_range={r['IntradayRangePct']}%"
        )


if __name__ == "__main__":
    main()
