import streamlit as st
import pandas as pd
import psycopg2

# DB 연결
conn = psycopg2.connect(
    host="localhost",
    dbname="airflow",
    user="airflow",
    password="airflow",
    port=5433
)

st.title("📊 Binance 거래 데이터 대시보드")

st.markdown("""
이 대시보드는 **Binance 실시간 거래 데이터**를 수집하여 집계한 결과를 보여줍니다.  
- `trade_agg_1m`: 1분 단위 집계 데이터  
- `trade_agg_1h`: 1시간 단위 집계 데이터  
아래 표와 그래프는 **BTCUSDT(비트코인/테더 거래쌍)** 기준입니다.
""")

# 최근 24시간 (1440분) 데이터 불러오기
query = """
    SELECT symbol, window_start_ts, trade_cnt, volume_sum,
           price_avg, price_min, price_max, updated_at
    FROM trade_agg_1m
    WHERE symbol = 'BTCUSDT'
    ORDER BY window_start_ts DESC
    LIMIT 1440;
"""
df = pd.read_sql(query, conn)

# --- 📌 데이터 컬럼 설명 추가 ---
st.subheader("📑 데이터 컬럼 설명")
st.markdown("""
- **symbol**: 거래쌍 (예: BTCUSDT → 비트코인/테더)  
- **window_start_ts**: 집계 시작 시각 (1분 단위)  
- **trade_cnt**: 해당 구간 내 거래 횟수  
- **volume_sum**: 해당 구간 내 거래량(USDT 기준)  
- **price_avg**: 평균 거래 가격  
- **price_min**: 최저 거래 가격  
- **price_max**: 최고 거래 가격  
- **updated_at**: DB에 데이터가 업데이트된 시각  
""")

# --- 📊 원본 데이터 테이블 표시 ---
st.subheader("📋 최근 24시간 (1분 단위) 거래 데이터")
st.dataframe(df)

# --- 📈 시각화 ---
st.subheader("📈 시세 및 거래량 추이")
df_plot = df.set_index("window_start_ts").sort_index()

st.line_chart(df_plot["price_avg"], height=300)
st.bar_chart(df_plot["volume_sum"], height=300)

st.caption("※ 위 그래프는 최근 24시간 동안의 비트코인 평균 가격 및 거래량 변화를 보여줍니다.")
