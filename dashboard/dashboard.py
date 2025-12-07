import streamlit as st
import pandas as pd
import psycopg2
import numpy as np
import os
from dotenv import load_dotenv
import plotly.express as px

load_dotenv()
print(os.getenv("HOST"))
# DB 연결
conn = psycopg2.connect(
    host=os.getenv("HOST"),
    dbname=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    port=os.getenv("POSTGRES_PORT1")
)
#####################SQL 쿼리 ##########################
query_1h = """
        SELECT *
        FROM trade_agg_1m
        WHERE symbol = 'BTCUSDT'
          AND window_start_ts >= NOW() - INTERVAL '1 hour'
        ORDER BY window_start_ts;
    """

query_recent = """
        SELECT *
        FROM trade_agg_1m
        WHERE symbol = 'BTCUSDT'
          AND window_start_ts >= NOW() - INTERVAL '10 minutes'
        ORDER BY window_start_ts DESC
        LIMIT 10;
    """

#######################################################
df_recent = pd.read_sql(query_recent, conn)
df_recent["minute"] = pd.to_datetime(df_recent["window_start_ts"]).dt.strftime("%H:%M")
df_1h = pd.read_sql(query_1h, conn)

df_1h["window_start_ts"] = (
    pd.to_datetime(df_1h["window_start_ts"])
    .dt.tz_convert("Asia/Seoul")
    .dt.tz_localize(None)
)

avg_price = df_1h["price_avg"].mean()
max_price = df_1h["price_max"].max()
min_price = df_1h["price_min"].min()
trade_count = df_1h["trade_cnt"].sum()

st.set_page_config(page_title="Binance Dashboard", layout="wide")

st.title("Binance BTC/USDT 거래 대시보드")

# =========================
#  컬럼1: 데이터 수집 상태
# =========================
col1, col2= st.columns([1, 2]) 

with col1:
    if not df_recent.empty:
        st.subheader("🟢 데이터 수집 상태")
        st.success("정상적으로 데이터가 수집되고 있습니다.!")
        st.caption(f"최근 10분간 수집된 데이터: {sum(df_recent['trade_cnt'])} rows")
        
        bar_fig = px.bar(
            df_recent,
            x="minute",
            y="trade_cnt",
            title="최근 10분간 거래 건수 (분 단위)",
            labels={"minute": "시간", "trade_cnt": "거래 건수"},
            text="trade_cnt"
        )
        bar_fig.update_traces(textposition="outside")
        st.plotly_chart(bar_fig, use_container_width=True)

    else:
        st.subheader("🔴 데이터 수집 상태")
        st.error("데이터가 수집되지 않고 있습니다. 확인이 필요합니다!")
    
    st.subheader("변동성 분석")

    if not df_1h.empty:
        std_dev = df_1h["price_avg"].std()
        vol_rate = (df_1h["price_max"].max() - df_1h["price_min"].min()) / avg_price * 100

        st.metric("표준편차 (1시간)", f"{std_dev:,.2f}")
        st.metric("변동률 (max-min / avg)", f"{vol_rate:.2f}%")

        # 변동성 그래프 (rolling std)
        df_1h["rolling_std"] = df_1h["price_avg"].rolling(window=5).std()
        st.line_chart(df_1h.set_index("window_start_ts")["rolling_std"], height=200)

# =========================
#  컬럼2: 메인 대시보드
# =========================
with col2:
    st.subheader("메인 대시보드")
    if not df_1h.empty:

        # CSS 스타일 적용
        st.markdown(
            """
            <style>
            .kpi-box {
                background-color: #f9f9f9;
                padding: 10px;
                border-radius: 10px;
                text-align: center;
                margin: 5px;
            }
            .kpi-title {
                font-size: 16px;
                color: #555;
            }
            .kpi-value {
                font-size: 20px;
                font-weight: bold;
                color: #000;
                white-space: nowrap; /* 숫자가 줄바꿈되거나 ... 으로 줄여지지 않게 */
            }
            </style>
            """,
            unsafe_allow_html=True
        )

        # KPI를 가로로 배치
        kpi1, kpi2, kpi3, kpi4 = st.columns(4)

        with kpi1:
            st.markdown(
                f"<div class='kpi-box'><div class='kpi-title'>평균 거래가 (1시간)</div>"
                f"<div class='kpi-value'>{avg_price:,.2f} USDT</div></div>",
                unsafe_allow_html=True
            )
        with kpi2:
            st.markdown(
                f"<div class='kpi-box'><div class='kpi-title'>최고가</div>"
                f"<div class='kpi-value'>{max_price:,.2f} USDT</div></div>",
                unsafe_allow_html=True
            )
        with kpi3:
            st.markdown(
                f"<div class='kpi-box'><div class='kpi-title'>최저가</div>"
                f"<div class='kpi-value'>{min_price:,.2f} USDT</div></div>",
                unsafe_allow_html=True
            )
        with kpi4:
            st.markdown(
                f"<div class='kpi-box'><div class='kpi-title'>거래 횟수 (1시간)</div>"
                f"<div class='kpi-value'>{trade_count:,}</div></div>",
                unsafe_allow_html=True
            )

        # 차트 (가격/거래량)
        #st.line_chart(df_1h.set_index("window_start_ts")["price_avg"], height=300)
        line_fig = px.line(
            df_1h,
            x="window_start_ts",
            y="price_avg",
            title="평균 거래가 (1시간)",
            labels={"window_start_ts": "시간", "price_avg": "평균 거래가 (USDT)"}
        )

        # y축 범위를 데이터 값 주변으로 좁게 설정
        ymin = df_1h["price_avg"].min() * 0.999
        ymax = df_1h["price_avg"].max() * 1.001
        line_fig.update_yaxes(range=[ymin, ymax])

        st.plotly_chart(line_fig, use_container_width=True, height=300)
        st.bar_chart(df_1h.set_index("window_start_ts")["volume_sum"], height=200)

