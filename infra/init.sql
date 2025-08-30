-- 1분 집계 테이블: (symbol, window_start_ts)이 기본키 → 멱등 업서트에 적합
CREATE TABLE IF NOT EXISTS trade_agg_1m (
  symbol            TEXT            NOT NULL,          -- BTCUSDT 등
  window_start_ts   TIMESTAMPTZ     NOT NULL,          -- 1분 윈도우 시작(UTC 권장)
  trade_cnt         BIGINT          NOT NULL,          -- 거래 건수
  volume_sum        NUMERIC(38,10)  NOT NULL,          -- 수량 합계
  price_avg         NUMERIC(18,8)   NOT NULL,          -- 평균 가격
  price_min         NUMERIC(18,8)   NOT NULL,          -- 최저가
  price_max         NUMERIC(18,8)   NOT NULL,          -- 최고가
  updated_at        TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
  PRIMARY KEY(symbol, window_start_ts)                 -- 멱등 업서트 키
);

-- 시간 기준 조회 최적화
CREATE INDEX IF NOT EXISTS idx_trade_agg_1m_ts ON trade_agg_1m(window_start_ts);

-- 1시간 롤업 테이블: Airflow DAG가 채움(1분 집계 → 1시간 집계)
CREATE TABLE IF NOT EXISTS trade_agg_1h (
  symbol            TEXT            NOT NULL,
  window_start_ts   TIMESTAMPTZ     NOT NULL,
  trade_cnt         BIGINT          NOT NULL,
  volume_sum        NUMERIC(38,10)  NOT NULL,
  price_avg         NUMERIC(18,8)   NOT NULL,
  price_min         NUMERIC(18,8)   NOT NULL,
  price_max         NUMERIC(18,8)   NOT NULL,
  updated_at        TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
  PRIMARY KEY(symbol, window_start_ts)
);

CREATE INDEX IF NOT EXISTS idx_trade_agg_1h_ts ON trade_agg_1h(window_start_ts);
