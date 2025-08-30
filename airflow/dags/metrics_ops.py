from datetime import datetime, timedelta
from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# ---- 공통 기본 설정 ----
DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# ---- DAG 정의 ----
with DAG(
    dag_id="metrics_ops",                    # DAG 이름
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 8, 1),
    schedule_interval="0 * * * *",          # 매 정시(1시간마다) 실행
    catchup=False,
    tags=["realtime", "ops"],
) as dag:

    # ---- DQ: 최근 10분 내 행이 있는지 검사 ----
    def dq_check_recent_30m():
        hook = PostgresHook(postgres_conn_id="postgres_default")
        sql = """
          SELECT COUNT(*) AS cnt
          FROM trade_agg_1m
          WHERE window_start_ts >= NOW() - INTERVAL '30 minutes'
        """
        cnt = hook.get_first(sql)[0]
        if cnt == 0:
            # 실패로 처리 → 알림 연동 시 Slack 등으로 통지 가능
            raise ValueError("DQ FAIL: last 30 minutes have no rows in trade_agg_1m")

    dq_recent_30m = PythonOperator(
        task_id="dq_recent_30m",
        python_callable=dq_check_recent_30m,
    )

    # ---- 1시간 롤업: 1분 집계를 1시간 버킷으로 upsert ----
    rollup_1h_sql = """
    WITH src AS (
      SELECT
        symbol,
        date_trunc('hour', window_start_ts) AS hour_bucket,
        SUM(trade_cnt)  AS trade_cnt,
        SUM(volume_sum) AS volume_sum,
        AVG(price_avg)  AS price_avg,
        MIN(price_min)  AS price_min,
        MAX(price_max)  AS price_max
      FROM trade_agg_1m
      WHERE window_start_ts >= date_trunc('hour', NOW()) - INTERVAL '1 hour'
        AND window_start_ts <  date_trunc('hour', NOW()) + INTERVAL '1 hour'
      GROUP BY symbol, hour_bucket
    )
    INSERT INTO trade_agg_1h (
      symbol, window_start_ts, trade_cnt, volume_sum, price_avg, price_min, price_max
    )
    SELECT
      symbol, hour_bucket, trade_cnt, volume_sum, price_avg, price_min, price_max
    FROM src
    ON CONFLICT (symbol, window_start_ts) DO UPDATE SET
      trade_cnt = EXCLUDED.trade_cnt,
      volume_sum = EXCLUDED.volume_sum,
      price_avg = EXCLUDED.price_avg,
      price_min = EXCLUDED.price_min,
      price_max = EXCLUDED.price_max,
      updated_at = NOW();
    """

    rollup_1h = SQLExecuteQueryOperator(
        task_id="rollup_1h",
        conn_id="postgres_default",
        sql=rollup_1h_sql,
    )

    # ---- 백업: pg_dump로 전체 DB 백업 파일 생성 ----
    backup_cmd = r"""
    set -e
    TS=$(date +%Y%m%d_%H%M%S)
    PGPASSWORD=airflow pg_dump -h postgres -p 5432 -U airflow -d airflow -F p -f /opt/backups/backup_${TS}.sql
    """
    backup_daily = BashOperator(
        task_id="backup_daily",
        bash_command=backup_cmd,
    )

    # ---- 실행 순서: DQ → 롤업 → 백업 ----
    chain(dq_recent_30m, rollup_1h, backup_daily)
