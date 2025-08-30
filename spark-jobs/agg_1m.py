from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp, window,
    count, sum as _sum, avg, min as _min, max as _max
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
import psycopg2
from psycopg2.extras import execute_values

# ---- Kafka/Spark/체크포인트 설정 ----
KAFKA_BOOTSTRAP = "kafka:9092"                          # 브로커 주소(컨테이너 네트워크)
KAFKA_TOPIC = "trades_raw"                              # 입력 토픽
CHECKPOINT = "/tmp/spark-checkpoint/trade_agg_1m"       # 스트리밍 상태 저장 경로

# ---- Postgres 연결 정보 ----
PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PW = "postgres", 5432, "airflow", "airflow", "airflow"

# ---- JSON 스키마: producer가 넣은 필드 구조 ----
schema = StructType([
    StructField("ts", LongType(), False),               # 이벤트 시간(ms since epoch)
    StructField("symbol", StringType(), False),         # BTCUSDT
    StructField("price", DoubleType(), False),          # 가격
    StructField("qty", DoubleType(), False),            # 수량
    StructField("side", StringType(), True),            # buy/sell (옵션)
])

def upsert_to_postgres(df, epoch_id: int):
    """
    foreachBatch 콜백: 마이크로배치 결과를 Postgres에 멱등 upsert.
    - df는 이미 (symbol, window_start_ts, 집계들) 스키마로 정리되어 들어옴.
    """
    rows = [
        (
            r["symbol"],
            r["window_start_ts"],
            int(r["trade_cnt"]),
            float(r["volume_sum"]),
            float(r["price_avg"]),
            float(r["price_min"]),
            float(r["price_max"]),
        )
        for r in df.collect()                            # 소량 집계라 드라이버 수집 사용
    ]
    if not rows:
        return

    # (symbol, window_start_ts) 키로 upsert
    sql = """
        INSERT INTO trade_agg_1m
          (symbol, window_start_ts, trade_cnt, volume_sum, price_avg, price_min, price_max)
        VALUES %s
        ON CONFLICT (symbol, window_start_ts) DO UPDATE SET
          trade_cnt = EXCLUDED.trade_cnt,
          volume_sum = EXCLUDED.volume_sum,
          price_avg = EXCLUDED.price_avg,
          price_min = EXCLUDED.price_min,
          price_max = EXCLUDED.price_max,
          updated_at = NOW();
    """
    conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PW)
    try:
        with conn, conn.cursor() as cur:
            execute_values(cur, sql, rows, page_size=500)  # 대량 insert…on conflict
    finally:
        conn.close()

if __name__ == "__main__":
    # ---- Spark 세션 생성 ----
    spark = (
        SparkSession.builder
        .appName("agg_1m")                                # 앱 이름
        .master("local[2]")                               # 단일 컨테이너 로컬 모드(2 스레드)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")                # 로그 소음 줄이기

    # ---- Kafka 소스에서 스트림 읽기 ----
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")              # 최신부터
        .load()
    )

    # ---- JSON 파싱 + 이벤트 시간 컬럼 생성 ----
    parsed = (
        raw_df
        .selectExpr("CAST(value AS STRING) as json_str")  # Kafka value는 bytes → string
        .select(from_json(col("json_str"), schema).alias("data"))
        .select("data.*")
        .withColumn("event_time", to_timestamp((col("ts")/1000).cast("double")))  # ms → s → timestamp
    )

    # ---- 1분 텀블링 윈도우 집계 (워터마크 2분) ----
    agg = (
        parsed
        .withWatermark("event_time", "2 minutes")         # 2분 지연까지 허용
        .groupBy(
            window(col("event_time"), "1 minute").alias("w"),
            col("symbol")
        )
        .agg(
            count("*").alias("trade_cnt"),
            _sum("qty").alias("volume_sum"),
            avg("price").alias("price_avg"),
            _min("price").alias("price_min"),
            _max("price").alias("price_max"),
        )
        .select(
            col("symbol"),
            col("w.start").alias("window_start_ts"),
            "trade_cnt", "volume_sum", "price_avg", "price_min", "price_max"
        )
    )

    # ---- foreachBatch로 Postgres upsert ----
    query = (
        agg.writeStream
        .outputMode("update")                             # 윈도우 갱신 반영
        .option("checkpointLocation", CHECKPOINT)         # 상태/오프셋 저장
        .foreachBatch(upsert_to_postgres)                 # 배치 단위 upsert 콜백
        .start()
    )

    query.awaitTermination()                              # 스트리밍 지속 실행
