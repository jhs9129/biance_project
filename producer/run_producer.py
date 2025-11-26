import asyncio
import os
import ujson as json
import websockets
from kafka import KafkaProducer

# ---- 환경변수/기본값 ----
WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@trade"
TOPIC = os.getenv("KAFKA_TOPIC")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP")
SYMBOL = "BTCUSDT"

# ---- Producer 설정 ----
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda v: v.encode("utf-8"),
    
    acks=0,                    # ACK 대기 안함
    linger_ms=0,               # 즉시 전송
    max_block_ms=5000,         # 5초 타임아웃
    
    # 기존 설정 대부분 유지
    batch_size=8192,
    retries=3,                 # 재시도 횟수만 줄임
)

def map_binance_trade(msg: dict) -> dict:
    ts_ms = msg.get("T") or msg.get("E")
    price = float(msg["p"])
    qty   = float(msg["q"])
    side  = "sell" if msg.get("m") else "buy"
    return {"ts": ts_ms, "symbol": SYMBOL, "price": price, "qty": qty, "side": side}

async def run():
    while True:
        try:
            # 20초마다 ping/pong 교환 (타임아웃 20초)
            async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20) as ws: 
                print(f"[producer] connected to {WS_URL}")
                message_count = 0
                
                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                        data = map_binance_trade(msg)
                        key = data["symbol"]
                        
                        print(f"[producer] sending to Kafka: key={key}, value={data}")
                        
                        # 단순한 전송 (콜백 제거)
                        producer.send(TOPIC, value=data, key=key)
                        print(f"[producer] ✅ 메시지 전송 완료")
                        
                        message_count += 1
                        # 주기적 버퍼 비우기 (중요한 추가!)
                        if message_count % 20 == 0:
                            producer.flush(timeout=2)
                            print(f"[producer] 🔄 버퍼 플러시 완료 ({message_count}개 메시지)")
                            
                    except Exception as e:
                        print("[producer] parse/send error:", e)
        except Exception as e:
            print("[producer] ws error, reconnecting in 3s:", e)
            await asyncio.sleep(3)

if __name__ == "__main__":
    try:
        asyncio.run(run())
    finally:
        producer.flush(timeout=5)  # 타임아웃 추가!
        producer.close()
