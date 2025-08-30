from kafka import KafkaProducer
import json
import time

print("=== Kafka Producer 연결 테스트 ===")

def on_success(record_metadata):
    print(f"✅ 전송 성공: topic={record_metadata.topic}, partition={record_metadata.partition}, offset={record_metadata.offset}")

def on_error(excp):
    print(f"❌ 전송 실패: {type(excp).__name__}: {excp}")

# 간단한 Producer 생성 (키 없이)
print("1. Producer 생성 중...")
try:
    producer = KafkaProducer(
        bootstrap_servers=['kafka:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        # key_serializer 제거 (키 없이 전송)
        api_version=(2, 0, 1),
        request_timeout_ms=10000,
        acks="1"
    )
    print("✅ Producer 생성 성공!")
except Exception as e:
    print(f"❌ Producer 생성 실패: {e}")
    exit(1)

# 메타데이터 확인
print("2. 브로커 메타데이터 확인 중...")
try:
    metadata = producer._metadata
    metadata.request_update()
    time.sleep(2)
    
    brokers = metadata.brokers()
    print(f"✅ 연결된 브로커: {brokers}")
    
    if not brokers:
        print("❌ 브로커 메타데이터 없음!")
        raise Exception("메타데이터 수신 실패")
        
except Exception as e:
    print(f"❌ 메타데이터 확인 실패: {e}")

# 테스트 메시지 전송
print("3. 테스트 메시지 전송 중...")
try:
    for i in range(3):
        test_data = {
            'test': f'message_{i}', 
            'timestamp': int(time.time()),
            'index': i
        }
        print(f"  메시지 {i} 전송 요청: {test_data}")
        
        # 키 없이 전송
        future = producer.send('trades_raw', value=test_data)
        future.add_callback(on_success)
        future.add_errback(on_error)
        
        time.sleep(0.1)  # 약간의 간격
    
    print("4. 모든 메시지 전송 완료 대기 중...")
    producer.flush()  # 모든 메시지 전송 완료까지 대기
    print("✅ 테스트 완료!")
    
except Exception as e:
    print(f"❌ 테스트 실패: {type(e).__name__}: {e}")
finally:
    producer.close()
    print("Producer 종료 완료")
