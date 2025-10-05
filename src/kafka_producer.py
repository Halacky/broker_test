from kafka import KafkaProducer
import json
import time
from datetime import datetime

def create_producer():
    """Создание Kafka producer"""
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        api_version=(2, 5, 0)
    )
    return producer

def send_messages(producer, topic='test-topic_4persec'):
    """Отправка сообщений со скоростью 4 в секунду"""
    message_count = 0
    interval = 0.25  # 1/4 секунды между сообщениями = 4 сообщения в секунду
    
    print(f"Начинаю отправку сообщений в топик '{topic}'...")
    print("Нажмите Ctrl+C для остановки\n")
    
    try:
        while True:
            message_count += 1
            message = {
                "datetime": "2025-10-05T19:36:42.848947+07:00", 
                "ids": ["132134234", "132134264"], 
                "truck_id": "00200001" , 
                "new_state": 0,  
                "area": 6, 
                "type": 
                "t"
            }
            
            # Отправка сообщения
            future = producer.send(topic, value=message)
            
            # Ожидание подтверждения отправки
            record_metadata = future.get(timeout=10)
            
            print(f"✓ Отправлено: ID={message_count}, "
                  f"Partition={record_metadata.partition}, "
                  f"Offset={record_metadata.offset}")
            
            # Пауза для достижения скорости 4 сообщения/сек
            time.sleep(interval)
            break
            
    except KeyboardInterrupt:
        print(f"\n\nОстановка producer. Всего отправлено: {message_count} сообщений")
    finally:
        producer.close()

if __name__ == '__main__':
    try:
        producer = create_producer()
        send_messages(producer)
    except Exception as e:
        print(f"Ошибка: {e}")