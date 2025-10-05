import redis
import time
import json
from datetime import datetime

def create_redis_client():
    """Создание Redis клиента"""
    client = redis.Redis(
        host='localhost',
        port=6379,
        decode_responses=True
    )
    return client

def send_to_stream(client, stream_name='stream_1per30sec'):
    """Отправка сообщений в Redis Stream со скоростью 1 сообщение в 3 секунды"""
    message_count = 0
    interval = 3  # 3 секунды между сообщениями
    
    print(f"Начинаю отправку сообщений в стрим '{stream_name}'...")
    print("Скорость: 1 сообщение каждые 3 секунды")
    print("Нажмите Ctrl+C для остановки\n")
    
    try:
        while True:
            message_count += 1
            
            # Данные для отправки
            message_data = {
                "datetime": "2025-10-05T19:36:42.848947+07:00", 
                "ids": json.dumps(["132134234"]), 
                "truck_id": "00200001" , 
                "new_state": 1,  
                "area": 6, 
                "type": "mc"
            }
            
            # Отправка в Redis Stream
            # XADD возвращает ID сообщения в формате timestamp-sequence
            message_id = client.xadd(stream_name, message_data)
            
            print(f"✓ Отправлено сообщение #{message_count}")
            print(f"  Stream ID: {message_id}")
            print(f"  Timestamp: {message_data['timestamp']}")
            print(f"  Текст: {message_data['text']}\n")
            
            # Пауза 3 секунды
            time.sleep(interval)
            break
            
    except KeyboardInterrupt:
        print(f"\n\nОстановка producer. Всего отправлено: {message_count} сообщений")
    except redis.ConnectionError as e:
        print(f"Ошибка подключения к Redis: {e}")
    finally:
        client.close()

if __name__ == '__main__':
    try:
        # Проверка подключения
        client = create_redis_client()
        client.ping()
        print("✓ Подключение к Redis установлено\n")
        
        send_to_stream(client)
    except Exception as e:
        print(f"Ошибка: {e}")