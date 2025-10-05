import redis
import time

def create_redis_client():
    """Создание Redis клиента"""
    client = redis.Redis(
        host='localhost',
        port=6379,
        decode_responses=True
    )
    return client

def consume_from_stream(client, stream_name='test-stream', consumer_group='test-group', consumer_name='consumer-1'):
    """Чтение сообщений из Redis Stream"""
    
    # Создание consumer group (если не существует)
    try:
        client.xgroup_create(stream_name, consumer_group, id='0', mkstream=True)
        print(f"✓ Consumer group '{consumer_group}' создана")
    except redis.ResponseError as e:
        if "BUSYGROUP" in str(e):
            print(f"✓ Consumer group '{consumer_group}' уже существует")
        else:
            raise e
    
    print(f"\nConsumer запущен:")
    print(f"  Stream: {stream_name}")
    print(f"  Group: {consumer_group}")
    print(f"  Name: {consumer_name}")
    print("\nОжидание сообщений... (Нажмите Ctrl+C для остановки)\n")
    
    message_count = 0
    last_id = '>'  # '>' означает получение только новых сообщений
    
    try:
        while True:
            # Чтение сообщений из стрима
            # XREADGROUP читает сообщения для consumer group
            messages = client.xreadgroup(
                groupname=consumer_group,
                consumername=consumer_name,
                streams={stream_name: last_id},
                count=10,  # Читать до 10 сообщений за раз
                block=1000  # Блокировка на 1 секунду, если нет сообщений
            )
            
            if messages:
                for stream, message_list in messages:
                    for message_id, message_data in message_list:
                        message_count += 1
                        
                        print(f"═══════════════════════════════════════")
                        print(f"Получено сообщение #{message_count}")
                        print(f"─────────────────────────────────────")
                        print(f"Stream ID: {message_id}")
                        print(f"Message ID: {message_data.get('message_id', 'N/A')}")
                        print(f"Timestamp: {message_data.get('timestamp', 'N/A')}")
                        print(f"Текст: {message_data.get('text', 'N/A')}")
                        print(f"Данные: {message_data.get('data', 'N/A')}")
                        print()
                        
                        # Подтверждение обработки сообщения (ACK)
                        client.xack(stream_name, consumer_group, message_id)
            
            # Небольшая пауза для снижения нагрузки
            time.sleep(0.1)
            
    except KeyboardInterrupt:
        print(f"\n\nОстановка consumer. Всего получено: {message_count} сообщений")
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
        
        consume_from_stream(client)
    except Exception as e:
        print(f"Ошибка: {e}")