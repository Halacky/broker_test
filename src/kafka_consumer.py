from kafka import KafkaConsumer
import json

def create_consumer(topic='test-topic_4persec'):
    """Создание Kafka consumer"""
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',  # Читать с начала топика
        enable_auto_commit=True,
        group_id='test-consumer-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        api_version=(2, 5, 0)
    )
    return consumer

def consume_messages(consumer):
    """Чтение и вывод сообщений из топика"""
    print(f"Consumer запущен. Ожидание сообщений...")
    print(f"Топик: {consumer.subscription()}")
    print("Нажмите Ctrl+C для остановки\n")
    
    message_count = 0
    
    try:
        for message in consumer:
            message_count += 1
            
            print(f"─────────────────────────────────────")
            print(f"Получено сообщение #{message_count}")
            print(f"Partition: {message.partition}")
            print(f"Offset: {message.offset}")
            print(f"Данные: {message.value}")
            print()
            
    except KeyboardInterrupt:
        print(f"\n\nОстановка consumer. Всего получено: {message_count} сообщений")
    finally:
        consumer.close()

if __name__ == '__main__':
    try:
        consumer = create_consumer()
        consume_messages(consumer)
    except Exception as e:
        print(f"Ошибка: {e}")