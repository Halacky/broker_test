import json
import random
import time
import argparse
from datetime import datetime
from typing import Dict, Any

# --- Библиотеки ---
import pytz
import redis # <--- Импортируем redis вместо confluent_kafka

# --- Конфигурация ---
# Параметры подключения к Redis
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_CHANNEL = 'stream_1per3sec' # <--- Название канала Redis

# --- Правила генерации (без изменений) ---
STATE_DURATION_SECONDS = 15  # Длительность одного состояния (0 или 1)
# Теперь это количество ПОЛНЫХ ПЕРЕХОДОВ состояния (0->1 или 1->0)
TRANSITIONS_TO_LIVE = 5
MIN_PAUSE_SECONDS = 5        # Минимальная пауза между жизненными циклами ID
MAX_PAUSE_SECONDS = 30       # Максимальная пауза между жизненными циклами ID
TIMEZONE = 'Asia/Bangkok'    # Таймзона UTC+7

# --- Основная логика ---

def get_next_state(current_state: int) -> int:
    """Возвращает следующее состояние (0 -> 1, 1 -> 0)."""
    return 1 - current_state

def generate_message(id_val: str, new_state_val: int) -> str:
    """Генерирует JSON сообщение в нужном формате."""
    now_iso = datetime.now(pytz.timezone(TIMEZONE)).isoformat()
    x = random.uniform(0, 1000)
    y = random.uniform(0, 1000)
    
    message = {
        'datetime': now_iso,
        'id': id_val,
        'coord': [round(x, 2), round(y, 2)],
        'new_state': new_state_val
    }
    return json.dumps(message)

def run_producer(process_number: int):
    """Главная функция продюсера для конкретного процесса."""
    active_ids: Dict[str, Dict[str, Any]] = {}
    next_sequential_id = 1

    # --- Инициализация клиента Redis ---
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        r.ping() # Проверяем соединение
        print(f"[Процесс {process_number}] Успешное подключение к Redis.")
    except redis.exceptions.ConnectionError as e:
        print(f"[Процесс {process_number}] Не удалось подключиться к Redis: {e}")
        return

    print(f"[Процесс {process_number}] Запущен. Нажмите Ctrl+C для остановки.")

    try:
        while True:
            current_time = time.time()
            
            # Если нет активных ID, создаем новый
            if not active_ids:
                id_str = f"{process_number}_{next_sequential_id}"
                active_ids[id_str] = {
                    'current_state': 1,
                    'transition_count': 0,
                    'last_change_time': current_time
                }
                next_sequential_id += 1
                print(f"[Процесс {process_number}] Создан новый ID: {id_str} с начальным состоянием 1.")
                time.sleep(1)
                continue

            ids_to_check = list(active_ids.keys())

            for id_val in ids_to_check:
                id_data = active_ids[id_val]
                
                # Проверяем, пора ли сменить состояние
                if current_time - id_data['last_change_time'] >= STATE_DURATION_SECONDS:
                    
                    new_state = get_next_state(id_data['current_state'])
                    
                    # Генерируем и отправляем сообщение в Redis
                    message_json = generate_message(id_val, new_state)
                    print(message_json) # Выводим в консоль для отладки
                    
                    # --- ОТПРАВКА В REDIS ---
                    r.xadd(REDIS_CHANNEL, {'data': message_json})

                    # Увеличиваем счетчик переходов
                    id_data['transition_count'] += 1

                    # Если ID прожил все переходы, удаляем его
                    if id_data['transition_count'] >= TRANSITIONS_TO_LIVE:
                        print(f"[Процесс {process_number}] ID {id_val} завершил свою жизнь. Удаляем.")
                        del active_ids[id_val]
                        
                        # Делаем паузу перед созданием нового ID
                        pause_duration = random.randint(MIN_PAUSE_SECONDS, MAX_PAUSE_SECONDS)
                        print(f"[Процесс {process_number}] Пауза на {pause_duration} секунд...")
                        time.sleep(pause_duration)
                        break # Выходим из for, чтобы в while создать новый ID
                    
                    # Обновляем состояние ID
                    id_data['current_state'] = new_state
                    id_data['last_change_time'] = current_time
            
            # Небольшая задержка, чтобы не нагружать CPU
            time.sleep(1)

    except KeyboardInterrupt:
        print(f"\n[Процесс {process_number}] Остановка продюсера...")
    except redis.exceptions.RedisError as e:
        print(f"[Процесс {process_number}] Ошибка Redis: {e}")
    finally:
        # Для redis-py не нужно вызывать flush, как для Kafka
        print(f"[Процесс {process_number}] Остановлен.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Redis Producer для генерации состояний.")
    parser.add_argument(
        '--process_num', 
        type=int, 
        required=True, 
        help="Уникальный номер этого процесса (например, 1, 2, или 3)."
    )
    args = parser.parse_args()
    
    run_producer(args.process_num)
