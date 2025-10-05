import asyncio
import json
import os
from datetime import datetime

import matplotlib.pyplot as plt
from aiokafka import AIOKafkaConsumer

# --- Конфигурация ---
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "topic_0_25sec"
OBJECT_IDS = ['00200001', '00200002', '00200003', '00200004']

# Интервал для построения графиков в секундах
PLOT_INTERVAL_SEC = 10

# Основная директория для сохранения кадров
OUTPUT_DIR = "trajectory_frames"

async def listen_for_messages(consumer: AIOKafkaConsumer, trajectories: dict):
    """
    Асинхронная задача для непрерывного прослушивания сообщений из Kafka.
    Обновляет общий словарь с траекториями.
    """
    print("Слушатель сообщений запущен.")
    try:
        async for msg in consumer:
            try:
                data = json.loads(msg.value.decode('utf-8'))
                obj_id = data.get('id')
                coord = data.get('coord')

                if obj_id and coord and obj_id in trajectories:
                    # Добавляем новую точку в траекторию соответствующего объекта
                    trajectories[obj_id].append(tuple(coord))
            except (json.JSONDecodeError, KeyError) as e:
                print(f"Ошибка обработки сообщения: {e}. Пропускаем.")
    except Exception as e:
        print(f"Критическая ошибка в слушателе: {e}")

async def plot_and_save(trajectories: dict):
    """
    Асинхронная задача, которая каждые N секунд строит графики
    и сохраняет их в файлы.
    """
    print("Построитель графиков запущен.")
    frame_counter = 1

    # Создаем основную директорию, если ее нет
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    while True:
        await asyncio.sleep(PLOT_INTERVAL_SEC)
        print(f"\n--- Генерация кадров для кадра #{frame_counter} ---")

        for obj_id, points in trajectories.items():
            if not points:
                print(f"[{obj_id}] Нет данных для построения.")
                continue

            # Создаем отдельную папку для каждого ID
            obj_dir = os.path.join(OUTPUT_DIR, obj_id)
            os.makedirs(obj_dir, exist_ok=True)

            # Разделяем координаты на X и Y для построения
            x_coords, y_coords = zip(*points)

            # Создаем новый график
            plt.figure(figsize=(10, 8))
            plt.plot(x_coords, y_coords, marker='o', linestyle='-', markersize=3, label=f'Path of {obj_id}')
            
            # Настраиваем внешний вид графика
            plt.title(f"Траектория объекта ID: {obj_id}\nВремя генерации: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            plt.xlabel("Координата X (м)")
            plt.ylabel("Координата Y (м)")
            plt.legend()
            plt.axis('equal') # Масштаб осей делается одинаковым для сохранения пропорций
            plt.grid(True)

            # Формируем имя файла и сохраняем
            filename = os.path.join(obj_dir, f"frame_{frame_counter:03d}.png")
            plt.savefig(filename)
            plt.close() # Важно закрывать фигуру, чтобы освободить память
            
            print(f"[{obj_id}] График сохранен в {filename}")

        frame_counter += 1

async def main():
    """
    Основная функция для запуска консьюмера и задач.
    """
    # Инициализируем словарь для хранения траекторий каждого объекта
    trajectories = {obj_id: [] for obj_id in OBJECT_IDS}
    
    consumer = AIOKafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="earliest", # Начать чтение с самого начала, если нет смещения
        group_id="trajectory_plotter_group" # Уникальный ID группы консьюмеров
    )

    try:
        await consumer.start()
        print("Консьюмер Kafka запущен.")

        # Создаем две задачи, которые будут работать параллельно
        listener_task = asyncio.create_task(listen_for_messages(consumer, trajectories))
        plotter_task = asyncio.create_task(plot_and_save(trajectories))

        await asyncio.gather(listener_task, plotter_task)
        
    except KeyboardInterrupt:
        print("\nПрограмма остановлена пользователем.")
    finally:
        print("Остановка консьюмера...")
        await consumer.stop()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"Произошла непредвиденная ошибка: {e}")

