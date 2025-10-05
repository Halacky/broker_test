import asyncio
import json
import random
import time
from datetime import datetime, timezone, timedelta

import numpy as np
from math import cos, sin, radians

from aiokafka import AIOKafkaProducer

# --- Конфигурация ---
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "topic_0_25sec"
OBJECT_IDS = ['00200001', '00200002', '00200003', '00200004']

# Параметры движения
MOVEMENT_CYCLE_DURATION_SEC = 30.0
TURN_DURATION_SEC = 1.0
SPEED_MPS = 1.0
STRAIGHT_DISTANCE_M = SPEED_MPS * ((MOVEMENT_CYCLE_DURATION_SEC - TURN_DURATION_SEC) / 2)
TURN_ANGLE_DEG = 170

# Параметры остановки
MIN_STOP_DURATION_SEC = 5
MAX_STOP_DURATION_SEC = 30

# Частота отправки сообщений
SEND_INTERVAL_SEC = 0.25

# Таймзона +7
TIMEZONE = timezone(timedelta(hours=7))

def generate_movement_deltas():
    """
    Генерирует один цикл движения в виде списка векторов смещения (dx, dy).
    Это позволяет применять один и тот же паттерн движения из любой начальной точки.
    """
    first_straight_time = STRAIGHT_DISTANCE_M / SPEED_MPS
    
    dt = 0.1  # Дискретизация 50 Гц
    time_points = np.arange(0, MOVEMENT_CYCLE_DURATION_SEC + dt, dt)
    
    deltas = []
    prev_x, prev_y = 0.0, 0.0
    current_angle = 0.0
    
    for t in time_points:
        if t <= first_straight_time:
            distance = SPEED_MPS * t
            x = distance * cos(current_angle)
            y = distance * sin(current_angle)
            
        elif t <= first_straight_time + TURN_DURATION_SEC:
            x = STRAIGHT_DISTANCE_M * cos(current_angle)
            y = STRAIGHT_DISTANCE_M * sin(current_angle)
            print("!-----------TURN-----------!")
        else:
            turn_angle_rad = radians(TURN_ANGLE_DEG)
            move_time = t - (first_straight_time + TURN_DURATION_SEC)
            distance_after_turn = SPEED_MPS * move_time
            
            start_x = STRAIGHT_DISTANCE_M * cos(current_angle)
            start_y = STRAIGHT_DISTANCE_M * sin(current_angle)
            
            x = start_x + distance_after_turn * cos(turn_angle_rad)
            y = start_y + distance_after_turn * sin(turn_angle_rad)

        # Рассчитываем вектор смещения от предыдущей точки
        dx = x - prev_x
        dy = y - prev_y
        deltas.append((dx, dy))
        
        prev_x, prev_y = x, y
    
    return deltas


class ObjectSimulator:
    """
    Класс для симуляции состояния одного объекта.
    Теперь корректно обрабатывает непрерывную траекторию.
    """
    def __init__(self, object_id: str, movement_cycle: list):
        self.id = object_id
        self.movement_cycle = movement_cycle  # Список векторов (dx, dy)
        self.cycle_index = 0
        self.is_moving = True
        self.stop_timer = 0
        # Начинаем с нулевых координат
        self.last_coord = (0.0, 0.0)

    def get_next_state(self) -> tuple:
        """
        Возвращает следующие координаты и состояние движения.
        """
        if self.is_moving:
            if self.cycle_index < len(self.movement_cycle):
                # Берем следующий вектор движения и применяем его к последней координате
                dx, dy = self.movement_cycle[self.cycle_index]
                self.last_coord = (self.last_coord[0] + dx, self.last_coord[1] + dy)
                self.cycle_index += 1
                return self.last_coord, True
            else:
                # Цикл движения завершен, начинаем остановку
                self.is_moving = False
                self.stop_timer = random.randint(MIN_STOP_DURATION_SEC, MAX_STOP_DURATION_SEC)
                # Сбрасываем индекс для следующего цикла
                self.cycle_index = 0
                print('Цикл движения завершен, начинаем остановку')
                return self.last_coord, False
        else:  # Объект стоит на месте
            self.stop_timer -= SEND_INTERVAL_SEC
            if self.stop_timer <= 0:
                # Остановка закончилась, начинаем новый цикл движения
                self.is_moving = True
                # cycle_index уже равен 0, так что новый цикл начнется правильно
            
            return self.last_coord, False


async def produce_for_object(producer: AIOKafkaProducer, object_id: str, deltas: list):
    """
    Асинхронная задача для одного объекта. Генерирует и отправляет сообщения.
    """
    simulator = ObjectSimulator(object_id, deltas)
    print(f"[{object_id}] Симулятор запущен.")
    
    while True:
        coord, is_moving = simulator.get_next_state()
        
        message = {
            "datetime": datetime.now(TIMEZONE).isoformat(),
            "id": simulator.id,
            "section_name": f"section_{random.randint(1, 40)}",
            "coord": list(coord)
        }
        
        try:
            # print(message)
            await producer.send_and_wait(TOPIC_NAME, json.dumps(message).encode('utf-8'))
        except Exception as e:
            print(f"[{object_id}] Ошибка отправки: {e}")

        await asyncio.sleep(SEND_INTERVAL_SEC)


async def main():
    """
    Основная функция для запуска продюсера и создания задач для каждого объекта.
    """
    # Предварительно генерируем цикл ВЕКТОРОВ движения
    movement_deltas = generate_movement_deltas()
    print(f"Сгенерирован цикл движения из {len(movement_deltas)} векторов.")

    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BROKER)
    
    try:
        await producer.start()
        print("Продюсер Kafka запущен.")
        
        tasks = [
            asyncio.create_task(produce_for_object(producer, oid, movement_deltas))
            for oid in OBJECT_IDS
        ]
        
        await asyncio.gather(*tasks)
        
    finally:
        print("Остановка продюсера...")
        await producer.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nПрограмма остановлена пользователем.")

