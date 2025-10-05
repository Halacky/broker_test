# file path: /home/kirill/projects/folium/broker_stand/src/project/data_proccesor.py

from typing import Optional, Dict, Any, List, Tuple
from collections import deque
import json
import asyncio
import logging
import os
import math
from datetime import datetime, timedelta

from scipy.signal import find_peaks
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

from queue_manager import QueueManager
from data_source import Message
from models import Product, Truck, Area

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataProcessor:
    def __init__(self, queue_manager: QueueManager):
        self.queue_manager = queue_manager
        # ИСПРАВЛЕНО: используем правильные имена источников из main_pp.py
        self.redis_source_name = "redis_chanel_1per3sec"
        self.kafka_source_name = "kafka_topic_1per0_25sec"
        self._running = False
        self.cleanup_counter = 0
        self.id_loggers: Dict[str, logging.Logger] = {}
        self.frame_counters: Dict[str, int] = {}
        self.id_log_dir = os.path.join("logs", "ids")
        self.windows_dir = os.path.join("logs", "windows")
        os.makedirs(self.id_log_dir, exist_ok=True)
        os.makedirs(self.windows_dir, exist_ok=True)
        # Регистры доменных сущностей
        self.products_registry: Dict[str, Product] = {}
        self.trucks_registry: Dict[str, Truck] = {}
        self.areas_registry: Dict[str, Area] = {}

    # --- Работа со стримом redis_chanel_1per30sec ---
    def _ensure_truck(self, truck_id: str, actual_state: Optional[int] = None) -> Truck:
        if truck_id in self.trucks_registry:
            return self.trucks_registry[truck_id]
        truck = Truck.create(id=truck_id, actual_state=actual_state if actual_state is not None else 0)
        self.trucks_registry[truck_id] = truck
        return truck

    def _ensure_product(self, internal_id: str, type_value: str, first_ts: datetime) -> Product:
        if internal_id in self.products_registry:
            return self.products_registry[internal_id]
        product = Product.create(
            internal_id=internal_id,
            type=type_value,
            first_interaction_at=first_ts,
        )
        self.products_registry[internal_id] = product
        return product

    def _process_stream_30sec_message(self, msg: Message) -> None:
        """Обработка сообщений из stream_1per30sec (redis_chanel_1per30sec).

        Формат ожидания msg.data:
          {"ids": ["..."], "truck_id": "...", "type": "t", ...}
        """
        data = msg.data or {}
        ids = data.get("ids") or []
        truck_id = data.get("truck_id")
        type_value = data.get("type")
        actual_state = data.get("new_state")

        # Нормализация ids: иногда приходит JSON-строка вида '["1","2"]'
        if isinstance(ids, str):
            try:
                parsed_ids = json.loads(ids)
                if isinstance(parsed_ids, list):
                    ids = parsed_ids
                else:
                    ids = [str(parsed_ids)]
            except Exception:
                # Фоллбек: оборачиваем исходную строку как один ID
                ids = [ids]

        if not isinstance(ids, list) or not truck_id or not type_value:
            logger.warning("Некорректное сообщение 30sec: %s", data)
            return

        # Приводим состояние к int, если возможно
        try:
            actual_state_int = int(actual_state) if actual_state is not None else 0
        except (TypeError, ValueError):
            actual_state_int = 0

        truck = self._ensure_truck(truck_id, actual_state=actual_state_int)

        created_or_attached: List[str] = []
        for internal_id in ids:
            if not internal_id:
                continue
            product = self._ensure_product(internal_id=internal_id, type_value=type_value, first_ts=msg.timestamp)

            # Добавляем в Truck, если ещё не добавлен
            if all(p.internal_id != product.internal_id for p in truck.products):
                truck.products.append(product)
                truck.last_modified_at = msg.timestamp
                truck.actual_state = actual_state_int
            created_or_attached.append(product.internal_id)

        # Логирование в файл конкретного truck_id через уже существующий механизм
        self._log_to_id_file(truck_id, "stream_30sec_product_attach", {
            "truck_state": truck.get_state(),
            "affected_products": created_or_attached,
            "raw_data": data,
        })

    def _save_window_visualization(self, kafka_id: str, redis_msg: Message, 
                                   window_data: List[Message], 
                                   peaks_valleys: Dict[str, np.ndarray],
                                   x_data: np.ndarray, y_data: np.ndarray,
                                   timestamps: List[datetime],
                                   best_match: Optional[Tuple[float, Dict[str, Any]]] = None):
        """Сохраняет визуализацию временного окна с траекторией и найденными пиками."""
        try:
            # Создаем директорию для конкретного ID
            id_window_dir = os.path.join(self.windows_dir, kafka_id)
            os.makedirs(id_window_dir, exist_ok=True)
            
            # Получаем номер кадра
            if kafka_id not in self.frame_counters:
                self.frame_counters[kafka_id] = 0
            frame_num = self.frame_counters[kafka_id]
            self.frame_counters[kafka_id] += 1
            
            # Создаем фигуру с одним графиком траектории
            fig, ax = plt.subplots(figsize=(12, 10))
            fig.suptitle(f'ID: {kafka_id} | Кадр: {frame_num} | Redis Time: {redis_msg.timestamp.strftime("%H:%M:%S.%f")[:-3]}', 
                        fontsize=14, fontweight='bold')
            
            # Строим траекторию
            ax.plot(x_data, y_data, 'b-', linewidth=2, label='Траектория Kafka', alpha=0.7, zorder=1)
            
            # Отмечаем начало и конец траектории
            ax.plot(x_data[0], y_data[0], 'go', markersize=12, label='Начало окна', zorder=3)
            ax.plot(x_data[-1], y_data[-1], 'ro', markersize=12, label='Конец окна', zorder=3)
            
            # Собираем все пики и впадины
            peak_labels = {
                'peaks_x': ('Пики X', 'g^'),
                'valleys_x': ('Впадины X', 'gv'),
                'peaks_y': ('Пики Y', 'm^'),
                'valleys_y': ('Впадины Y', 'mv')
            }
            
            for peak_type, (label, marker) in peak_labels.items():
                indices = peaks_valleys[peak_type]
                if len(indices) > 0:
                    x_coords = x_data[indices]
                    y_coords = y_data[indices]
                    ax.plot(x_coords, y_coords, marker, markersize=10, 
                           label=label, zorder=4, markeredgecolor='black', markeredgewidth=0.5)
                    
                    # Добавляем подписи времени для пиков
                    for idx in indices:
                        time_str = timestamps[idx].strftime('%H:%M:%S')
                        ax.annotate(time_str, (x_data[idx], y_data[idx]), 
                                   textcoords="offset points", xytext=(5, 5),
                                   fontsize=7, rotation=45, alpha=0.7)
            
            # Отмечаем лучшее совпадение, если найдено
            if best_match:
                _, best_data = best_match
                best_coord = best_data['point']
                ax.plot(best_coord[0], best_coord[1], 'o', color='gold', markersize=18,
                       label='Лучшее совпадение', zorder=6, 
                       markeredgecolor='black', markeredgewidth=2)
            
            ax.set_xlabel('X координата', fontsize=12)
            ax.set_ylabel('Y координата', fontsize=12)
            ax.legend(loc='best', fontsize=9)
            ax.grid(True, alpha=0.3)
            ax.set_aspect('equal', adjustable='box')
            
            # Добавляем информацию о совпадении
            if best_match:
                distance, best_data = best_match
                info_text = (f"Совпадение найдено!\n"
                           f"Тип: {best_data['type']}\n"
                           f"Расстояние: {distance:.2f}м\n"
                           f"Разница времени: {best_data['time_diff']:.2f}с\n"
                           f"Время: {best_data['timestamp'].strftime('%H:%M:%S.%f')[:-3]}")
                fig.text(0.02, 0.02, info_text, fontsize=10, 
                        bbox=dict(boxstyle='round', facecolor='lightgreen', alpha=0.8))
            else:
                info_text = (f"Совпадение не найдено\n"
                           f"Размер окна: {len(window_data)} точек\n"
                           f"Найдено пиков: {sum(len(v) for v in peaks_valleys.values())}")
                fig.text(0.02, 0.02, info_text, fontsize=10,
                        bbox=dict(boxstyle='round', facecolor='lightcoral', alpha=0.8))
            
            plt.tight_layout(rect=[0, 0.08, 1, 0.96])
            
            # Сохраняем
            filename = f"frame_{frame_num:04d}.png"
            filepath = os.path.join(id_window_dir, filename)
            plt.savefig(filepath, dpi=100, bbox_inches='tight')
            plt.close(fig)
            
            logger.debug(f"Сохранена визуализация траектории для ID {kafka_id}: {filename}")
            
        except Exception as e:
            logger.error(f"Ошибка при сохранении визуализации для ID {kafka_id}: {e}", exc_info=True)

    # --- Логирование ---
    def _get_id_logger(self, msg_id: str) -> logging.Logger:
        """Получает или создает логгер для конкретного ID."""
        if msg_id not in self.id_loggers:
            id_logger = logging.getLogger(f"id.{msg_id}")
            id_logger.setLevel(logging.INFO)
            id_logger.propagate = False
            
            log_file_path = os.path.join(self.id_log_dir, f"{msg_id}.log")
            file_handler = logging.FileHandler(log_file_path, mode='a', encoding='utf-8')
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            file_handler.setFormatter(formatter)
            
            id_logger.addHandler(file_handler)
            self.id_loggers[msg_id] = id_logger
            logger.info(f"Создан логгер для ID: {msg_id}")
        
        return self.id_loggers[msg_id]

    def _log_to_id_file(self, msg_id: str, step: str, data: dict):
        """Логирует этап обработки в файл, именованный по ID."""
        id_logger = self._get_id_logger(msg_id)
        log_message = f"[STEP: {step}] {data}"
        id_logger.info(log_message)

    def _cleanup_old_kafka_messages(self):
        """
        Удаляет старые сообщения из всех очередей Kafka.
        Удаляются сообщения, которые старее самого старого сообщения в Redis 
        более чем на cleanup_threshold_seconds секунд.
        """
        try:
            # Получаем очередь Redis
            redis_queue = self.queue_manager.simple_queues.get(self.redis_source_name)
            if not redis_queue or len(redis_queue) == 0:
                return 0  # Нет сообщений Redis для определения временной метки
            
            # Находим самое старое сообщение в Redis
            oldest_redis_msg = redis_queue[0]  # Первое сообщение в очереди (самое старое)
            cutoff_time = oldest_redis_msg.timestamp - timedelta(seconds=self.cleanup_threshold_seconds)
            
            # Получаем словарь очередей Kafka
            dict_queues = self.queue_manager.dict_queues.get(self.kafka_source_name, {})
            if not dict_queues:
                return 0
                
            total_removed = 0
            queues_cleaned = 0
            
            # Проходим по всем очередям Kafka
            for kafka_id, kafka_queue in dict_queues.items():
                if not kafka_queue:
                    continue
                    
                removed_from_queue = 0
                # Удаляем все сообщения старше cutoff_time
                while kafka_queue and kafka_queue[0].timestamp < cutoff_time:
                    kafka_queue.popleft()
                    removed_from_queue += 1
                    
                if removed_from_queue > 0:
                    total_removed += removed_from_queue
                    queues_cleaned += 1
                    
                    # Логируем очистку для этого ID
                    self._log_to_id_file(kafka_id, "periodic_cleanup", {
                        "removed_messages": removed_from_queue,
                        "cutoff_time": str(cutoff_time),
                        "oldest_redis_time": str(oldest_redis_msg.timestamp),
                        "remaining_messages": len(kafka_queue)
                    })
            
            if total_removed > 0:
                logger.info(f"🔄 Очистка Kafka: удалено {total_removed} сообщений из {queues_cleaned} очередей (старше {cutoff_time.strftime('%H:%M:%S')})")
            
            return total_removed
            
        except Exception as e:
            logger.error(f"Ошибка при очистке старых сообщений Kafka: {e}", exc_info=True)
            return 0

    async def process_messages(self):
        """Основной цикл обработки сообщений."""
        self._running = True
        logger.info(f"Процессор запущен. Ожидание сообщений из {self.redis_source_name}...")
        # Счетчик для периодической очистки
        cleanup_interval = 10  # Выполнять очистку каждые 10 итераций
        while self._running:
            try:
                # 0. Однократная обработка сообщений из redis_chanel_1per30sec
                self._drain_stream_30_once()

                # 1. Получаем очередь и самое старое сообщение Redis
                redis_msg = self._peek_oldest_redis_message()
                if redis_msg is None:
                    await asyncio.sleep(0.1)
                    continue

                # 2. Получаем релевантные очереди Kafka
                relevant_queues = self._find_relevant_queues(redis_msg)
                if not relevant_queues:
                    logger.debug(f"Для сообщения Redis от {redis_msg.timestamp} нет релевантных очередей Kafka. Ожидание...")
                    await asyncio.sleep(1)
                    continue

                # 3. Ищем лучшее сопоставление во всех релевантных очередях
                best_match, winning_kafka_id, any_peaks_found = self._evaluate_best_match(redis_msg, relevant_queues)

                # 4. Обработка результата сопоставления
                if best_match and winning_kafka_id:
                    await self._handle_match(best_match, winning_kafka_id, redis_msg)
                elif not any_peaks_found and relevant_queues:
                    logger.warning(f"⚠ Не найдено ни одного пика во всех релевантных очередях для Redis({redis_msg.timestamp}). Удаляем сообщение.")
                    self.queue_manager.simple_queues.get(self.redis_source_name).popleft()

                # 5. Периодические операции
                self.cleanup_counter += 1
                if self.cleanup_counter >= cleanup_interval:
                    _ = self._cleanup_old_kafka_messages()
                    self.cleanup_counter = 0

                await asyncio.sleep(0.01)

            except asyncio.CancelledError:
                logger.info("Процессор остановлен по CancelledError")
                break
            except Exception as e:
                logger.error(f"Ошибка в цикле обработки: {e}", exc_info=True)
                await asyncio.sleep(1)

    def _find_relevant_queues(self, redis_msg: Message) -> Dict[str, 'deque']:
        """
        Этап 1: Поиск релевантных очередей.
        Условие: самое новое сообщение в очереди Kafka должно быть старше
        сообщения Redis как минимум на 3 секунды.
        """
        relevant = {}
        dict_queues = self.queue_manager.dict_queues.get(self.kafka_source_name, {})
        
        if not dict_queues:
            logger.debug(f"Словарь очередей {self.kafka_source_name} пуст или не найден")
            return relevant
        
        for kafka_id, kafka_queue in dict_queues.items():
            if not kafka_queue:
                continue
            
            # Проверяем разницу между Redis и самым НОВЫМ сообщением в Kafka
            newest_kafka_msg = kafka_queue[-1]  # Самое новое
            time_diff = (newest_kafka_msg.timestamp - redis_msg.timestamp).total_seconds()
            
            # Логируем проверку релевантности для каждого ID
            self._log_to_id_file(kafka_id, "relevance_check", {
                "redis_timestamp": str(redis_msg.timestamp),
                "newest_kafka_timestamp": str(newest_kafka_msg.timestamp),
                "time_diff_sec": time_diff,
                "is_relevant": time_diff > 3
            })
            
            if time_diff > 3:
                relevant[kafka_id] = kafka_queue
        
        if relevant:
            logger.info(f"Найдено {len(relevant)} релевантных очередей для Redis({redis_msg.timestamp}): {list(relevant.keys())}")
        
        return relevant

    def _find_best_match_in_window(self, redis_msg: Message, kafka_queue: 'deque', kafka_id: str) -> Optional[Tuple[float, Dict[str, Any]]]:
        """
        Этапы 2 и 3: Формирование окна, поиск пиков и фильтрация.
        """
        redis_coord = redis_msg.data.get("coord")
        if not redis_coord or len(redis_coord) != 2:
            self._log_to_id_file(kafka_id, "invalid_redis_coord", {
                "redis_coord": redis_coord
            })
            return None

        # Определяем временное окно
        window_start = redis_msg.timestamp - timedelta(seconds=10)
        window_end = redis_msg.timestamp + timedelta(seconds=3)

        # Извлекаем данные для окна
        window_data = [msg for msg in kafka_queue if window_start <= msg.timestamp <= window_end]
        
        if not window_data:
            self._log_to_id_file(kafka_id, "window_formation", {
                "window_start": str(window_start),
                "window_end": str(window_end),
                "window_size": 0,
                "status": "no_data_in_window"
            })
            return None

        # Логируем успешное формирование окна
        self._log_to_id_file(kafka_id, "window_formation", {
            "window_start": str(window_start),
            "window_end": str(window_end),
            "window_size": len(window_data),
            "status": "success"
        })

        # Подготовка данных для поиска пиков
        coords = [msg.data.get("coord") for msg in window_data if msg.data.get("coord")]
        if len(coords) < 2:  # Нужно минимум 2 точки для поиска пиков
            self._log_to_id_file(kafka_id, "peak_detection", {
                "status": "insufficient_data",
                "coords_count": len(coords)
            })
            return None
            
        x_data = np.array([c[0] for c in coords])
        y_data = np.array([c[1] for c in coords])
        timestamps = [msg.timestamp for msg in window_data if msg.data.get("coord")]
        section_names = [msg.data.get("section_name") for msg in window_data if msg.data.get("coord")]

        # Поиск пиков и впадин
        peaks_valleys = self._find_peaks_and_valleys(x_data, y_data)
        
        # Логируем результаты поиска пиков
        peaks_valleys_counts = {k: len(v) for k, v in peaks_valleys.items()}
        self._log_to_id_file(kafka_id, "peak_detection", {
            "status": "completed",
            "peaks_valleys_counts": peaks_valleys_counts,
            "total_points": sum(peaks_valleys_counts.values())
        })
        
        # Собираем все найденные точки (пики и впадины)
        all_points = []
        for type_name, indices in peaks_valleys.items():
            for idx in indices:
                if idx < len(timestamps):
                    point = {
                        "timestamp": timestamps[idx],
                        "coord": coords[idx],
                        "type": type_name,
                        "section_name": section_names[idx] if idx < len(section_names) else None
                    }
                    all_points.append(point)
        
        if not all_points:
            # Сохраняем визуализацию даже если пики не найдены
            self._save_window_visualization(kafka_id, redis_msg, window_data, 
                                           peaks_valleys, x_data, y_data, timestamps, None)
            
            self._log_to_id_file(kafka_id, "filtering_and_matching", {
                "status": "no_peaks_found"
            })
            return None

        # Фильтрация по времени и поиск ближайшего по расстоянию
        best_point_data = None
        min_distance = float('inf')
        candidates = []

        for point in all_points:
            time_diff = abs((point['timestamp'] - redis_msg.timestamp).total_seconds())
            dist = math.dist(point['coord'], redis_coord)
            
            candidate_info = {
                "type": point['type'],
                "timestamp": str(point['timestamp']),
                "coord": point['coord'],
                "section_name": point.get('section_name'),
                "time_diff_sec": time_diff,
                "distance": dist,
                "time_filter_passed": time_diff <= 5
            }
            candidates.append(candidate_info)
            
            if time_diff <= 5:
                if dist < min_distance:
                    min_distance = dist
                    best_point_data = {
                        "timestamp": point['timestamp'],
                        "point": point['coord'],
                        "type": point['type'],
                        "time_diff": time_diff,
                        "section_name": point.get('section_name')
                    }
        
        # Сохраняем визуализацию окна с результатами
        best_match_for_viz = (min_distance, best_point_data) if best_point_data else None
        self._save_window_visualization(kafka_id, redis_msg, window_data, 
                                       peaks_valleys, x_data, y_data, timestamps, best_match_for_viz)
        
        # Логируем результаты фильтрации
        self._log_to_id_file(kafka_id, "filtering_and_matching", {
            "total_candidates": len(candidates),
            "candidates": candidates,
            "best_match_found": best_point_data is not None,
            "best_distance": min_distance if best_point_data else None
        })
        
        if best_point_data:
            return (min_distance, best_point_data)
        
        return None

    def _cleanup_kafka_queue(self, kafka_id: str, center_time: datetime):
        """Удаляет из очереди Kafka все сообщения, которые старше center_time."""
        dict_queues = self.queue_manager.dict_queues.get(self.kafka_source_name, {})
        if kafka_id not in dict_queues:
            return
        
        kafka_queue = dict_queues[kafka_id]
        removed_count = 0
        while kafka_queue and kafka_queue[0].timestamp < center_time:
            kafka_queue.popleft()
            removed_count += 1
        
        if removed_count > 0:
            self._log_to_id_file(kafka_id, "queue_cleanup", {
                "removed_messages": removed_count,
                "center_time": str(center_time),
                "remaining_messages": len(kafka_queue)
            })
            logger.debug(f"Очистка очереди Kafka для ID {kafka_id}: удалено {removed_count} сообщений.")

    @staticmethod
    def _find_peaks_and_valleys(x_data: np.ndarray, y_data: np.ndarray) -> Dict[str, np.ndarray]:
        """Находит пики и впадины в последовательностях координат."""
        # Параметры можно вынести в конфиг, если потребуется
        peaks_x, _ = find_peaks(x_data, height=0.1, distance=100)
        peaks_y, _ = find_peaks(y_data, height=0.1, distance=100)
        valleys_x, _ = find_peaks(-x_data, height=0.1, distance=100)
        valleys_y, _ = find_peaks(-y_data, height=0.1, distance=100)
        
        return {
            'peaks_x': peaks_x,
            'valleys_x': valleys_x,
            'peaks_y': peaks_y,
            'valleys_y': valleys_y
        }

    def stop(self):
        """Останавливает процессор."""
        self._running = False
        logger.info("DataProcessor остановлен")

    # === Decomposition helpers ===
    def _drain_stream_30_once(self) -> None:
        """Обрабатывает одно сообщение из очереди redis_chanel_1per30sec, если есть."""
        redis_30_queue = self.queue_manager.simple_queues.get("redis_chanel_1per30sec")
        if redis_30_queue and len(redis_30_queue) > 0:
            msg_30 = redis_30_queue.popleft()
            try:
                self._process_stream_30sec_message(msg_30)
            except Exception as e:
                logger.error("Ошибка при обработке сообщения 30sec: %s", e, exc_info=True)

    def _peek_oldest_redis_message(self) -> Optional[Message]:
        """Возвращает самое старое сообщение из redis_chanel_1per3sec без удаления."""
        redis_queue = self.queue_manager.simple_queues.get(self.redis_source_name)
        if not redis_queue or len(redis_queue) == 0:
            return None
        return redis_queue[0]

    def _evaluate_best_match(self, redis_msg: Message, relevant_queues: Dict[str, 'deque']) -> Tuple[Optional[Tuple[float, Dict[str, Any]]], Optional[str], bool]:
        """Ищет лучшее совпадение во всех релевантных очередях Kafka."""
        best_match: Optional[Tuple[float, Dict[str, Any]]] = None
        winning_kafka_id: Optional[str] = None
        any_peaks_found = False

        for kafka_id, kafka_queue in relevant_queues.items():
            self._log_to_id_file(kafka_id, "window_search_start", {
                "redis_timestamp": str(redis_msg.timestamp),
                "redis_coord": redis_msg.data.get("coord"),
                "queue_size": len(kafka_queue)
            })

            match_candidate = self._find_best_match_in_window(redis_msg, kafka_queue, kafka_id)
            if match_candidate:
                distance, best_point_data = match_candidate
                any_peaks_found = True
                self._log_to_id_file(kafka_id, "match_candidate_found", {
                    "distance": distance,
                    "time_diff_sec": best_point_data['time_diff'],
                    "point": best_point_data['point'],
                    "type": best_point_data['type']
                })
                if best_match is None or distance < best_match[0]:
                    best_match = match_candidate
                    winning_kafka_id = kafka_id
            else:
                self._log_to_id_file(kafka_id, "no_match_found", {
                    "redis_timestamp": str(redis_msg.timestamp)
                })

        return best_match, winning_kafka_id, any_peaks_found

    async def _handle_match(self, best_match: Tuple[float, Dict[str, Any]], winning_kafka_id: str, redis_msg: Message) -> None:
        """Обрабатывает найденное совпадение, включая доменную логику Truck/Area."""
        distance, best_point_data = best_match
        final_result = {
            "status": "matched",
            "redis": {
                "timestamp": str(redis_msg.timestamp),
                "id": redis_msg.id,
                "coord": redis_msg.data.get("coord"),
                "new_state": redis_msg.data.get("new_state")
            },
            "kafka": {
                "id": winning_kafka_id,
                "point": best_point_data['point'],
                "timestamp": str(best_point_data['timestamp']),
                "type": best_point_data['type'],
                "section_name": best_point_data.get('section_name')
            },
            "metrics": {
                "time_diff_sec": best_point_data['time_diff'],
                "distance": distance
            }
        }
        self._log_to_id_file(winning_kafka_id, "final_result", final_result)
        logger.info(f"✓ Найдено сопоставление: Redis({redis_msg.timestamp}) -> Kafka(ID: {winning_kafka_id}, dist: {distance:.2f}m)")

        # Доменная логика Truck/Area
        new_state_raw = redis_msg.data.get("new_state")
        try:
            new_state_val = int(new_state_raw) if new_state_raw is not None else None
        except (TypeError, ValueError):
            new_state_val = None

        truck = self.trucks_registry.get(winning_kafka_id)
        if truck is None:
            truck = Truck.create(
                id=winning_kafka_id,
                actual_state=new_state_val if new_state_val is not None else 0,
            )
            self.trucks_registry[winning_kafka_id] = truck
            self._log_to_id_file(winning_kafka_id, "truck_created_on_match", {
                "truck_state": truck.get_state()
            })
        else:
            prev_state = truck.actual_state
            truck.update(actual_state=new_state_val if new_state_val is not None else truck.actual_state)
            self._log_to_id_file(winning_kafka_id, "truck_state_updated_on_match", {
                "previous_state": prev_state,
                "new_state": truck.actual_state,
                "truck_state": truck.get_state()
            })

            if prev_state == truck.actual_state:
                truck.last_modified_at = best_point_data.get('timestamp', redis_msg.timestamp)
                self._log_to_id_file(winning_kafka_id, "truck_timestamp_refreshed_same_state", {
                    "state": truck.actual_state,
                    "last_modified_at": str(truck.last_modified_at)
                })

            if prev_state == 1 and truck.actual_state == 0:
                await self._move_truck_products_to_area(winning_kafka_id, best_point_data)
            elif prev_state == 0 and truck.actual_state == 1:
                await self._take_mc_from_area_to_truck(winning_kafka_id, best_point_data, redis_msg)

        # Очистка очередей после успешного сопоставления
        self.queue_manager.simple_queues.get(self.redis_source_name).popleft()
        self._cleanup_kafka_queue(winning_kafka_id, redis_msg.timestamp)

    async def _move_truck_products_to_area(self, winning_kafka_id: str, best_point_data: Dict[str, Any]) -> None:
        section_name = best_point_data.get('section_name')
        if section_name is None:
            self._log_to_id_file(winning_kafka_id, "area_move_skipped_no_section", {"reason": "section_name is None"})
            return
        area = self.areas_registry.get(section_name)
        created_area = False
        if area is None:
            area = Area()
            self.areas_registry[section_name] = area
            created_area = True
            self._log_to_id_file(winning_kafka_id, "area_created", {"section_name": section_name})
        truck = self.trucks_registry.get(winning_kafka_id)
        products_to_move = list(truck.products)
        moved: List[str] = []
        already_in_area: List[str] = []
        removed_from_truck_only: List[str] = []
        for product in products_to_move:
            exists_in_area = any(p.internal_id == product.internal_id for p in area.products)
            if exists_in_area:
                already_in_area.append(product.internal_id)
                removed_from_truck_only.append(product.internal_id)
            else:
                area.add_product(product)
                moved.append(product.internal_id)
            truck.products = [p for p in truck.products if p.internal_id != product.internal_id]
        truck.last_modified_at = datetime.now()
        self._log_to_id_file(winning_kafka_id, "area_move_completed", {
            "section_name": section_name,
            "created_area": created_area,
            "moved_products": moved,
            "already_in_area": already_in_area,
            "products_in_area": [p.internal_id for p in area.products],
            "removed_from_truck_only": removed_from_truck_only,
            "truck_state_after": truck.get_state()
        })

    async def _take_mc_from_area_to_truck(self, winning_kafka_id: str, best_point_data: Dict[str, Any], redis_msg: Message) -> None:
        section_name = best_point_data.get('section_name')
        if section_name is None:
            self._log_to_id_file(winning_kafka_id, "area_takeback_skipped_no_section", {"reason": "section_name is None"})
            return
        area = self.areas_registry.get(section_name)
        if area is None:
            self._log_to_id_file(winning_kafka_id, "area_takeback_area_not_found", {"section_name": section_name})
            return
        truck = self.trucks_registry.get(winning_kafka_id)
        mc_product = None
        for p in area.products:
            if p.type == 'mc':
                mc_product = p
                break
        if mc_product is None:
            self._log_to_id_file(winning_kafka_id, "area_takeback_no_mc_product", {
                "section_name": section_name,
                "area_products_count": len(area.products)
            })
            return
        exists_in_truck = any(tp.internal_id == mc_product.internal_id for tp in truck.products)
        if not exists_in_truck:
            truck.products.append(mc_product)
        truck.last_modified_at = redis_msg.timestamp
        area.delete_product(mc_product)
        self._log_to_id_file(winning_kafka_id, "area_takeback_completed", {
            "section_name": section_name,
            "moved_internal_id": mc_product.internal_id,
            "truck_state_after": truck.get_state()
        })