from typing import Dict
from collections import deque
import logging

logger = logging.getLogger(__name__)

class QueueManager:
    """Менеджер очередей для хранения данных"""
    
    def __init__(self, max_queue_size: int = 10000):
        self.simple_queues: Dict[str, deque] = {}
        self.dict_queues: Dict[str, Dict[str, deque]] = {}
        self.max_queue_size = max_queue_size
    
    def create_simple_queue(self, source_name: str):
        """Создание простой очереди для источника"""
        if source_name not in self.simple_queues:
            self.simple_queues[source_name] = deque(maxlen=self.max_queue_size)
            logger.info(f"Создана очередь для {source_name}")
    
    def create_dict_queue(self, source_name: str):
        """Создание словаря очередей для источника"""
        if source_name not in self.dict_queues:
            self.dict_queues[source_name] = {}
            logger.info(f"Создан словарь очередей для {source_name}")
    
    def add_to_simple_queue(self, source_name: str, message):
        """Добавление сообщения в простую очередь"""
        if source_name not in self.simple_queues:
            self.create_simple_queue(source_name)
        self.simple_queues[source_name].append(message)
    
    def add_to_dict_queue(self, source_name: str, message):
        """Добавление сообщения в словарь очередей по ID"""
        if source_name not in self.dict_queues:
            self.create_dict_queue(source_name)
        
        msg_id = message.id
        if msg_id is None:
            logger.warning(f"Сообщение без ID из {source_name}, пропускаем")
            return
        
        if msg_id not in self.dict_queues[source_name]:
            self.dict_queues[source_name][msg_id] = deque(maxlen=self.max_queue_size)
        
        self.dict_queues[source_name][msg_id].append(message)
    
    def get_queue_sizes(self) -> Dict[str, int]:
        """Получение размеров всех очередей для мониторинга"""
        sizes = {}
        for name, queue in self.simple_queues.items():
            sizes[name] = len(queue)
        
        for name, dict_queue in self.dict_queues.items():
            total = sum(len(q) for q in dict_queue.values())
            sizes[f"{name}_total"] = total
            sizes[f"{name}_ids"] = len(dict_queue)
        
        return sizes
