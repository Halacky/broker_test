# file path: /home/kirill/projects/folium/broker_stand/src/project/data_collector.py

from typing import Dict
import asyncio
import logging
import os

from queue_manager import QueueManager
from data_source import DataSource

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataCollector:
    
    def __init__(self, queue_manager: QueueManager):
        self.queue_manager = queue_manager
        self.sources: Dict[str, DataSource] = {}
        self.source_configs: Dict[str, bool] = {}
        self.source_loggers: Dict[str, logging.Logger] = {}
        
        # Создаем директорию для логов, если ее нет
        self.log_dir = "logs"
        os.makedirs(self.log_dir, exist_ok=True)

    def _setup_source_logger(self, source_name: str):
        """Настраивает отдельный файловый логгер для источника."""
        if source_name in self.source_loggers:
            return

        source_logger = logging.getLogger(f"source.{source_name}")
        source_logger.setLevel(logging.INFO)
        
        # Предотвращаем дублирование логов в родительские логгеры
        source_logger.propagate = False

        # Создаем обработчик для записи в файл
        log_file_path = os.path.join(self.log_dir, f"{source_name}.log")
        file_handler = logging.FileHandler(log_file_path, mode='a', encoding='utf-8')
        
        # Формат для логов в файле
        formatter = logging.Formatter('%(asctime)s - %(message)s')
        file_handler.setFormatter(formatter)
        
        source_logger.addHandler(file_handler)
        self.source_loggers[source_name] = source_logger
        logger.info(f"Настроен логгер для источника '{source_name}' в файл {log_file_path}")

    def register_source(self, source: DataSource, use_dict_queue: bool = False):
        """Регистрирует источник данных."""
        self.sources[source.name] = source
        self.source_configs[source.name] = use_dict_queue
        
        # Настраиваем логгер для нового источника
        self._setup_source_logger(source.name)
        
        if use_dict_queue:
            self.queue_manager.create_dict_queue(source.name)
            logger.info(f"Источник '{source.name}' зарегистрирован с dict_queue")
        else:
            self.queue_manager.create_simple_queue(source.name)
            logger.info(f"Источник '{source.name}' зарегистрирован с simple_queue")
    
    async def collect_from_source(self, source_name: str):
        """Собирает данные из указанного источника."""
        if source_name not in self.sources:
            logger.error(f"Источник '{source_name}' не зарегистрирован!")
            return
            
        source = self.sources[source_name]
        use_dict_queue = self.source_configs[source_name]
        source_logger = self.source_loggers.get(source_name)
        
        logger.info(f"Начинаем сбор данных из '{source_name}'")
        
        try:
            await source.connect()
        except Exception as e:
            logger.error(f"Не удалось подключиться к '{source_name}': {e}")
            return
        
        message_count = 0
        
        try:
            while source._running:
                try:
                    message = await source.consume()
                    
                    if message is None:
                        await asyncio.sleep(0.01)
                        continue
                    
                    message_count += 1
                    
                    # Записываем исходные данные в отдельный файл
                    # Формат теперь чистый без дублирования
                    if source_logger:
                        log_data = {
                            "timestamp": str(message.timestamp),
                            "id": message.id if message.id else "N/A",
                            "data": message.data
                        }
                        source_logger.info(f"Raw data: {log_data}")
                    
                    # Добавляем в соответствующую очередь
                    if use_dict_queue:
                        self.queue_manager.add_to_dict_queue(source_name, message)
                        if message_count % 100 == 0:  # Логируем каждое 100-е сообщение
                            logger.info(f"{source_name}: получено {message_count} сообщений (ID: {message.id})")
                    else:
                        self.queue_manager.add_to_simple_queue(source_name, message)
                        if message_count % 10 == 0:  # Логируем каждое 10-е сообщение
                            logger.info(f"{source_name}: получено {message_count} сообщений")
                    
                    logger.debug(f"Получено сообщение из {source_name}: {message.timestamp}")
                    
                except asyncio.CancelledError:
                    logger.info(f"Сбор данных из {source_name} отменен")
                    break
                except Exception as e:
                    logger.error(f"Ошибка при сборе из {source_name}: {e}", exc_info=True)
                    await asyncio.sleep(1)
        finally:
            await source.disconnect()
            logger.info(f"Сбор данных из '{source_name}' завершен. Всего получено: {message_count} сообщений")