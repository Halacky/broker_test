# file path: /home/kirill/projects/folium/broker_stand/src/project/main_pp.py

from queue_manager import QueueManager
from data_collector import DataCollector
from data_proccesor import DataProcessor
from data_source import RedisStreamSource, KafkaTopicSource
from typing import Any, Dict, Optional, List
import asyncio
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class Pipeline:
    """Главный класс пайплайна"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.queue_manager = QueueManager(
            max_queue_size=config.get('max_queue_size', 1000)
        )
        self.collector = DataCollector(self.queue_manager)
        self.processor = DataProcessor(self.queue_manager)
        self.tasks: List[asyncio.Task] = []
    
    def setup_sources(self):
        """Настройка всех источников данных"""
        redis_config = self.config.get('redis', {})
        kafka_config = self.config.get('kafka', {})
        
        logger.info("=== Настройка источников данных ===")
        
        # Redis Sources
        logger.info("Настройка Redis источников...")
        redis_1 = RedisStreamSource(
            "redis_chanel_1per3sec",
            stream_key=redis_config.get('stream_1', 'stream_1per3sec'),
            redis_url=redis_config.get('url', 'redis://localhost:6379'),
            read_from_start=False  # Читать только новые сообщения
        )
        
        redis_2 = RedisStreamSource(
            "redis_chanel_1per30sec",
            stream_key=redis_config.get('stream_2', 'stream_1per30sec'),
            redis_url=redis_config.get('url', 'redis://localhost:6379'),
            read_from_start=False  # Читать только новые сообщения
        )
        
        # Kafka Sources
        logger.info("Настройка Kafka источников...")
        kafka_fast = KafkaTopicSource(
            "kafka_topic_1per0_25sec",
            topic=kafka_config.get('topic_fast', 'topic_0_25sec'),
            bootstrap_servers=kafka_config.get('bootstrap_servers', 'localhost:9092'),
            extract_id=True,
            id_field=kafka_config.get('id_field', 'id')
        )
        
        kafka_1 = KafkaTopicSource(
            "kafka_topic_1per30sec",
            topic=kafka_config.get('topic_1', 'topic_30sec'),
            bootstrap_servers=kafka_config.get('bootstrap_servers', 'localhost:9092')
        )
        
        kafka_2 = KafkaTopicSource(
            "kafka_topic_1per25min",
            topic=kafka_config.get('topic_2', 'topic_25min'),
            bootstrap_servers=kafka_config.get('bootstrap_servers', 'localhost:9092')
        )
        
        kafka_3 = KafkaTopicSource(
            "kafka_topic_1per30min",
            topic=kafka_config.get('topic_3', 'topic_30min'),
            bootstrap_servers=kafka_config.get('bootstrap_servers', 'localhost:9092')
        )
        
        # Регистрация источников
        logger.info("Регистрация источников в коллекторе...")
        self.collector.register_source(redis_1, use_dict_queue=False)
        self.collector.register_source(redis_2, use_dict_queue=False)
        self.collector.register_source(kafka_fast, use_dict_queue=True)  # С ID
        self.collector.register_source(kafka_1, use_dict_queue=False)
        self.collector.register_source(kafka_2, use_dict_queue=False)
        self.collector.register_source(kafka_3, use_dict_queue=False)
        
        logger.info(f"Всего зарегистрировано источников: {len(self.collector.sources)}")
        logger.info("=== Настройка источников завершена ===")
    
    async def monitoring_task(self):
        """Задача мониторинга размеров очередей"""
        try:
            while True:
                await asyncio.sleep(10)
                sizes = self.queue_manager.get_queue_sizes()
                
                # Форматируем вывод для лучшей читаемости
                logger.info("=" * 60)
                logger.info("МОНИТОРИНГ ОЧЕРЕДЕЙ:")
                
                # Простые очереди
                simple_queues = {k: v for k, v in sizes.items() if not k.endswith('_total') and not k.endswith('_ids')}
                if simple_queues:
                    logger.info("Простые очереди:")
                    for name, size in simple_queues.items():
                        logger.info(f"  {name}: {size} сообщений")
                
                # Словари очередей
                dict_queues = {k: v for k, v in sizes.items() if k.endswith('_total') or k.endswith('_ids')}
                if dict_queues:
                    logger.info("Словари очередей:")
                    for name, size in dict_queues.items():
                        logger.info(f"  {name}: {size}")
                
                logger.info("=" * 60)
                
        except asyncio.CancelledError:
            logger.info("Мониторинг остановлен")
    
    async def run(self):
        """Запуск пайплайна"""
        logger.info("=" * 60)
        logger.info("ЗАПУСК ПАЙПЛАЙНА")
        logger.info("=" * 60)
        
        self.setup_sources()
        
        try:
            # Создаем задачи для каждого источника
            logger.info("Создание задач сбора данных...")
            for source_name in self.collector.sources.keys():
                task = asyncio.create_task(
                    self.collector.collect_from_source(source_name),
                    name=f"collector_{source_name}"
                )
                self.tasks.append(task)
                logger.info(f"  ✓ Задача для '{source_name}' создана")
            
            # Задача обработки
            logger.info("Создание задачи обработки...")
            process_task = asyncio.create_task(
                self.processor.process_messages(),
                name="processor"
            )
            self.tasks.append(process_task)
            logger.info("  ✓ Задача обработки создана")
            
            # Задача мониторинга
            logger.info("Создание задачи мониторинга...")
            monitor_task = asyncio.create_task(
                self.monitoring_task(),
                name="monitor"
            )
            self.tasks.append(monitor_task)
            logger.info("  ✓ Задача мониторинга создана")
            
            logger.info("=" * 60)
            logger.info(f"ПАЙПЛАЙН ЗАПУЩЕН. Активных задач: {len(self.tasks)}")
            logger.info("=" * 60)
            
            # Ожидание всех задач
            await asyncio.gather(*self.tasks)
            
        except Exception as e:
            logger.error(f"Критическая ошибка в пайплайне: {e}", exc_info=True)
            raise
    
    async def shutdown(self):
        """Корректная остановка пайплайна"""
        logger.info("=" * 60)
        logger.info("ОСТАНОВКА ПАЙПЛАЙНА")
        logger.info("=" * 60)
        
        # Останавливаем процессор
        logger.info("Остановка процессора...")
        self.processor.stop()
        
        # Останавливаем источники
        logger.info("Остановка источников...")
        for source_name, source in self.collector.sources.items():
            source._running = False
            logger.info(f"  ✓ Источник '{source_name}' помечен для остановки")
        
        # Отменяем все задачи
        logger.info("Отмена задач...")
        for task in self.tasks:
            if not task.done():
                task.cancel()
                logger.info(f"  ✓ Задача '{task.get_name()}' отменена")
        
        # Ждем завершения всех задач
        logger.info("Ожидание завершения задач...")
        await asyncio.gather(*self.tasks, return_exceptions=True)
        
        logger.info("=" * 60)
        logger.info("ПАЙПЛАЙН ОСТАНОВЛЕН")
        logger.info("=" * 60)