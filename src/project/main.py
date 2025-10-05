# from abc import ABC, abstractmethod
# from collections import deque
# from dataclasses import dataclass
# from datetime import datetime
# import json

# # Импорты для Redis и Kafka
# import redis.asyncio as aioredis
# from aiokafka import AIOKafkaConsumer

import logging
from main_pp import Pipeline
import asyncio

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def main():
    """Точка входа"""
    
    # Конфигурация
    config = {
        'max_queue_size': 1000,
        'redis': {
            'url': 'redis://localhost:6379',
            'stream_1': 'stream_1per3sec',
            'stream_2': 'stream_1per30sec',
        },
        'kafka': {
            'bootstrap_servers': 'localhost:9092',
            'topic_fast': 'topic_0_25sec',
            'topic_1': 'topic_30sec',
            'topic_2': 'topic_25min',
            'topic_3': 'topic_30min',
            'id_field': 'id'
        }
    }
    
    pipeline = Pipeline(config)
    
    try:
        await pipeline.run()
    except KeyboardInterrupt:
        logger.info("Получен сигнал прерывания")
    finally:
        await pipeline.shutdown()


if __name__ == "__main__":
    asyncio.run(main())