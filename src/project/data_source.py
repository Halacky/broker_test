# file path: /home/kirill/projects/folium/broker_stand/src/project/data_source.py

from abc import ABC, abstractmethod
from datetime import datetime
import json
import asyncio
import logging
import redis.asyncio as aioredis
from aiokafka import AIOKafkaConsumer
from dataclasses import dataclass
from typing import Any, Optional
import numpy as np

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class Message:
    """Стандартная структура данных для сообщений из всех источников."""
    timestamp: datetime
    data: dict  # Теперь всегда dict с полями, специфичными для источника
    source: str
    id: Optional[str] = None

class DataSource(ABC):
    
    def __init__(self, name: str):
        self.name = name
        self._running = False
    
    @abstractmethod
    async def connect(self):
        pass
    
    @abstractmethod
    async def disconnect(self):
        pass
    
    @abstractmethod
    async def consume(self):
        pass

class RedisStreamSource(DataSource):
    """
    Источник данных из Redis Stream.
    Ожидаемый формат сообщения:
    {
        "datetime": "2025-10-04T14:12:46.322861+07:00",
        "id": "1_4",
        "coord": [939.05, 91.5],
        "new_state": 1
    }
    """
    def __init__(
        self, 
        name: str, 
        stream_key: str,
        redis_url: str = "redis://localhost:6379",
        block_ms: int = 1000,
        read_from_start: bool = False
    ):
        super().__init__(name)
        self.stream_key = stream_key
        self.redis_url = redis_url
        self.block_ms = block_ms
        self.client = None
        self.read_from_start = read_from_start
        # $ означает "читать только новые сообщения с текущего момента"
        # 0 означает "читать все сообщения с начала стрима"
        self.last_id = '0' if read_from_start else '$'
    
    async def connect(self):
        try:
            self.client = await aioredis.from_url(
                self.redis_url,
                encoding="utf-8",
                decode_responses=True
            )
            logger.info(f"Подключено к Redis для {self.name}")
            self._running = True
        except Exception as e:
            logger.error(f"Ошибка подключения к Redis для {self.name}: {e}")
            raise
    
    async def disconnect(self):
        if self.client:
            await self.client.close()
            self._running = False
            logger.info(f"Отключено от Redis для {self.name}")
    
    async def consume(self):
        if not self.client:
            raise RuntimeError(f"Redis client не подключен для {self.name}")
        
        try:
            response = await self.client.xread(
                {self.stream_key: self.last_id},
                block=self.block_ms,
                count=1
            )
            
            if response:
                stream_name, messages = response[0]
                if messages:
                    msg_id, msg_data = messages[0]
                    self.last_id = msg_id
                    
                    # Redis возвращает данные как dict с полем 'data', 
                    # которое содержит JSON-строку
                    if 'data' in msg_data and isinstance(msg_data['data'], str):
                        try:
                            parsed_data = json.loads(msg_data['data'])
                        except json.JSONDecodeError:
                            logger.error(f"Не удалось распарсить JSON из Redis: {msg_data['data']}")
                            return None
                    else:
                        # Если данные уже в нужном формате
                        parsed_data = msg_data
                    
                    # Извлекаем timestamp из самого сообщения
                    msg_ts_str = parsed_data.get('datetime')
                    try:
                        timestamp = datetime.fromisoformat(msg_ts_str)
                    except (TypeError, ValueError):
                        timestamp = datetime.now()
                        logger.warning(f"Не удалось распарсить datetime из Redis сообщения, используем текущее время")
                    
                    # Извлекаем ID из данных Redis (если есть)
                    redis_msg_id = parsed_data.get('id')
                    
                    # Формируем чистую структуру данных без дублирования ID и datetime
                    clean_data = {k: v for k, v in parsed_data.items() if k not in ('datetime', 'id')}
                    
                    return Message(
                        timestamp=timestamp,
                        data=clean_data,
                        source=self.name,
                        id=redis_msg_id
                    )
            
            return None
            
        except Exception as e:
            logger.error(f"Ошибка чтения из Redis {self.name}: {e}")
            raise


class KafkaTopicSource(DataSource):
    """
    Источник данных из Kafka Topic.
    Ожидаемый формат сообщения:
    {
        'datetime': '2025-10-04T14:12:47.177009+07:00',
        'id': '00200004',
        'section_name': 'section_35',
        'coord': [5.060862743968945, 7.55369572851146]
    }
    """
    def __init__(
        self,
        name: str,
        topic: str,
        bootstrap_servers: str = "localhost:9092",
        group_id: str = None,
        extract_id: bool = False,
        id_field: str = "id"
    ):
        super().__init__(name)
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id or f"{name}_group"
        self.extract_id = extract_id
        self.id_field = id_field
        self.consumer = None
    
    async def connect(self):
        try:
            self.consumer = AIOKafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                enable_auto_commit=True,
                auto_offset_reset='latest',
                value_deserializer=self._deserialize_kafka_message
            )
            await self.consumer.start()
            logger.info(f"Подключено к Kafka топику {self.topic} для {self.name}")
            self._running = True
        except Exception as e:
            logger.error(f"Ошибка подключения к Kafka для {self.name}: {e}")
            raise
    
    async def disconnect(self):
        if self.consumer:
            await self.consumer.stop()
            self._running = False
            logger.info(f"Отключено от Kafka для {self.name}")
    
    @staticmethod
    def _deserialize_kafka_message(value: bytes):
        """Десериализатор для Kafka с конвертацией numpy типов."""
        def convert_numpy(obj):
            if isinstance(obj, dict):
                return {k: convert_numpy(v) for k, v in obj.items()}
            if isinstance(obj, list):
                return [convert_numpy(i) for i in obj]
            if isinstance(obj, (np.float64, np.float32)):
                return float(obj)
            if isinstance(obj, (np.int64, np.int32)):
                return int(obj)
            return obj

        try:
            decoded = value.decode('utf-8')
            data = json.loads(decoded)
            return convert_numpy(data)
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logger.error(f"Не удалось десериализовать сообщение Kafka: {e}")
            return None
    
    async def consume(self):
        if not self.consumer:
            raise RuntimeError(f"Kafka consumer не подключен для {self.name}")
        
        try:
            msg = await asyncio.wait_for(
                self.consumer.__anext__(),
                timeout=1.0
            )
            
            data = msg.value
            if data is None:
                return None

            # Извлекаем ID из данных Kafka (не дублируем)
            msg_id = None
            if self.extract_id and isinstance(data, dict):
                msg_id = data.get(self.id_field)
            
            # Извлекаем timestamp из самого сообщения
            msg_ts_str = data.get('datetime')
            try:
                timestamp = datetime.fromisoformat(msg_ts_str)
            except (TypeError, ValueError):
                timestamp = datetime.fromtimestamp(msg.timestamp / 1000.0)
                logger.warning(f"Не удалось распарсить datetime из Kafka сообщения, используем метку Kafka")
            
            # Формируем чистую структуру данных без дублирования ID
            clean_data = {
                'coord': data.get('coord'),
                'section_name': data.get('section_name')
            }
            
            return Message(
                timestamp=timestamp,
                data=clean_data,
                source=self.name,
                id=msg_id
            )
            
        except asyncio.TimeoutError:
            return None
        except Exception as e:
            logger.error(f"Ошибка чтения из Kafka {self.name}: {e}")
            raise

    
    async def connect(self):
        try:
            self.client = await aioredis.from_url(
                self.redis_url,
                encoding="utf-8",
                decode_responses=True
            )
            logger.info(f"Подключено к Redis для {self.name}")
            self._running = True
        except Exception as e:
            logger.error(f"Ошибка подключения к Redis для {self.name}: {e}")
            raise
    
    async def disconnect(self):
        if self.client:
            await self.client.close()
            self._running = False
            logger.info(f"Отключено от Redis для {self.name}")
    
    async def consume(self):
        if not self.client:
            raise RuntimeError(f"Redis client не подключен для {self.name}")
        
        try:
            response = await self.client.xread(
                {self.stream_key: self.last_id},
                block=self.block_ms,
                count=1
            )
            
            if response:
                stream_name, messages = response[0]
                if messages:
                    msg_id, msg_data = messages[0]
                    self.last_id = msg_id
                    
                    # Redis возвращает данные как dict с полем 'data', 
                    # которое содержит JSON-строку
                    if 'data' in msg_data and isinstance(msg_data['data'], str):
                        try:
                            parsed_data = json.loads(msg_data['data'])
                        except json.JSONDecodeError:
                            logger.error(f"Не удалось распарсить JSON из Redis: {msg_data['data']}")
                            return None
                    else:
                        # Если данные уже в нужном формате
                        parsed_data = msg_data
                    
                    # Извлекаем timestamp из самого сообщения
                    msg_ts_str = parsed_data.get('datetime')
                    try:
                        timestamp = datetime.fromisoformat(msg_ts_str)
                    except (TypeError, ValueError):
                        timestamp = datetime.now()
                        logger.warning(f"Не удалось распарсить datetime из Redis сообщения, используем текущее время")
                    
                    # Извлекаем ID из данных Redis (если есть)
                    redis_msg_id = parsed_data.get('id')
                    
                    # Формируем чистую структуру данных без дублирования
                    clean_data = {
                        'coord': parsed_data.get('coord'),
                        'new_state': parsed_data.get('new_state')
                    }
                    
                    return Message(
                        timestamp=timestamp,
                        data=clean_data,
                        source=self.name,
                        id=redis_msg_id
                    )
            
            return None
            
        except Exception as e:
            logger.error(f"Ошибка чтения из Redis {self.name}: {e}")
            raise


class KafkaTopicSource(DataSource):
    """
    Источник данных из Kafka Topic.
    Ожидаемый формат сообщения:
    {
        'datetime': '2025-10-04T14:12:47.177009+07:00',
        'id': '00200004',
        'section_name': 'section_35',
        'coord': [5.060862743968945, 7.55369572851146]
    }
    """
    def __init__(
        self,
        name: str,
        topic: str,
        bootstrap_servers: str = "localhost:9092",
        group_id: str = None,
        extract_id: bool = False,
        id_field: str = "id"
    ):
        super().__init__(name)
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id or f"{name}_group"
        self.extract_id = extract_id
        self.id_field = id_field
        self.consumer = None
    
    async def connect(self):
        try:
            self.consumer = AIOKafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                enable_auto_commit=True,
                auto_offset_reset='latest',
                value_deserializer=self._deserialize_kafka_message
            )
            await self.consumer.start()
            logger.info(f"Подключено к Kafka топику {self.topic} для {self.name}")
            self._running = True
        except Exception as e:
            logger.error(f"Ошибка подключения к Kafka для {self.name}: {e}")
            raise
    
    async def disconnect(self):
        if self.consumer:
            await self.consumer.stop()
            self._running = False
            logger.info(f"Отключено от Kafka для {self.name}")
    
    @staticmethod
    def _deserialize_kafka_message(value: bytes):
        """Десериализатор для Kafka с конвертацией numpy типов."""
        def convert_numpy(obj):
            if isinstance(obj, dict):
                return {k: convert_numpy(v) for k, v in obj.items()}
            if isinstance(obj, list):
                return [convert_numpy(i) for i in obj]
            if isinstance(obj, (np.float64, np.float32)):
                return float(obj)
            if isinstance(obj, (np.int64, np.int32)):
                return int(obj)
            return obj

        try:
            decoded = value.decode('utf-8')
            data = json.loads(decoded)
            return convert_numpy(data)
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logger.error(f"Не удалось десериализовать сообщение Kafka: {e}")
            return None
    
    async def consume(self):
        if not self.consumer:
            raise RuntimeError(f"Kafka consumer не подключен для {self.name}")
        
        try:
            msg = await asyncio.wait_for(
                self.consumer.__anext__(),
                timeout=1.0
            )
            
            data = msg.value
            if data is None:
                return None

            # Извлекаем ID из данных Kafka (не дублируем)
            msg_id = None
            if self.extract_id and isinstance(data, dict):
                msg_id = data.get(self.id_field)
            
            # Извлекаем timestamp из самого сообщения
            msg_ts_str = data.get('datetime')
            try:
                timestamp = datetime.fromisoformat(msg_ts_str)
            except (TypeError, ValueError):
                timestamp = datetime.fromtimestamp(msg.timestamp / 1000.0)
                logger.warning(f"Не удалось распарсить datetime из Kafka сообщения, используем метку Kafka")
            
            # Формируем чистую структуру данных без дублирования ID
            clean_data = {
                'coord': data.get('coord'),
                'section_name': data.get('section_name')
            }
            
            return Message(
                timestamp=timestamp,
                data=clean_data,
                source=self.name,
                id=msg_id
            )
            
        except asyncio.TimeoutError:
            return None
        except Exception as e:
            logger.error(f"Ошибка чтения из Kafka {self.name}: {e}")
            raise