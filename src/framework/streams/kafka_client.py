import time
import threading
from typing import Optional, Dict, Any

from kafka import KafkaProducer, KafkaConsumer, TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import KafkaError, TopicAlreadyExistsError, NoBrokersAvailable

from ..commons.logger import logger
from ..streams.stream_interface import StreamClientInterface


class KafkaClient(StreamClientInterface):
    _instance = None
    _lock = threading.Lock()
    _initialized = False

    def __new__(cls, *args, **kwargs):
        """
        Thread-safe singleton implementation. Returns the existing instance
        if it exists, otherwise creates it.
        """
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(KafkaClient, cls).__new__(cls)
        return cls._instance

    def __init__(
            self,
            security_protocol: Optional[str] = None,
            sasl_mechanism: str = 'PLAIN',
            ssl_check_hostname: bool = False,
            ssl_cafile: Optional[str] = None,
            sasl_plain_username: Optional[str] = None,
            sasl_plain_password: Optional[str] = None,
            ssl_certfile: Optional[str] = None,
            ssl_keyfile: Optional[str] = None,
            api_version: str = "(0, 10)",
            auto_offset_reset: str = 'earliest',
            group_id: str = 'default_group',
            bootstrap_servers: str = '10.10.20.185:9094'
    ):
        # Prevent re-initialization if the singleton is already set up
        if KafkaClient._initialized:
            return

        self.security_protocol = security_protocol
        self.sasl_mechanism = sasl_mechanism
        self.ssl_check_hostname = ssl_check_hostname
        self.ssl_cafile = ssl_cafile
        self.sasl_plain_username = sasl_plain_username
        self.sasl_plain_password = sasl_plain_password
        self.ssl_certfile = ssl_certfile
        self.ssl_keyfile = ssl_keyfile
        self.api_version = api_version
        self.auto_offset_reset = auto_offset_reset
        self.group_id = group_id
        self.bootstrap_servers = bootstrap_servers
        self.consumers = {}

        # Consolidate configuration to avoid code duplication
        base_configs = self._get_base_config()

        try:
            self.producer = KafkaProducer(**base_configs)
            self.admin_client = KafkaAdminClient(**base_configs)

            # The main consumer needs the group and offset reset
            consumer_configs = base_configs.copy()
            consumer_configs.update({
                'auto_offset_reset': self.auto_offset_reset,
                'group_id': self.group_id
            })
            self.consumer = KafkaConsumer(**consumer_configs)

            KafkaClient._initialized = True
            logger.debug("🔵 KafkaClient singleton initialized successfully", "blue")
        except NoBrokersAvailable as e:
            logger.error(f"Kafka brokers are not available: {e}")
            raise

    def _get_base_config(self) -> Dict[str, Any]:
        """Helper to build the shared configuration dictionary."""
        configs = {'bootstrap_servers': self.bootstrap_servers}

        if self.security_protocol and self.security_protocol.upper() != 'NONE':
            configs.update({
                'security_protocol': self.security_protocol,
                'sasl_mechanism': self.sasl_mechanism,
                'ssl_check_hostname': self.ssl_check_hostname,
                'ssl_cafile': self.ssl_cafile,
                'sasl_plain_username': self.sasl_plain_username,
                'sasl_plain_password': self.sasl_plain_password,
                'ssl_certfile': self.ssl_certfile,
                'ssl_keyfile': self.ssl_keyfile,
            })
        return configs

    # ==========================================================
    # Logic methods
    # ==========================================================

    def create_topic(self, topic_name: str, num_partitions: int = 1, replication_factor: int = 1,
                     retention_time: str = '10000'):
        if self.topic_exists(topic_name):
            logger.debug(f"Topic {topic_name} already exists.")
            return

        topic_list = [
            NewTopic(
                name=topic_name,
                num_partitions=num_partitions,
                replication_factor=replication_factor,
                topic_configs={"retention.ms": retention_time}
            )
        ]
        try:
            self.admin_client.create_topics(new_topics=topic_list, validate_only=False)
            logger.debug(f"Topic {topic_name} created with retention.ms={retention_time}")
        except TopicAlreadyExistsError:
            logger.debug(f"Topic {topic_name} already exists.")
        except KafkaError as e:
            logger.error(f"Failed to create topic {topic_name}: {e}")

    def put_message(self, topic_name: str, message: str, key: str = None):
        try:
            start_time = time.time()
            future = self.producer.send(topic_name, key=key.encode('utf-8') if key else None,
                                        value=message.encode('utf-8'))
            future.get(timeout=10)
            end_time = time.time()
            logger.debug(
                f"Message sent to {topic_name}: {message} with key {key}. Duration time: {(end_time - start_time) * 1000} ms")
        except KafkaError as e:
            logger.error(f"Failed to send message to {topic_name}: {e}")

    def get_consumer(self, group_id: str, auto_offset_reset: str, topic_name: str):
        if group_id not in self.consumers:
            configs = self._get_base_config()
            configs.update({
                'auto_offset_reset': auto_offset_reset,
                'group_id': group_id
            })
            try:
                self.consumers[group_id] = KafkaConsumer(**configs)
                self.consumers[group_id].poll(1.0)  # Warm up
            except Exception as e:
                logger.debug(f'Error initializing specific consumer: {e}')

        return self.consumers.get(group_id)

    def consume_message_by_key(self, topic_name: str, key: str, group_id: str = 'default_group',
                               auto_offset_reset: str = 'earliest', timeout_ms: int = 10000):
        try:
            consumer = self.get_consumer(group_id, auto_offset_reset, topic_name)
            partitions = consumer.partitions_for_topic(topic_name)
            if not partitions:
                return None

            start_time = time.time()
            for partition in partitions:
                tp = TopicPartition(topic_name, partition)
                consumer.assign([tp])
                end_offsets = consumer.end_offsets([tp])[tp]
                consumer.seek_to_beginning(tp)

                while consumer.position(tp) < end_offsets:
                    if time.time() - start_time > timeout_ms / 1000.0:
                        return None
                    msg = consumer.poll(timeout_ms / 1000.0)
                    if msg:
                        for record in msg.values():
                            for rec in record:
                                if rec.key and rec.key.decode('utf-8') == key:
                                    return rec.value.decode('utf-8')
        except KafkaError as e:
            logger.error(f"Error consuming by key: {e}")
        return None

    def consume_message(self, topic_name: str, group_id: str = 'default_group', auto_offset_reset: str = 'earliest',
                        timeout_ms: int = 10000):
        try:
            consumer = self.get_consumer(group_id, auto_offset_reset, topic_name)
            partitions = consumer.partitions_for_topic(topic_name)
            if not partitions:
                return None

            for partition in partitions:
                tp = TopicPartition(topic_name, partition)
                consumer.assign([tp])
                consumer.seek_to_end(tp)
                pos = consumer.position(tp)
                if pos > 0:
                    consumer.seek(tp, pos - 1)
                    msg = consumer.poll(timeout_ms / 1000.0)
                    if msg:
                        for record in msg.values():
                            for rec in record:
                                return rec.value.decode('utf-8')
        except KafkaError as e:
            logger.error(f"Error consuming last message: {e}")
        return None

    def delete_topic(self, topic_name: str):
        if not self.topic_exists(topic_name):
            return
        try:
            self.admin_client.delete_topics([topic_name])
            logger.debug(f"Topic {topic_name} deleted.")
        except KafkaError as e:
            logger.error(f"Failed to delete topic {topic_name}: {e}")

    def topic_exists(self, topic_name: str) -> bool:
        try:
            cluster_metadata = self.admin_client.list_topics()
            return topic_name in cluster_metadata
        except KafkaError as e:
            logger.error(f"Failed to check topic existence: {e}")
            return False