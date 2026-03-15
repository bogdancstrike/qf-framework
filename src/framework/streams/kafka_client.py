import time
from typing import Optional

from kafka import KafkaProducer, KafkaConsumer, TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import KafkaError, TopicAlreadyExistsError, NoBrokersAvailable

from ..commons.logger import logger
from ..streams.stream_interface import StreamClientInterface


class KafkaClient(StreamClientInterface):
    _instance = None

    # @classmethod
    def __init__(
            self,
            security_protocol: Optional[str] = 'SSL',  # SASL_SSL
            sasl_mechanism='PLAIN',  # 'PLAIN, GSSAPI
            ssl_check_hostname: Optional[bool] = False,
            ssl_cafile: Optional[str] = None,
            sasl_plain_username: Optional[str] = None,
            sasl_plain_password: Optional[str] = None,
            ssl_certfile: Optional[str] = None,
            ssl_keyfile: Optional[str] = None,

            api_version: Optional[str] = "(0, 10)",
            auto_offset_reset: Optional[str] = 'earliest',
            group_id: Optional[str] = 'default_group',

            bootstrap_servers: Optional[str] = '10.10.20.185:9094'
    ):
        if KafkaClient._instance is not None:
            raise Exception("This class is a singleton!")
        self.security_protocol = security_protocol
        self.sasl_mechanism = sasl_mechanism
        self.ssl_check_hostname = ssl_check_hostname
        self.ssl_cafile = ssl_cafile
        self.sasl_plain_username = sasl_plain_username
        self.sasl_plain_password = sasl_plain_password
        self.ssl_certfile = ssl_certfile
        self.ssl_keyfile = ssl_keyfile

        self.api_version = api_version  # (0, 10)
        self.auto_offset_reset = auto_offset_reset
        self.group_id = group_id

        self.bootstrap_servers = bootstrap_servers

        if self.security_protocol==None or self.security_protocol=='NONE':
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                )
                self.admin_client = KafkaAdminClient(
                    bootstrap_servers=self.bootstrap_servers,
                )
                self.consumer = KafkaConsumer(
                    bootstrap_servers=self.bootstrap_servers,
                    auto_offset_reset=self.auto_offset_reset,
                    group_id=self.group_id,
                )
                self.consumers = {}
            except NoBrokersAvailable as e:
                logger.error(f"Kafka brokers are not available: {e}")
                raise
        else:
            try:
                self.producer = KafkaProducer(
                    security_protocol=self.security_protocol,
                    sasl_mechanism=self.sasl_mechanism,
                    ssl_check_hostname=self.ssl_check_hostname,
                    ssl_cafile=self.ssl_cafile,
                    sasl_plain_username=self.sasl_plain_username,
                    sasl_plain_password=self.sasl_plain_password,
                    ssl_certfile=self.ssl_certfile,
                    ssl_keyfile=self.ssl_keyfile,

                    # # api_version = self.api_version
                    # auto_offset_reset=self.auto_offset_reset,
                    # group_id=self.group_id,
                    # # value_deserializer=lambda x: x.decode('utf-8'),

                    bootstrap_servers=self.bootstrap_servers
                )
                self.admin_client = KafkaAdminClient(
                    security_protocol=self.security_protocol,
                    sasl_mechanism=self.sasl_mechanism,
                    ssl_check_hostname=self.ssl_check_hostname,
                    ssl_cafile=self.ssl_cafile,
                    sasl_plain_username=self.sasl_plain_username,
                    sasl_plain_password=self.sasl_plain_password,
                    ssl_certfile=self.ssl_certfile,
                    ssl_keyfile=self.ssl_keyfile,

                    # # api_version = self.api_version
                    # auto_offset_reset=self.auto_offset_reset,
                    # group_id=self.group_id,
                    # # value_deserializer=lambda x: x.decode('utf-8'),

                    bootstrap_servers=self.bootstrap_servers
                )
                self.consumer = KafkaConsumer(
                    security_protocol=self.security_protocol,
                    sasl_mechanism=self.sasl_mechanism,
                    ssl_check_hostname=self.ssl_check_hostname,
                    ssl_cafile=self.ssl_cafile,
                    sasl_plain_username=self.sasl_plain_username,
                    sasl_plain_password=self.sasl_plain_password,
                    ssl_certfile=self.ssl_certfile,
                    ssl_keyfile=self.ssl_keyfile,

                    # # api_version = self.api_version
                    auto_offset_reset=self.auto_offset_reset,
                    group_id=self.group_id,
                    # # value_deserializer=lambda x: x.decode('utf-8'),

                    bootstrap_servers=self.bootstrap_servers
                )
                self.consumers = {}
            except NoBrokersAvailable as e:
                logger.error(f"Kafka brokers are not available: {e}")
                raise
        KafkaClient._instance = self

    @classmethod
    def get_instance(
            cls,
            security_protocol: Optional[str] = 'SSL',  # SASL_SSL
            sasl_mechanism='PLAIN',  # 'PLAIN, GSSAPI
            ssl_check_hostname: Optional[bool] = False,
            ssl_cafile: Optional[str] = None,
            sasl_plain_username: Optional[str] = None,
            sasl_plain_password: Optional[str] = None,
            ssl_certfile: Optional[str] = None,
            ssl_keyfile: Optional[str] = None,

            api_version: Optional[str] = "(0, 10)",
            auto_offset_reset: Optional[str] = 'earliest',
            group_id: Optional[str] = 'default_group',

            bootstrap_servers: Optional[str] = '10.10.20.185:9094'
    ):
        if cls._instance is None:
            cls._instance = cls(
                security_protocol=security_protocol,
                sasl_mechanism=sasl_mechanism,
                ssl_check_hostname=ssl_check_hostname,
                ssl_cafile=ssl_cafile,
                sasl_plain_username=sasl_plain_username,
                sasl_plain_password=sasl_plain_password,
                ssl_certfile=ssl_certfile,
                ssl_keyfile=ssl_keyfile,

                # # api_version=api_version
                auto_offset_reset=auto_offset_reset,
                group_id=group_id,

                bootstrap_servers=bootstrap_servers
            )
        return cls._instance

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
            # Wait for send to complete
            future.get(timeout=10)
            end_time = time.time()
            logger.debug(
                f"Message sent to {topic_name}: {message} with key {key}. Duration time: {(end_time - start_time) * 1000} ms")
        except KafkaError as e:
            logger.error(f"Failed to send message to {topic_name}: {e}")

    def get_consumer(
            self,
            group_id: str,
            auto_offset_reset: str,
            topic_name: str,

    ):
        if group_id not in self.consumers:
            try:
                if self.security_protocol == None or self.security_protocol == 'NONE':
                    self.consumers[group_id] = KafkaConsumer(
                        bootstrap_servers=self.bootstrap_servers,
                        auto_offset_reset=self.auto_offset_reset,
                        group_id=self.group_id,
                    )
                else:
                    self.consumers[group_id] = KafkaConsumer(
                        security_protocol=self.security_protocol,
                        sasl_mechanism=self.sasl_mechanism,
                        ssl_check_hostname=self.ssl_check_hostname,
                        ssl_cafile=self.ssl_cafile,
                        sasl_plain_username=self.sasl_plain_username,
                        sasl_plain_password=self.sasl_plain_password,
                        ssl_certfile=self.ssl_certfile,
                        ssl_keyfile=self.ssl_keyfile,

                        # # api_version = self.api_version
                        auto_offset_reset=auto_offset_reset,
                        group_id=group_id,
                        # # value_deserializer=lambda x: x.decode('utf-8'),

                        bootstrap_servers=self.bootstrap_servers
                    )
            except Exception as e:
                logger.debug(f'e : {e}')
            # Warm up the consumer by polling once
            try:
                self.consumers[group_id].poll(1.0)
            except Exception as e:
                logger.debug(f'e : {e}')
        try:
            return self.consumers[group_id]
        except Exception as e:
            logger.debug(f'e : {e}')
            return None

    def consume_message_by_key(self, topic_name: str, key: str, group_id: str = 'default_group',
                               auto_offset_reset: str = 'earliest', timeout_ms: int = 10000):
        try:
            consumer = self.get_consumer(group_id, auto_offset_reset, topic_name)
            partitions = consumer.partitions_for_topic(topic_name)
            if not partitions:
                logger.error(f"No partitions found for topic {topic_name}")
                return None

            start_time = time.time()
            for partition in partitions:
                tp = TopicPartition(topic_name, partition)
                consumer.assign([tp])
                end_offsets = consumer.end_offsets([tp])[tp]
                consumer.seek_to_beginning(tp)

                while consumer.position(tp) < end_offsets:
                    if time.time() - start_time > timeout_ms / 1000.0:
                        logger.debug(f"Timeout reached while consuming message with key: {key}")
                        return None

                    msg = consumer.poll(timeout_ms / 1000.0)
                    if msg:
                        for record in msg.values():
                            for rec in record:
                                if rec.key and rec.key.decode('utf-8') == key:
                                    message = rec.value.decode('utf-8')
                                    end_time = time.time()
                                    logger.debug(
                                        f"Consumed message with key: {key} from topic: {topic_name}: {message}. Duration time: {(end_time - start_time) * 1000} ms")
                                    return message

                logger.debug(f"Reached end of partition {partition} without finding key: {key}")
        except KafkaError as e:
            logger.error(f"Failed to consume message by key from {topic_name}: {e}")
        return None

    def consume_message(self, topic_name: str, group_id: str = 'default_group', auto_offset_reset: str = 'earliest',
                        timeout_ms: int = 10000):
        try:
            start_time = time.time()
            consumer = self.get_consumer(group_id, auto_offset_reset, topic_name)
            partitions = consumer.partitions_for_topic(topic_name)
            if not partitions:
                logger.error(f"No partitions found for topic {topic_name}")
                return None

            last_message = None
            for partition in partitions:
                tp = TopicPartition(topic_name, partition)
                consumer.assign([tp])
                consumer.seek_to_end(tp)
                # Move one offset back from the end to get the last message
                end_offset = consumer.position(tp)
                consumer.seek(tp, end_offset - 1)

                msg = consumer.poll(timeout_ms / 1000.0)
                if msg:
                    for record in msg.values():
                        for rec in record:
                            last_message = rec.value.decode('utf-8')
                            end_time = time.time()
                            logger.debug(
                                f"Consumed last message from topic: {topic_name}: {last_message}. Duration time: {(end_time - start_time) * 1000} ms")

            return last_message
        except KafkaError as e:
            logger.error(f"Failed to consume last message from {topic_name}: {e}")
        return None

    def delete_topic(self, topic_name: str):
        if not self.topic_exists(topic_name):
            logger.debug(f"Topic {topic_name} does not exist.")
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
            logger.error(f"Failed to check if topic {topic_name} exists: {e}")
            return False
