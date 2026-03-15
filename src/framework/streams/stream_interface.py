from abc import ABC, abstractmethod


class StreamClientInterface(ABC):

    @abstractmethod
    def create_topic(self, topic_name: str, num_partitions: int = 1, replication_factor: int = 1,
                     retention_time: str = '10000'):
        pass

    @abstractmethod
    def put_message(self, topic_name: str, message: str, key: str):
        pass

    @abstractmethod
    def consume_message_by_key(self, topic_name: str, key: str, group_id: str = 'default_group',
                               auto_offset_reset: str = 'earliest', timeout_ms: int = 10000):
        pass

    @abstractmethod
    def consume_message(self, topic_name: str, group_id: str = 'default_group',
                        auto_offset_reset: str = 'earliest', timeout_ms: int = 10000):
        pass

    @abstractmethod
    def delete_topic(self, topic_name: str):
        pass

    @abstractmethod
    def topic_exists(self, topic_name: str) -> bool:
        pass
