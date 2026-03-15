# Service layer for the PoC application.
#
# Modules in this package sit between the HTTP boundary (api_endpoints.py)
# and the business-logic layer (workers/workers.py).  They own cross-cutting
# concerns that do not belong in either layer:
#
#   api_handler.py   — Redis caching, OTel tracing, logging, request stats
#   kafka_service.py — One-off Kafka produce/consume via KafkaClient
