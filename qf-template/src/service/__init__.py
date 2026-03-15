# Service layer — sits between HTTP handlers and business logic / third parties.
#
#   health_service.py — connectivity checks for Redis, Kafka, Postgres
#   api_handler.py    — Redis result caching, OTel tracing, call stats
