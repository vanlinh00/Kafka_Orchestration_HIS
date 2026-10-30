# HIS Backend – Kafka Event Stream & Saga Orchestrator

See root [`/README.md`](../README.md) for the full architecture overview, compliance guide, and system diagrams.

## Quick Run

```bash
# 1. Package JAR
mvn clean package -DskipTests

# 2. Run Spring Boot Service
mvn spring-boot:run
```

## Key Java Components

* `org.his.config.KafkaProducerConsumerConfig`: Exactly-Once Producer & Manual-Ack Consumer container factory with Dead Letter Publisher.
* `org.his.config.KafkaTopicConfiguration`: Programmatic topic builder defining 10 enterprise topics across 5 domains.
* `org.his.partitioner.PatientTimelinePartitioner`: Guarantees in-order patient ADT/EHR timeline and salts ICU telemetry keys.
* `org.his.saga.PharmacyOrderSagaOrchestrator`: 4-step stateful saga orchestrator with automatic compensation rollbacks.
* `org.his.consumer.IdempotentBillingConsumer`: Dual-write elimination consumer preventing duplicate charges.
* `org.his.security.KmsEnvelopeEncryptionSerializer`: AES-256-GCM envelope encryption for HIPAA & GDPR crypto-shredding.
