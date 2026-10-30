export interface CodeSnippet {
  id: string;
  title: string;
  fileName: string;
  language: 'java' | 'yaml' | 'json' | 'sql';
  category: string;
  description: string;
  code: string;
}

export const HIS_CODE_SNIPPETS: CodeSnippet[] = [
  {
    id: 'spring-kafka-config',
    title: 'Kafka Consumer & Producer Enterprise Config',
    fileName: 'KafkaProducerConsumerConfig.java',
    language: 'java',
    category: 'Configuration',
    description: 'Spring Boot 3.4 Kafka config with transactional producer factory, manual ack container factory, and DeadLetterPublishingRecoverer.',
    code: `// See /backend/src/main/java/org/his/config/KafkaProducerConsumerConfig.java`
  },
  {
    id: 'saga-orchestrator',
    title: 'Pharmacy Order Saga Orchestrator',
    fileName: 'PharmacyOrderSagaOrchestrator.java',
    language: 'java',
    category: 'Saga Orchestration',
    description: 'Implements the Orchestrated Saga Pattern in Spring Boot 3.4 with PostgreSQL state tracking and compensations.',
    code: `// See /backend/src/main/java/org/his/saga/PharmacyOrderSagaOrchestrator.java`
  },
  {
    id: 'patient-partitioner',
    title: 'Patient Chronological Timeline Partitioner',
    fileName: 'PatientTimelinePartitioner.java',
    language: 'java',
    category: 'Partitioning & Routing',
    description: 'Custom Kafka partitioner ensuring all ADT events for a patient land on the same partition.',
    code: `// See /backend/src/main/java/org/his/partitioner/PatientTimelinePartitioner.java`
  }
];
