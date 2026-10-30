export type DomainType = 'patient' | 'clinical' | 'pharmacy' | 'billing' | 'telemetry';

export interface KafkaTopicDef {
  name: string;
  domain: DomainType;
  entity: string;
  eventType: string;
  version: string;
  description: string;
  partitionKey: string;
  partitionKeyRationale: string;
  partitions: number;
  replicationFactor: number;
  minInsyncReplicas: number;
  cleanupPolicy: 'delete' | 'compact' | 'compact,delete';
  retentionHours: number | string;
  maxMessageBytes: string;
  schemaFormat: 'Avro' | 'Protobuf' | 'FHIR JSON';
  throughputEstimate: string;
  guarantee: 'EOS' | 'At-Least-Once';
}

export type SagaStepStatus = 'idle' | 'executing' | 'completed' | 'failed' | 'compensated';

export interface SagaEventLog {
  id: string;
  timestamp: string;
  stepName: string;
  topic: string;
  payloadType: string;
  status: 'published' | 'consumed' | 'compensated' | 'error';
  message: string;
  data: Record<string, unknown>;
}

export interface SagaState {
  sagaId: string;
  patientId: string;
  orderId: string;
  medication: string;
  dosage: string;
  price: number;
  status: 'PENDING' | 'INVENTORY_RESERVED' | 'BILLING_PREAUTH_APPROVED' | 'DISPATCHED' | 'FAILED_ROLLED_BACK';
  currentStep: number;
  steps: {
    orderPlaced: SagaStepStatus;
    inventoryCheck: SagaStepStatus;
    billingPreAuth: SagaStepStatus;
    prescriptionDispatch: SagaStepStatus;
  };
  failureReason?: string;
  eventHistory: SagaEventLog[];
}

export interface DlqMessage {
  id: string;
  originalTopic: string;
  failedTimestamp: string;
  partition: number;
  offset: number;
  key: string;
  exceptionClass: string;
  exceptionMessage: string;
  rawPayload: string;
  status: 'QUARANTINED' | 'REPLAYED' | 'DISCARDED';
  retryCount: number;
}
