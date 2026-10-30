import http from 'node:http';

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({
    service: 'Hospital Information System (HIS) - Backend Service',
    status: 'HEALTHY',
    version: '1.0.0',
    backend: {
      stack: 'Java 17 / Spring Boot 3.4 / Apache Kafka (KRaft) / PostgreSQL 16',
      directory: '/backend',
      domains: [
        {
          name: 'Pharmacy Service',
          domain: 'Order & Inventory Management',
          tables: ['pharmacy_orders', 'medication_inventory', 'prescriptions', 'his_pharmacy_saga_state']
        },
        {
          name: 'Billing Service',
          domain: 'Finance & Revenue Cycle',
          tables: ['billing_accounts', 'invoices', 'payment_transactions', 'his_consumed_messages']
        },
        {
          name: 'Patient Service',
          domain: 'Clinical EHR & ADT',
          tables: ['patients', 'adt_history', 'patient_timeline_events', 'his_kms_patient_keys']
        }
      ]
    }
  }, null, 2));
});

const PORT = 3000;
server.listen(PORT, '0.0.0.0', () => {
  console.log(`HIS Backend API running on port ${PORT}`);
});
