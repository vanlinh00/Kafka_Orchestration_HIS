import React, { useState } from 'react';
import { Play, RotateCcw, AlertOctagon, CheckCircle2, ArrowRight, ShieldAlert, Cpu } from 'lucide-react';
import { SagaState, SagaEventLog } from '../types/his-kafka';

export const SagaOrchestratorSimulator: React.FC = () => {
  const initialSagaState: SagaState = {
    sagaId: 'SAGA-7729-A',
    patientId: 'MRN-842109',
    orderId: 'RX-55018',
    medication: 'Meropenem 1g IV Infusion',
    dosage: '1000mg q8h',
    price: 340.00,
    status: 'PENDING',
    currentStep: 0,
    steps: {
      orderPlaced: 'idle',
      inventoryCheck: 'idle',
      billingPreAuth: 'idle',
      prescriptionDispatch: 'idle'
    },
    eventHistory: []
  };

  const [saga, setSaga] = useState<SagaState>(initialSagaState);
  const [simulationScenario, setSimulationScenario] = useState<'success' | 'inventory_failure' | 'billing_failure' | 'hardware_failure'>('success');
  const [isRunning, setIsRunning] = useState<boolean>(false);

  const resetSimulation = () => {
    setSaga({
      ...initialSagaState,
      sagaId: `SAGA-${Math.floor(1000 + Math.random() * 9000)}-${simulationScenario.charAt(0).toUpperCase()}`
    });
    setIsRunning(false);
  };

  const runSimulation = async () => {
    if (isRunning) return;
    setIsRunning(true);
    resetSimulation();

    const addEvent = (
      stepName: string,
      topic: string,
      payloadType: string,
      status: 'published' | 'consumed' | 'compensated' | 'error',
      message: string,
      data: Record<string, unknown>
    ) => {
      setSaga(prev => ({
        ...prev,
        eventHistory: [
          ...prev.eventHistory,
          {
            id: `evt-${Date.now()}-${Math.random().toString(36).substr(2, 4)}`,
            timestamp: new Date().toISOString().split('T')[1].replace('Z', ''),
            stepName,
            topic,
            payloadType,
            status,
            message,
            data
          }
        ]
      }));
    };

    // STEP 1: Physician Places Order
    setSaga(prev => ({
      ...prev,
      status: 'PENDING',
      currentStep: 1,
      steps: { ...prev.steps, orderPlaced: 'executing' }
    }));
    addEvent(
      'Step 1: Order Ingestion',
      'his.pharmacy.prescriptions.v1',
      'MedicationOrderCreatedEvent',
      'published',
      'Physician submitted CPOE pharmacy order for Meropenem 1g IV.',
      { orderId: saga.orderId, patientId: saga.patientId, medication: saga.medication }
    );

    await new Promise(r => setTimeout(r, 600));

    setSaga(prev => ({
      ...prev,
      steps: { ...prev.steps, orderPlaced: 'completed', inventoryCheck: 'executing' }
    }));

    // STEP 2: Inventory Check
    addEvent(
      'Step 2: Inventory Reservation',
      'his.pharmacy.saga-commands.v1',
      'ReserveInventoryCommand',
      'published',
      'Saga Orchestrator issued ReserveInventoryCommand to Automated Pharmacy Stock.',
      { sagaId: saga.sagaId, medicationCode: 'MEROPENEM-1G', quantity: 3 }
    );

    await new Promise(r => setTimeout(r, 700));

    if (simulationScenario === 'inventory_failure') {
      // Inventory Out of Stock!
      setSaga(prev => ({
        ...prev,
        status: 'FAILED_ROLLED_BACK',
        steps: { ...prev.steps, inventoryCheck: 'failed' },
        failureReason: 'Stock shortage: Meropenem 1g IV currently unavailable in central cleanroom.'
      }));
      addEvent(
        'Step 2: Stock Depleted',
        'his.pharmacy.inventory-responses.v1',
        'InventoryOutOfStockEvent',
        'error',
        'Central Pharmacy Inventory replied: OUT OF STOCK.',
        { reason: 'ZERO_STOCK_AVAILABLE' }
      );
      addEvent(
        'Compensation: Order Cancelled',
        'his.pharmacy.prescriptions.v1',
        'PrescriptionRejectedEvent',
        'compensated',
        'Saga terminated. Physician notified to select alternate carbapenem/cephalosporin.',
        { alertPhysicianId: 'DOC-3921' }
      );
      setIsRunning(false);
      return;
    }

    // Inventory success
    setSaga(prev => ({
      ...prev,
      status: 'INVENTORY_RESERVED',
      currentStep: 2,
      steps: { ...prev.steps, inventoryCheck: 'completed', billingPreAuth: 'executing' }
    }));
    addEvent(
      'Step 2: Stock Locked',
      'his.pharmacy.inventory-responses.v1',
      'InventoryReservedEvent',
      'consumed',
      'Stock lock #LOCK-9921 acquired for 3 units.',
      { lockId: 'LOCK-9921', cabinet: 'CENTRAL_CAROUSEL_4' }
    );

    await new Promise(r => setTimeout(r, 700));

    // STEP 3: Billing Pre-authorization
    addEvent(
      'Step 3: Billing Pre-auth',
      'his.pharmacy.saga-commands.v1',
      'AuthorizeBillingCommand',
      'published',
      'Saga Orchestrator dispatched AuthorizeBillingCommand to Payer Gateway (BlueCross EDI 270).',
      { amount: saga.price, patientId: saga.patientId, cpt: 'J7131' }
    );

    await new Promise(r => setTimeout(r, 800));

    if (simulationScenario === 'billing_failure') {
      // Billing Rejected!
      setSaga(prev => ({
        ...prev,
        status: 'FAILED_ROLLED_BACK',
        steps: {
          ...prev.steps,
          billingPreAuth: 'failed',
          inventoryCheck: 'compensated'
        },
        failureReason: 'Payer Pre-Authorization Denied: Prior Authorization Requirement Missing (CARC 197).'
      }));
      addEvent(
        'Step 3: Pre-Auth Denied',
        'his.billing.preauth-responses.v1',
        'BillingPreAuthDeniedEvent',
        'error',
        'Insurance Clearinghouse denied coverage: Prior Authorization required.',
        { denialCode: 'CARC-197', payer: 'BLUE_CROSS_COMMERCIAL' }
      );
      // Compensation: Release locked inventory
      addEvent(
        'Compensation: Rollback Stock',
        'his.pharmacy.saga-commands.v1',
        'ReleaseInventoryCommand',
        'compensated',
        'COMPENSATION TRIGGERED: Orchestrator released Inventory Lock #LOCK-9921.',
        { lockId: 'LOCK-9921', action: 'STOCK_RESTORED' }
      );
      addEvent(
        'Compensation: Notify Clinician',
        'his.pharmacy.prescriptions.v1',
        'PrescriptionHoldEvent',
        'compensated',
        'Order placed on Billing Hold. Financial counselor and attending alerted.',
        { status: 'HELD_FINANCIAL' }
      );
      setIsRunning(false);
      return;
    }

    // Billing Approved
    setSaga(prev => ({
      ...prev,
      status: 'BILLING_PREAUTH_APPROVED',
      currentStep: 3,
      steps: { ...prev.steps, billingPreAuth: 'completed', prescriptionDispatch: 'executing' }
    }));
    addEvent(
      'Step 3: Pre-Auth Approved',
      'his.billing.preauth-responses.v1',
      'BillingPreAuthApprovedEvent',
      'consumed',
      'Pre-auth approved #AUTH-88401. Copay: $15.00, Payer Coverage: $325.00.',
      { authCode: 'AUTH-88401', allowable: 340.00 }
    );

    await new Promise(r => setTimeout(r, 700));

    // STEP 4: Prescription Dispatch
    addEvent(
      'Step 4: Dispense Dispatch',
      'his.pharmacy.saga-commands.v1',
      'DispatchPrescriptionCommand',
      'published',
      'Saga Orchestrator routed dispense command to ICU Automated Dispensing Cabinet (Pyxis MedStation).',
      { cabinetId: 'ICU-PYXIS-NORTH-2', patientBed: 'ICU-BED-04' }
    );

    await new Promise(r => setTimeout(r, 800));

    if (simulationScenario === 'hardware_failure') {
      // Cabinet Drawer Jam / Hardware Failure!
      setSaga(prev => ({
        ...prev,
        status: 'FAILED_ROLLED_BACK',
        steps: {
          ...prev.steps,
          prescriptionDispatch: 'failed',
          billingPreAuth: 'compensated',
          inventoryCheck: 'compensated'
        },
        failureReason: 'Pyxis Cabinet Drawer Jam: Solenoid hardware timeout on pocket 3B.'
      }));
      addEvent(
        'Step 4: Hardware Fault',
        'his.pharmacy.dispense-events.v1',
        'DispenseCabinetHardwareFaultEvent',
        'error',
        'Cabinet sensor reported door interlock jam.',
        { errorCode: 'SOLENOID_TIMEOUT_3B' }
      );
      // Double compensation: Rollback Billing AND Release Inventory
      addEvent(
        'Compensation: Void Billing Hold',
        'his.pharmacy.saga-commands.v1',
        'VoidBillingHoldCommand',
        'compensated',
        'COMPENSATION: Reversed insurance pre-authorization claim hold #AUTH-88401.',
        { authCode: 'AUTH-88401' }
      );
      addEvent(
        'Compensation: Return Stock',
        'his.pharmacy.saga-commands.v1',
        'ReleaseInventoryCommand',
        'compensated',
        'COMPENSATION: Restocked inventory allocation.',
        { lockId: 'LOCK-9921' }
      );
      setIsRunning(false);
      return;
    }

    // Nominal Full Success
    setSaga(prev => ({
      ...prev,
      status: 'DISPATCHED',
      currentStep: 4,
      steps: { ...prev.steps, prescriptionDispatch: 'completed' }
    }));
    addEvent(
      'Step 4: Dispensed & Verified',
      'his.pharmacy.dispense-events.v1',
      'PrescriptionDispatchedEvent',
      'consumed',
      'Pyxis drawer 3B opened. Nurse barcode verified medication vial and patient wristband.',
      { nurseBadge: 'RN-99201', fiveRightsVerified: true }
    );
    addEvent(
      'Saga Completion',
      'his.pharmacy.prescriptions.v1',
      'PrescriptionFulfilledEvent',
      'published',
      'SAGA WORKFLOW COMPLETED: Total patient timeline updated atomically.',
      { sagaId: saga.sagaId, totalLatencyMs: 3100 }
    );

    setIsRunning(false);
  };

  const getStepBadge = (status: string) => {
    switch (status) {
      case 'completed':
        return <span className="text-emerald-700 font-semibold flex items-center gap-1"><CheckCircle2 className="w-3.5 h-3.5" /> Complete</span>;
      case 'executing':
        return <span className="text-blue-700 font-semibold flex items-center gap-1 animate-pulse"><Cpu className="w-3.5 h-3.5" /> Processing</span>;
      case 'failed':
        return <span className="text-rose-700 font-semibold flex items-center gap-1"><AlertOctagon className="w-3.5 h-3.5" /> Failed</span>;
      case 'compensated':
        return <span className="text-amber-700 font-semibold flex items-center gap-1"><RotateCcw className="w-3.5 h-3.5" /> Compensated</span>;
      default:
        return <span className="text-slate-400">Pending</span>;
    }
  };

  return (
    <div className="space-y-6">
      {/* Simulation Control Bar */}
      <div className="bg-white rounded-lg border border-slate-200 p-6">
        <div className="flex flex-col md:flex-row md:items-center justify-between gap-4 border-b border-slate-100 pb-4 mb-4">
          <div>
            <h2 className="text-lg font-bold text-slate-900 tracking-tight">
              Interactive Saga Orchestrator: Pharmacy Workflow
            </h2>
            <p className="text-xs text-slate-500 mt-0.5">
              Simulate the 4-step distributed transaction: Doctor Order → Inventory Check → Billing Pre-auth → Prescription Dispatch.
            </p>
          </div>
          <div className="flex items-center gap-2">
            <button
              onClick={runSimulation}
              disabled={isRunning}
              className={`flex items-center gap-1.5 px-4 py-2 rounded text-xs font-semibold text-white transition-colors cursor-pointer ${
                isRunning ? 'bg-slate-400 cursor-not-allowed' : 'bg-slate-900 hover:bg-slate-800'
              }`}
            >
              <Play className="w-3.5 h-3.5" />
              <span>{isRunning ? 'Orchestrating...' : 'Execute Saga Workflow'}</span>
            </button>
            <button
              onClick={resetSimulation}
              disabled={isRunning}
              className="px-3 py-2 border border-slate-200 hover:bg-slate-50 rounded text-xs text-slate-700 cursor-pointer"
            >
              <RotateCcw className="w-3.5 h-3.5" />
            </button>
          </div>
        </div>

        {/* Scenario Selectors */}
        <div className="space-y-2">
          <label className="text-xs font-semibold text-slate-700">Choose Execution Scenario:</label>
          <div className="grid grid-cols-1 sm:grid-cols-4 gap-2">
            <button
              onClick={() => { setSimulationScenario('success'); resetSimulation(); }}
              className={`p-3 rounded border text-left text-xs transition-all cursor-pointer ${
                simulationScenario === 'success' ? 'bg-slate-900 text-white border-slate-900' : 'bg-slate-50 text-slate-700 border-slate-200 hover:bg-slate-100'
              }`}
            >
              <div className="font-semibold">1. Nominal Success Path</div>
              <div className={`text-[11px] mt-0.5 ${simulationScenario === 'success' ? 'text-slate-300' : 'text-slate-500'}`}>
                All steps succeed; medication dispensed.
              </div>
            </button>

            <button
              onClick={() => { setSimulationScenario('inventory_failure'); resetSimulation(); }}
              className={`p-3 rounded border text-left text-xs transition-all cursor-pointer ${
                simulationScenario === 'inventory_failure' ? 'bg-slate-900 text-white border-slate-900' : 'bg-slate-50 text-slate-700 border-slate-200 hover:bg-slate-100'
              }`}
            >
              <div className="font-semibold">2. Inventory Out-of-Stock</div>
              <div className={`text-[11px] mt-0.5 ${simulationScenario === 'inventory_failure' ? 'text-slate-300' : 'text-slate-500'}`}>
                Stock shortage halts saga; alerts doctor.
              </div>
            </button>

            <button
              onClick={() => { setSimulationScenario('billing_failure'); resetSimulation(); }}
              className={`p-3 rounded border text-left text-xs transition-all cursor-pointer ${
                simulationScenario === 'billing_failure' ? 'bg-slate-900 text-white border-slate-900' : 'bg-slate-50 text-slate-700 border-slate-200 hover:bg-slate-100'
              }`}
            >
              <div className="font-semibold">3. Billing Pre-Auth Denied</div>
              <div className={`text-[11px] mt-0.5 ${simulationScenario === 'billing_failure' ? 'text-slate-300' : 'text-slate-500'}`}>
                Triggers compensation: Unlocks inventory!
              </div>
            </button>

            <button
              onClick={() => { setSimulationScenario('hardware_failure'); resetSimulation(); }}
              className={`p-3 rounded border text-left text-xs transition-all cursor-pointer ${
                simulationScenario === 'hardware_failure' ? 'bg-slate-900 text-white border-slate-900' : 'bg-slate-50 text-slate-700 border-slate-200 hover:bg-slate-100'
              }`}
            >
              <div className="font-semibold">4. Pyxis Cabinet Jam</div>
              <div className={`text-[11px] mt-0.5 ${simulationScenario === 'hardware_failure' ? 'text-slate-300' : 'text-slate-500'}`}>
                Reverses billing pre-auth AND restocks inventory!
              </div>
            </button>
          </div>
        </div>

        {/* 4-Step Interactive Pipeline Status */}
        <div className="mt-6 pt-5 border-t border-slate-100">
          <div className="grid grid-cols-1 sm:grid-cols-4 gap-3">
            {/* Step 1 */}
            <div className={`p-3 rounded border ${saga.steps.orderPlaced === 'completed' ? 'border-slate-300 bg-slate-50' : 'border-slate-200'}`}>
              <div className="text-[10px] uppercase font-semibold text-slate-400">Step 1</div>
              <div className="text-xs font-bold text-slate-900 mt-0.5">Doctor Places Order</div>
              <div className="text-[11px] text-slate-500 font-mono mt-1">CPOE MedicationRequest</div>
              <div className="mt-2 text-xs">{getStepBadge(saga.steps.orderPlaced)}</div>
            </div>

            {/* Step 2 */}
            <div className={`p-3 rounded border ${saga.steps.inventoryCheck === 'completed' ? 'border-slate-300 bg-slate-50' : saga.steps.inventoryCheck === 'compensated' ? 'border-amber-300 bg-amber-50/50' : 'border-slate-200'}`}>
              <div className="text-[10px] uppercase font-semibold text-slate-400">Step 2</div>
              <div className="text-xs font-bold text-slate-900 mt-0.5">Inventory Reservation</div>
              <div className="text-[11px] text-slate-500 font-mono mt-1">Stock Lock Allocation</div>
              <div className="mt-2 text-xs">{getStepBadge(saga.steps.inventoryCheck)}</div>
            </div>

            {/* Step 3 */}
            <div className={`p-3 rounded border ${saga.steps.billingPreAuth === 'completed' ? 'border-slate-300 bg-slate-50' : saga.steps.billingPreAuth === 'compensated' ? 'border-amber-300 bg-amber-50/50' : 'border-slate-200'}`}>
              <div className="text-[10px] uppercase font-semibold text-slate-400">Step 3</div>
              <div className="text-xs font-bold text-slate-900 mt-0.5">Billing Pre-authorization</div>
              <div className="text-[11px] text-slate-500 font-mono mt-1">Payer EDI 270/271 Check</div>
              <div className="mt-2 text-xs">{getStepBadge(saga.steps.billingPreAuth)}</div>
            </div>

            {/* Step 4 */}
            <div className={`p-3 rounded border ${saga.steps.prescriptionDispatch === 'completed' ? 'border-slate-300 bg-slate-50' : saga.steps.prescriptionDispatch === 'failed' ? 'border-rose-300 bg-rose-50/50' : 'border-slate-200'}`}>
              <div className="text-[10px] uppercase font-semibold text-slate-400">Step 4</div>
              <div className="text-xs font-bold text-slate-900 mt-0.5">Prescription Dispatch</div>
              <div className="text-[11px] text-slate-500 font-mono mt-1">Pyxis Cabinet & Bedside RN</div>
              <div className="mt-2 text-xs">{getStepBadge(saga.steps.prescriptionDispatch)}</div>
            </div>
          </div>
        </div>
      </div>

      {/* Real-time State & Event Log Viewer */}
      <div className="grid grid-cols-1 lg:grid-cols-12 gap-6">
        {/* PostgreSQL Saga State Entity Store */}
        <div className="lg:col-span-5 bg-white rounded-lg border border-slate-200 p-5 space-y-4">
          <div className="border-b border-slate-100 pb-3">
            <h3 className="text-sm font-bold text-slate-900">PostgreSQL Saga State Store</h3>
            <p className="text-[11px] text-slate-500">Table: his_pharmacy_saga_state (Row Snapshot)</p>
          </div>

          <div className="space-y-2 text-xs font-mono">
            <div className="flex justify-between py-1 border-b border-slate-100">
              <span className="text-slate-500">saga_id:</span>
              <span className="font-semibold text-slate-900">{saga.sagaId}</span>
            </div>
            <div className="flex justify-between py-1 border-b border-slate-100">
              <span className="text-slate-500">patient_id:</span>
              <span className="text-slate-800">{saga.patientId}</span>
            </div>
            <div className="flex justify-between py-1 border-b border-slate-100">
              <span className="text-slate-500">order_id:</span>
              <span className="text-slate-800">{saga.orderId}</span>
            </div>
            <div className="flex justify-between py-1 border-b border-slate-100">
              <span className="text-slate-500">medication:</span>
              <span className="text-slate-800 truncate max-w-[180px]">{saga.medication}</span>
            </div>
            <div className="flex justify-between py-1 border-b border-slate-100">
              <span className="text-slate-500">estimated_cost:</span>
              <span className="text-slate-800 tabular-nums">${saga.price.toFixed(2)} USD</span>
            </div>
            <div className="flex justify-between py-1 border-b border-slate-100">
              <span className="text-slate-500">status:</span>
              <span className={`font-bold ${saga.status === 'DISPATCHED' ? 'text-emerald-700' : saga.status === 'FAILED_ROLLED_BACK' ? 'text-rose-700' : 'text-blue-700'}`}>
                {saga.status}
              </span>
            </div>
          </div>

          {saga.failureReason && (
            <div className="p-3 bg-rose-50 border border-rose-200 rounded text-xs text-rose-800 space-y-1">
              <div className="font-bold flex items-center gap-1">
                <ShieldAlert className="w-3.5 h-3.5" />
                <span>Saga Interruption & Compensation:</span>
              </div>
              <p className="text-[11px] text-rose-700">{saga.failureReason}</p>
            </div>
          )}

          <div className="pt-2 text-[11px] text-slate-500 leading-relaxed">
            <strong>State Machine Invariant:</strong> The orchestrator updates this record atomically with each Kafka command emitted. If the broker rebalances, recovery consumers resume from the last committed step with zero ghost orders.
          </div>
        </div>

        {/* Kafka Command & Event Log Stream */}
        <div className="lg:col-span-7 bg-slate-900 rounded-lg p-5 text-slate-100 space-y-3 font-mono text-xs">
          <div className="flex items-center justify-between border-b border-slate-800 pb-2">
            <span className="text-slate-400 font-semibold text-[11px] uppercase tracking-wider">
              Kafka Event Stream Activity Log ({saga.eventHistory.length} messages)
            </span>
            <span className="text-slate-500 text-[10px]">Topic Channel: his.pharmacy.*</span>
          </div>

          <div className="space-y-2 max-h-80 overflow-y-auto pr-1">
            {saga.eventHistory.length === 0 ? (
              <div className="text-slate-500 py-10 text-center font-sans text-xs">
                No active execution. Select a scenario and click "Execute Saga Workflow" above.
              </div>
            ) : (
              saga.eventHistory.map((evt) => (
                <div
                  key={evt.id}
                  className={`p-2.5 rounded border text-[11px] ${
                    evt.status === 'error'
                      ? 'bg-rose-950/40 border-rose-800 text-rose-200'
                      : evt.status === 'compensated'
                      ? 'bg-amber-950/40 border-amber-800 text-amber-200'
                      : evt.status === 'published'
                      ? 'bg-slate-800/80 border-slate-700 text-slate-200'
                      : 'bg-emerald-950/30 border-emerald-800 text-emerald-200'
                  }`}
                >
                  <div className="flex items-center justify-between text-[10px] text-slate-400 mb-1">
                    <span>{evt.timestamp} · {evt.stepName}</span>
                    <span className="font-semibold uppercase tracking-wider">{evt.status}</span>
                  </div>
                  <div className="font-semibold text-white">{evt.payloadType}</div>
                  <div className="text-slate-300 mt-0.5">{evt.message}</div>
                  <div className="text-[10px] text-slate-400 mt-1">Topic: {evt.topic}</div>
                </div>
              ))
            )}
          </div>
        </div>
      </div>

      {/* Architectural Evaluation: Orchestration vs Choreography */}
      <div className="bg-white rounded-lg border border-slate-200 p-6 space-y-4">
        <h3 className="text-base font-bold text-slate-900 tracking-tight">
          Architectural Evaluation: Orchestration vs. Choreography in Healthcare (HIS)
        </h3>
        <p className="text-xs text-slate-600 leading-relaxed">
          While choreography is popular in basic e-commerce setups, healthcare information systems have strict statutory liability (FDA 21 CFR Part 11, JCAHO clinical safety) where an uncoordinated medication failure can result in medical malpractice.
        </p>

        <div className="overflow-x-auto">
          <table className="w-full text-left text-xs border-collapse">
            <thead>
              <tr className="bg-slate-50 border-b border-slate-200 text-slate-500 font-semibold uppercase text-[10px]">
                <th className="py-2.5 px-3">Evaluation Dimension</th>
                <th className="py-2.5 px-4">Choreography (Decentralized Events)</th>
                <th className="py-2.5 px-4 bg-slate-100 text-slate-900">Orchestration (Stateful Coordinator)</th>
                <th className="py-2.5 px-3">HIS Clinical Verdict</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-100 text-slate-700">
              <tr>
                <td className="py-2.5 px-3 font-semibold text-slate-900">Clinical Auditability</td>
                <td className="py-2.5 px-4 text-slate-600">State fragmented across 4 microservice DBs; difficult to reconstruct single timeline.</td>
                <td className="py-2.5 px-4 bg-slate-50 font-medium text-slate-900">Centralized state record in PostgreSQL with exact timestamps for each transition.</td>
                <td className="py-2.5 px-3 text-emerald-700 font-semibold">Orchestration Required for JCAHO Audits</td>
              </tr>
              <tr>
                <td className="py-2.5 px-3 font-semibold text-slate-900">Cyclic Deadlock Risk</td>
                <td className="py-2.5 px-4 text-slate-600">High risk of event ping-pong loops when multiple compensations overlap.</td>
                <td className="py-2.5 px-4 bg-slate-50 font-medium text-slate-900">Zero deadlock risk; star topology ensures commands and replies follow strict state table.</td>
                <td className="py-2.5 px-3 text-emerald-700 font-semibold">Orchestration Prevents Race Conditions</td>
              </tr>
              <tr>
                <td className="py-2.5 px-3 font-semibold text-slate-900">Compensation Determinism</td>
                <td className="py-2.5 px-4 text-slate-600">Each service must listen to every failure event and write custom rollback logic.</td>
                <td className="py-2.5 px-4 bg-slate-50 font-medium text-slate-900">Orchestrator explicitly issues compensating commands (ReleaseInventory, CancelClaim) in reverse order.</td>
                <td className="py-2.5 px-3 text-emerald-700 font-semibold">Guarantees Patient Safety & Financial Reconciliation</td>
              </tr>
              <tr>
                <td className="py-2.5 px-3 font-semibold text-slate-900">Operational Overhead</td>
                <td className="py-2.5 px-4 text-slate-600">Lower upfront setup, higher maintenance and debugging cost.</td>
                <td className="py-2.5 px-4 bg-slate-50 font-medium text-slate-900">Requires orchestrator service and PostgreSQL state table (~3ms latency overhead).</td>
                <td className="py-2.5 px-3 text-slate-600">Negligible latency impact in hospital workflows</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
};
