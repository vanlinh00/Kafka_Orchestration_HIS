package org.his.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.his.saga.PharmacyOrderSagaOrchestrator;
import org.his.saga.dto.SagaCommandsAndEvents.ProcessPrescriptionRequest;
import org.his.saga.model.PharmacySagaState;
import org.his.saga.repository.SagaStateRepository;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.util.Map;
import java.util.Optional;

/**
 * REST API Controller for triggering and monitoring the Pharmacy Order Saga.
 */
@Slf4j
@RestController
@RequestMapping("/api/v1/pharmacy/orders")
@RequiredArgsConstructor
public class PrescriptionOrderController {

    private final PharmacyOrderSagaOrchestrator sagaOrchestrator;
    private final SagaStateRepository sagaRepository;

    /**
     * Trigger: The API receives a request to process a prescription.
     */
    @PostMapping("/process")
    public ResponseEntity<Map<String, Object>> processPrescription(@RequestBody ProcessPrescriptionRequest request) {
        if (request.getPrice() == null) {
            request.setPrice(new BigDecimal("50.00")); // Standard copay/price default
        }

        log.info("Received REST API request to process prescription {} for Patient {}",
                request.getPrescriptionId(), request.getPatientId());

        String sagaId = sagaOrchestrator.startPharmacyOrderSaga(request);

        return ResponseEntity.accepted().body(Map.of(
                "status", "SAGA_INITIATED",
                "sagaId", sagaId,
                "message", "Prescription processing initiated across Pharmacy, Billing, and Patient services.",
                "patientId", request.getPatientId(),
                "medicationCode", request.getMedicationCode(),
                "cost", request.getPrice()
        ));
    }

    /**
     * Inspect current Saga state and step progression.
     */
    @GetMapping("/saga/{sagaId}")
    public ResponseEntity<?> getSagaStatus(@PathVariable String sagaId) {
        Optional<PharmacySagaState> stateOpt = sagaRepository.findById(sagaId);
        if (stateOpt.isEmpty()) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(stateOpt.get());
    }
}
