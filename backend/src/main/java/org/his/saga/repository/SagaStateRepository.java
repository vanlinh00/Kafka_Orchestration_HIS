package org.his.saga.repository;

import org.his.saga.model.PharmacySagaState;
import org.his.saga.model.SagaStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface SagaStateRepository extends JpaRepository<PharmacySagaState, String> {

    Optional<PharmacySagaState> findByOrderId(String orderId);

    List<PharmacySagaState> findByPatientId(String patientId);

    List<PharmacySagaState> findByStatus(SagaStatus status);
}
