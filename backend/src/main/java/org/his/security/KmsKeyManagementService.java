package org.his.security;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Key Management Service (KMS) integration for HIPAA & GDPR compliance.
 * Manages per-patient Data Encryption Keys (DEKs) and handles Cryptographic Erasure (Crypto-Shredding).
 */
@Slf4j
@Service
public class KmsKeyManagementService {

    // In-memory simulation cache; in production backed by AWS KMS, Google Cloud KMS, or HashiCorp Vault
    private final Map<String, EncryptedDek> patientKeyStore = new ConcurrentHashMap<>();

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class EncryptedDek {
        private String keyId;
        private String patientId;
        private byte[] plaintextKeyBytes;
        private byte[] encryptedKeyBlob;
        private boolean shredded;
    }

    /**
     * Retrieves or generates a unique 256-bit AES Data Encryption Key (DEK) per patient.
     */
    public EncryptedDek getOrCreatePatientDek(String patientId) {
        return patientKeyStore.computeIfAbsent(patientId, id -> {
            try {
                KeyGenerator keyGen = KeyGenerator.getInstance("AES");
                keyGen.init(256, new SecureRandom());
                SecretKey secretKey = keyGen.generateKey();

                byte[] rawKey = secretKey.getEncoded();
                String keyId = "kms/arn:his:kms:dek-" + id;

                log.info("Generated new AES-256 DEK for Patient MRN: {} (KeyId: {})", id, keyId);
                return EncryptedDek.builder()
                        .keyId(keyId)
                        .patientId(id)
                        .plaintextKeyBytes(rawKey)
                        .encryptedKeyBlob(rawKey) // In production: ciphertext from KMS Encrypt API
                        .shredded(false)
                        .build();
            } catch (NoSuchAlgorithmException e) {
                throw new IllegalStateException("Failed to initialize AES key generator", e);
            }
        });
    }

    /**
     * GDPR Article 17 "Right to be Forgotten" - Cryptographic Shredding.
     * Deleting the DEK in KMS permanently scrambles historical immutable Kafka log entries.
     */
    public void cryptoShredPatient(String patientId) {
        EncryptedDek dek = patientKeyStore.get(patientId);
        if (dek != null) {
            // Overwrite plaintext key in memory with zeroes
            java.util.Arrays.fill(dek.getPlaintextKeyBytes(), (byte) 0);
            dek.setShredded(true);
            patientKeyStore.remove(patientId);
            log.warn("CRYPTO-SHREDDED DEK for Patient MRN: {}. Historical Kafka logs are now mathematically unrecoverable.", patientId);
        }
    }

    public EncryptedDek getActiveDek(String patientId) {
        EncryptedDek dek = patientKeyStore.get(patientId);
        if (dek == null || dek.isShredded()) {
            throw new SecurityException("CryptoShreddedException: DEK for patient " + patientId + " has been revoked or purged.");
        }
        return dek;
    }
}
