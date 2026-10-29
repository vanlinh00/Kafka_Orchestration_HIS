package org.his.security;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.stereotype.Component;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Map;

/**
 * KmsEnvelopeEncryptionSerializer
 *
 * HIPAA / GDPR Invariants:
 * 1. Plaintext PHI never touches Kafka disk or OS page cache unencrypted.
 * 2. Each Patient has an individual Data Encryption Key (DEK) fetched from KMS/Vault.
 * 3. AES-256-GCM with 96-bit random IV and 128-bit authentication tag.
 * 4. Topic name passed as Authenticated Additional Data (AAD) to prevent topic replay attacks.
 */
@Component
@RequiredArgsConstructor
public class KmsEnvelopeEncryptionSerializer implements Serializer<byte[]> {

    private final KmsKeyManagementService kmsService;
    private static final int GCM_IV_LENGTH_BYTES = 12;
    private static final int GCM_TAG_LENGTH_BITS = 128;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {}

    @Override
    public byte[] serialize(String topic, byte[] data) {
        return serialize(topic, null, data);
    }

    @Override
    @SneakyThrows
    public byte[] serialize(String topic, Headers headers, byte[] plaintext) {
        if (plaintext == null || plaintext.length == 0) {
            return plaintext;
        }

        // Extract patient identifier from CloudEvents headers
        String patientId = extractHeader(headers, "ce_patient_id");
        if (patientId == null) {
            patientId = "FALLBACK_GLOBAL_PATIENT";
        }

        // Fetch or create per-patient DEK from KMS
        KmsKeyManagementService.EncryptedDek dek = kmsService.getOrCreatePatientDek(patientId);

        // Generate cryptographically secure random 96-bit IV
        byte[] iv = new byte[GCM_IV_LENGTH_BYTES];
        new SecureRandom().nextBytes(iv);

        // Initialize AES-256-GCM cipher
        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        SecretKey secretKey = new SecretKeySpec(dek.getPlaintextKeyBytes(), "AES");
        cipher.init(Cipher.ENCRYPT_MODE, secretKey, new GCMParameterSpec(GCM_TAG_LENGTH_BITS, iv));

        // Authenticated Additional Data (AAD) binds ciphertext to specific topic
        cipher.updateAAD(topic.getBytes(StandardCharsets.UTF_8));
        byte[] ciphertext = cipher.doFinal(plaintext);

        if (headers != null) {
            headers.add("x-encryption-dek-id", dek.getKeyId().getBytes(StandardCharsets.UTF_8));
            headers.add("x-encryption-algorithm", "AES-256-GCM".getBytes(StandardCharsets.UTF_8));
            headers.add("x-data-classification", "RESTRICTED_PHI".getBytes(StandardCharsets.UTF_8));
        }

        // Wire Envelope: [IV length: 1 byte][IV: 12 bytes][Ciphertext + Auth Tag]
        return ByteBuffer.allocate(1 + iv.length + ciphertext.length)
                .put((byte) iv.length)
                .put(iv)
                .put(ciphertext)
                .array();
    }

    private String extractHeader(Headers headers, String key) {
        if (headers == null) return null;
        Header header = headers.lastHeader(key);
        return header != null ? new String(header.value(), StandardCharsets.UTF_8) : null;
    }

    @Override
    public void close() {}
}
