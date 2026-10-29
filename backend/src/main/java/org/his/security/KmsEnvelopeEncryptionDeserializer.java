package org.his.security;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.stereotype.Component;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * KmsEnvelopeEncryptionDeserializer
 *
 * Intercepts incoming Kafka byte records:
 * 1. Reads IV from binary envelope
 * 2. Fetches DEK from KMS for patient
 * 3. If DEK has been crypto-shredded, throws SecurityException to halt PHI leakage.
 */
@Component
@RequiredArgsConstructor
public class KmsEnvelopeEncryptionDeserializer implements Deserializer<byte[]> {

    private final KmsKeyManagementService kmsService;
    private static final int GCM_TAG_LENGTH_BITS = 128;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {}

    @Override
    public byte[] deserialize(String topic, byte[] data) {
        return deserialize(topic, null, data);
    }

    @Override
    @SneakyThrows
    public byte[] deserialize(String topic, Headers headers, byte[] payload) {
        if (payload == null || payload.length == 0) {
            return payload;
        }

        String patientId = extractHeader(headers, "ce_patient_id");
        if (patientId == null) {
            patientId = "FALLBACK_GLOBAL_PATIENT";
        }

        // Resolves DEK; throws SecurityException if DEK was crypto-shredded
        KmsKeyManagementService.EncryptedDek dek = kmsService.getActiveDek(patientId);

        ByteBuffer buffer = ByteBuffer.wrap(payload);
        int ivLength = buffer.get();
        byte[] iv = new byte[ivLength];
        buffer.get(iv);

        byte[] ciphertext = new byte[buffer.remaining()];
        buffer.get(ciphertext);

        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        SecretKey secretKey = new SecretKeySpec(dek.getPlaintextKeyBytes(), "AES");
        cipher.init(Cipher.DECRYPT_MODE, secretKey, new GCMParameterSpec(GCM_TAG_LENGTH_BITS, iv));

        // Verify topic AAD matches original topic
        cipher.updateAAD(topic.getBytes(StandardCharsets.UTF_8));
        return cipher.doFinal(ciphertext);
    }

    private String extractHeader(Headers headers, String key) {
        if (headers == null) return null;
        Header header = headers.lastHeader(key);
        return header != null ? new String(header.value(), StandardCharsets.UTF_8) : null;
    }

    @Override
    public void close() {}
}
