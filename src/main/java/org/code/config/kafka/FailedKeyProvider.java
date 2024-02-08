package org.code.config.kafka;

import org.springframework.kafka.support.serializer.FailedDeserializationInfo;

import java.util.function.Function;

@SuppressWarnings("unused")
public class FailedKeyProvider implements Function<FailedDeserializationInfo, String> {
    @Override
    public String apply(FailedDeserializationInfo failedDeserializationInfo) {
        return null;
    }
}
