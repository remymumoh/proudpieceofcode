package org.code.config.kafka;

import com.google.protobuf.Message;
import org.springframework.kafka.support.serializer.FailedDeserializationInfo;

import java.util.function.Function;


@SuppressWarnings("unused")
public class FailedValueProvider implements Function<FailedDeserializationInfo, Message> {
    @Override
    public Message apply(FailedDeserializationInfo failedDeserializationInfo) {
        return null;
    }
}
