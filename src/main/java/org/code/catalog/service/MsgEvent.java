package org.code.catalog.service;

import lombok.Builder;
import lombok.Data;

@Builder
@Data
public class MsgEvent {
    private String msgRef;
    private EventType type;
}
