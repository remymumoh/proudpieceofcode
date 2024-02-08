package org.code.catalog.service;

import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

@Service
public class CatalogQueueService {

   private final Sinks.Many<MsgEvent> sink;

    public CatalogQueueService() {
        // Use unicast to ensure that any messages received before subscription are delivered
        this.sink = Sinks.many().unicast()
            .onBackpressureBuffer();
    }

    public void publish(String eventRef, EventType type){
            var msg = MsgEvent.builder().msgRef(eventRef).type(type).build();
            sink.tryEmitNext(msg);
    }

    public Flux<MsgEvent> getEventStream(){
        return sink.asFlux();
    }

}
