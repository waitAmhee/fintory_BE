package com.fintory.websocket.monitoring.config;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Component
@RequiredArgsConstructor
public class SSEMetrics {

    private final MeterRegistry meterRegistry;
    private final AtomicInteger activeConnections = new AtomicInteger(0);
    private final AtomicInteger activeSubscribers = new AtomicInteger(0);
    private Counter messagesFailed;
    private Timer messageLatency;

    @PostConstruct
    public void registerMetrics() {

        // 1. 활성 SSE 연결 수
        Gauge.builder("sse.connections.active",
                        activeConnections, AtomicInteger::get)
                .description("Active SSE connections")
                .register(meterRegistry);

        // 2. 메시지 처리 지연
        this.messageLatency = Timer.builder("sse.messages.latency")
                .description("\"Message processing latency")
                .register(meterRegistry);

        //3. 메시지 전송 실패 수
        this.messagesFailed = Counter.builder("sse.messages.failed")
                .description("Messages failed to send")
                .register(meterRegistry);
    }

    // 연결 관리
    public void incrementConnection() {
        activeConnections.incrementAndGet();
    }

    public void decrementConnection() {
        activeConnections.decrementAndGet();
    }

    // 메시지 실패
    public void incrementMessageFailed(){
        messagesFailed.increment();
    }

    //메시지 지연 측정
    public void recordLatency(long startTimeMillis){
        long duration = System.currentTimeMillis() - startTimeMillis;
        messageLatency.record(duration, TimeUnit.MILLISECONDS);
    }
}