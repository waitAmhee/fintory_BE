package com.fintory.websocket.monitoring.config;

import com.fintory.websocket.publisher.service.LiveStockPriceWebSocketService;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.PostConstruct;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

//REVIEW 혹시 해당 파일의 위치를 바꾸길 원하시면 리뷰 주세요! ->WebSocketMetrics는 Micrometer와 Prometheus 같은 외부 기술에 의존하기 때문에 infra 모듈에 위치시켰습니다
@Component
public class WebSocketMetrics {

    private final LiveStockPriceWebSocketService websocketService;
    private final MeterRegistry meterRegistry;
    private final AtomicInteger activeConnections = new AtomicInteger(0);
    private Counter messagesFailed;
    private Timer messageLatency;

    //REVIEW @Lazy를 쓰기 위해 명시적 생성자 사용 -> @Lazy는 생성자 파라미터에 직접 붙어 있어야 동작함
    // @RequiredConstructor는 생성자 파라미터별 어노테이션을 직접 지원하지 않는 것으로 알고 있음.
    public WebSocketMetrics(@Lazy LiveStockPriceWebSocketService websocketService, MeterRegistry meterRegistry) {
        this.websocketService = websocketService;
        this.meterRegistry = meterRegistry;
    }

    @PostConstruct
    public void registerMetrics() {


        // 1. 활성 웹소켓 연결 수 -> stomp는 논리적 연결 수
        Gauge.builder("websocket.connections.active", activeConnections, AtomicInteger::get)
                .description("Active WebSocket connections")
                .register(meterRegistry);

        // 2. 메시지 전송 실패 수
        this.messagesFailed = Counter.builder("websocket.messages.failed")
                .description("Message failed to send")
                .register(meterRegistry);

        // 3. 메시지 처리 지연
        this.messageLatency = Timer.builder("websocket.message.latency")
                .description("Message processing latency")
                .register(meterRegistry);
    }
    // 연결 관리
    public void incrementConnection() {
        activeConnections.incrementAndGet();
    }

    public void decrementConnection() {
        activeConnections.decrementAndGet();
    }

    public void incrementMessageFailed(){
        messagesFailed.increment();
    }

    public void recoredLatency(long startTimeMillis){
        long duration = System.currentTimeMillis() - startTimeMillis;
        messageLatency.record(duration, TimeUnit.MILLISECONDS);
    }
}


