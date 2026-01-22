package com.fintory.websocket.publisher.service;

import com.fintory.domain.stock.dto.websocket.LiveStockPriceStream;
import com.fintory.websocket.monitoring.config.SSEMetrics;
import com.fintory.websocket.publisher.handler.StockStreamBridge;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Lazy;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;
import io.micrometer.core.instrument.Timer;

import java.math.BigDecimal;
import java.util.Map;

@Service
@Slf4j
public class StockDataProcessService {
    private final SSEMetrics SSEMetrics;
    private final StockStreamBridge stockStreamBridge;
    private final Timer dataProcessingTime;
    private final LiveStockPriceWebSocketSaverService liveStockPriceWebSocketSaverService;
    private final RedisTemplate<Object,Object> redisTemplate;
    private static final String PRICE_ALERT_CHANNEL = "price:alert:channel";

    public StockDataProcessService(@Lazy SSEMetrics SSEMetrics, StockStreamBridge stockStreamBridge,
                                   MeterRegistry meterRegistry,
                                   LiveStockPriceWebSocketSaverService liveStockPriceWebSocketSaverService, RedisTemplate<Object, Object> redisTemplate) {
        this.SSEMetrics = SSEMetrics;
        this.stockStreamBridge = stockStreamBridge;
        this.dataProcessingTime = Timer.builder("websocket.data.processing.time")
                .description("Time to process and send stock data")
                .publishPercentiles(0.5,0.95,0.99)
                .register(meterRegistry);
        this.liveStockPriceWebSocketSaverService = liveStockPriceWebSocketSaverService;
        this.redisTemplate = redisTemplate;
    }

    //웹소켓으로 받은 데이터를 처리하는 메서드
    public void processStreamData(LiveStockPriceStream dto,
                                   Map<String, LiveStockPriceStream> previousData,
                                   Map<String, LiveStockPriceStream> pendingData,
                                   String marketName) {
        LiveStockPriceStream previous = previousData.get(dto.code());

        Timer.Sample sample = Timer.start();
        try {
            //이전 데이터와 비교하여 중복 체크
            if (previous != null && previous.equals(dto)) {
                return; //똑같은 데이터면 무시
            }
            //새로운 데이터를 받으면 -> 감시가 이벤트 발행
            // @EventListener는 같은 JVM 내에서만 동작함 -> 다른 통신 방법 필요 -> redis pub/sub 활용
            /* 알림 기능 -> 잠깐 미룬 상태
            try {

                redisTemplate.convertAndSend(PRICE_ALERT_CHANNEL, dto);
            } catch (Exception e) {
                log.error("Redis Pub/Sub 전송 실패: {}", dto.code(), e);
            }*/

            //스케쥴러 + 웹소켓 연결 시작하자마자 받은 데이터 값(첫 데이터) 저장
            if (previous == null) {
                try {
                    liveStockPriceWebSocketSaverService.saveStockData(dto); //DB에 바로 저장
                } catch (Exception e) {
                    // 실패 시 배치 저장을 위해 pendingData에 보관
                    pendingData.put(dto.code(), dto);
                    log.error("{} 종목 {} 실시간 저장 실패, 배치 저장 대기: {}", marketName, dto.code(), e.getMessage());
                }
            }

            //새로운 데이터면 다음 중복 체크용으로 저장
            previousData.put(dto.code(), dto);
            pendingData.put(dto.code(), dto); //배치 저장 대기
            sendStockData(dto.code(), dto); //클라이언트에게 전송
        }finally {
            sample.stop(dataProcessingTime);

        }
    }

    public void sendStockData(String stockCode, Object stockData) {
        if (stockData instanceof LiveStockPriceStream stream) {
            /*
            if (stream.priceChange() == null || stream.priceChange().compareTo(BigDecimal.ZERO) == 0) {
                return;
            }*/
            stockStreamBridge.publish((LiveStockPriceStream) stockData);
        }
    }


}
