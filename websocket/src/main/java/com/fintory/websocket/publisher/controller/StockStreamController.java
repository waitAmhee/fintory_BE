package com.fintory.websocket.publisher.controller;

import com.fintory.domain.stock.dto.websocket.LiveStockPriceStream;
import com.fintory.websocket.monitoring.config.SSEMetrics;
import com.fintory.websocket.publisher.handler.StockStreamBridge;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.catalina.Server;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;


@RestController
@RequestMapping("/api/stock")
@RequiredArgsConstructor
@Slf4j
public class StockStreamController {

    private final StockStreamBridge stockStreamBridge;
    private final SSEMetrics sseMetrics;

    @GetMapping(value="/live-price",produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<LiveStockPriceStream>> streamAll(){
        return stockStreamBridge.getStream()
                .map(data-> {
                    // 메시지 수신 시각 기록
                    long startTime = System.currentTimeMillis();

                    ServerSentEvent<LiveStockPriceStream> event = ServerSentEvent.<LiveStockPriceStream>builder()
                            .data(data)
                            .build();

                    //메시지 처리 지연 측정
                    sseMetrics.recordLatency(startTime);

                    return event;
                })
                .onBackpressureLatest()
                .doOnSubscribe(sub->{
                    sseMetrics.incrementConnection();
                })
                .doOnCancel(()->{
                    sseMetrics.decrementConnection();
                })
                .doOnComplete(()->{
                    sseMetrics.decrementConnection();
                })
                .doOnError(error->{
                    sseMetrics.decrementConnection();
                    sseMetrics.incrementMessageFailed();
                })
                .doOnDiscard(LiveStockPriceStream.class, discarded->{
                    //백프레셔로 버려진 로그
                });
    }
}
