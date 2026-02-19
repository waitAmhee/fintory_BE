package com.fintory.websocket.provider.handler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fintory.common.exception.DomainErrorCode;
import com.fintory.common.exception.DomainException;
import com.fintory.domain.stock.dto.websocket.LiveStockPriceStream;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

@Service
@Slf4j
@RequiredArgsConstructor
public class OverseasLiveStockPriceWebSocketHandler extends TextWebSocketHandler {

    private volatile WebSocketSession session;
    private final AtomicBoolean isConnected = new AtomicBoolean(false);

    private Consumer<LiveStockPriceStream> dataCallBack;
    private Consumer<LiveStockPriceStream> saveCallBack;

    private final ObjectMapper objectMapper;
    private final RedisTemplate<Object, Object> redisTemplate;

    private volatile CountDownLatch connectionLatch = new CountDownLatch(1);
    private final Object sendLock = new Object(); // 동기화용 락

    // 콜백 함수 설정
    public void setDataCallBack(Consumer<LiveStockPriceStream> dataCallBack) {
        this.dataCallBack = dataCallBack;
    }

    // 연결 상태 확인
    public boolean isConnected() {
        return isConnected.get() && session != null && session.isOpen();
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        this.session = session;
        this.isConnected.set(true);
        this.connectionLatch.countDown();
    }

    // 연결 대기
    public boolean waitForConnection(long timeoutSeconds) {
        try {
            return connectionLatch.await(timeoutSeconds, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("웹소켓 연결 대기 중 인터럽트 발생");
            return false;
        }
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        this.session = null;
        isConnected.set(false);
        this.connectionLatch = new CountDownLatch(1);
        log.info("해외 주식 웹소켓 연결 종료 - 상태: {}, 코드: {}", status.getReason(), status.getCode());
    }

    // 구독 메시지 전달
    public void subscribe(String code) {
        try {
            sendSubscribeMessage(code);
            TimeUnit.SECONDS.sleep(5); // 5초 대기
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("구독 요청 중 인터럽트 발생 - 종목: {}", code);
        }
    }

    public void sendSubscribeMessage(String code) {
        // 연결 상태 확인
        if (!isConnected()) {
            log.warn("웹소켓이 연결되지 않아 구독 메시지를 보낼 수 없습니다. 종목: {}", code);
            throw new DomainException(DomainErrorCode.WEBSOCKET_CONNECTION_FAILED);
        }

        // 동시 전송 방지
        synchronized (sendLock) {
            try {
                String token = (String) redisTemplate.opsForValue().get("db-access-token");

                if (token == null || token.trim().isEmpty()) {
                    log.error("Redis에서 DB 토큰을 찾을 수 없습니다.");
                    throw new DomainException(DomainErrorCode.TOKEN_NOT_FOUND);
                }

                Map<String, String> header = Map.of(
                        "token", token,
                        "tr_type", "1"
                );

                Map<String, String> body = Map.of(
                        "tr_cd", "V60",
                        "tr_key", "FN" + code
                );

                Map<String, Object> request = Map.of(
                        "header", header,
                        "body", body
                );

                String message = objectMapper.writeValueAsString(request);
                session.sendMessage(new TextMessage(message));

            } catch (Exception e) {
                log.error("DB API 실시간 현재가 데이터 조회 메시지 요청 중 에러 발생 - 종목: {}, 에러: {}", code, e.getMessage());
                throw new DomainException(DomainErrorCode.WEBSOCKET_SEND_FAILED);
            }
        }
    }

    // 구독 취소 요청
    public void unsubscribe(String code) {
        sendUnsubscribeMessage(code);
    }

    public void sendUnsubscribeMessage(String code) {
        if (!isConnected()) {
            log.warn("웹소켓 세션이 닫혀있어 구독 해제 메시지를 보낼 수 없습니다. 종목: {}", code);
            return;
        }

        synchronized (sendLock) {
            try {
                String token = (String) redisTemplate.opsForValue().get("db-access-token");

                if (token == null || token.trim().isEmpty()) {
                    log.error("Redis에서 DB 토큰을 찾을 수 없습니다.");
                    throw new DomainException(DomainErrorCode.TOKEN_NOT_FOUND);
                }

                Map<String, String> header = Map.of(
                        "token", token,
                        "tr_type", "2"
                );

                Map<String, String> body = Map.of(
                        "tr_cd", "V60",
                        "tr_key", "FN" + code
                );

                Map<String, Object> request = Map.of(
                        "header", header,
                        "body", body
                );

                String jsonMessage = objectMapper.writeValueAsString(request);

                session.sendMessage(new TextMessage(jsonMessage));

            } catch (Exception e) {
                log.error("DB API 실시간 현재가 데이터 구독 해제 요청 중 에러 발생 - 종목: {}, 에러: {}", code, e.getMessage());
                // 구독 해제는 실패해도 예외를 던지지 않음
            }
        }
    }

    // 메시지 수신 처리
    @Override
    public void handleTextMessage(WebSocketSession session, TextMessage message) {
        String payload = message.getPayload();
        synchronized (sendLock) {
            try {
                JsonNode root = objectMapper.readTree(payload);
                JsonNode header =  root.get("header");
                JsonNode body = root.get("body");

                //+ 구독 확인 요청도 자연스럽게 해결
                if (body != null && body.has("symbol")) {
                    parseAndProcessMessage(payload);
                } else if (header != null && header.has("tr_type") && "2".equals(header.get("tr_type").asText())) {
                    log.info("구독 해제 요청 완료");
                }

            } catch (Exception e) {
                log.error("메시지 처리 중 에러 발생 - payload: {}, 에러: {}", payload, e.getMessage());
                // 메시지 파싱 실패는 전체 연결을 끊지 않음
            }
        }
    }

    private void parseAndProcessMessage(String payload) {

        try {
            JsonNode root = objectMapper.readTree(payload);
            JsonNode body = root.get("body");

            // 안전한 필드 추출
            String symbol = getTextValue(body, "symbol");

            BigDecimal last = parseBigDecimal(getTextValue(body, "last"));
            BigDecimal diff = parseBigDecimal(getTextValue(body, "diff"));
            BigDecimal rate = parseBigDecimal(getTextValue(body, "rate"));

            LiveStockPriceStream stockData = new LiveStockPriceStream(
                    symbol.substring(2), // FN 접두사 제거
                    last,
                    diff,
                    rate
            );

            // 콜백 실행
            executeCallbacks(stockData);

        } catch (Exception e) {
            log.error("메시지 파싱 중 에러 발생: {}", e.getMessage());
            throw new DomainException(DomainErrorCode.WEBSOCKET_MESSAGE_PARSE_FAILED);
        }
    }

    private String getTextValue(JsonNode node, String fieldName) {
        JsonNode field = node.get(fieldName);
        return field != null ? field.asText() : null;
    }

    private BigDecimal parseBigDecimal(String value) {
        if (value == null || value.trim().isEmpty()) {
            return BigDecimal.ZERO;
        }

        try {
            return new BigDecimal(value.trim());
        } catch (NumberFormatException e) {
            log.warn("숫자 파싱 실패 - 값: {}, 0으로 대체", value);
            return BigDecimal.ZERO;
        }
    }

    private void executeCallbacks(LiveStockPriceStream stockData) {
        try {
            if (dataCallBack != null) {
                dataCallBack.accept(stockData);
            }
        } catch (Exception e) {
            log.error("데이터 콜백 실행 중 에러: {}", e.getMessage());
        }

        try {
            if (saveCallBack != null) {
                saveCallBack.accept(stockData);
            }
        } catch (Exception e) {
            log.error("저장 콜백 실행 중 에러: {}", e.getMessage());
        }
    }

    @Override
    public void handleTransportError(WebSocketSession session, Throwable exception) {
        log.error("해외 주식 웹소켓 전송 에러 발생: {}", exception.getMessage());
        isConnected.set(false);
    }
}