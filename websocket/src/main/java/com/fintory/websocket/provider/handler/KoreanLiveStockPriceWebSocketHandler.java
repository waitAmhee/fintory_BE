package com.fintory.websocket.provider.handler;

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
public class KoreanLiveStockPriceWebSocketHandler extends TextWebSocketHandler {

    private volatile WebSocketSession session;
    private final AtomicBoolean isConnected = new AtomicBoolean(false);

    private Consumer<LiveStockPriceStream> dataCallBack;
    private Consumer<LiveStockPriceStream> saveCallBack;

    private final ObjectMapper objectMapper;
    private final RedisTemplate<Object, Object> redisTemplate;

    private volatile CountDownLatch connectionLatch = new CountDownLatch(1);

    private final Object sendLock = new Object();


    // 데이터를 수신받을 때마다 호출되는 콜백 함수
    public void setDataCallBack(Consumer<LiveStockPriceStream> dataCallBack) {
        this.dataCallBack = dataCallBack;
    }


    //연결 상태 확인
    public boolean isConnected(){
        return isConnected.get() && session != null && session.isOpen();
    }

    //timeoutSeconds만큼 wait하도록 하는 동기화 함수
    public boolean waitForConnection(long timeoutSeconds){
        try{
            return connectionLatch.await(timeoutSeconds, TimeUnit.SECONDS);
        }catch(InterruptedException e){
            Thread.currentThread().interrupt();
            log.warn("웹소켓 연결 대기 중 인터럽트 발생");
            return false;
        }
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        this.session = session;
        this.isConnected.set(true);
        this.connectionLatch.countDown();
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status){
        this.session = null;
        isConnected.set(false);
        this.connectionLatch = new CountDownLatch(1);
        log.info("웹소켓 연결 종료");
    }


    //구독 메시지 전달
    public void subscribe(String code){
        try {
            sendSubscribeMessage(code);
            Thread.sleep(200);
        }catch (InterruptedException e){
            Thread.currentThread().interrupt();

        }
    }

    public void sendSubscribeMessage(String code) {
        if (!isConnected()) {
            log.warn("웹소켓이 연결되지 않아 구독 메시지를 보낼 수 없습니다. 종목: {}", code);
            throw new DomainException(DomainErrorCode.WEBSOCKET_CONNECTION_FAILED);
        }

        synchronized (sendLock) {
            try {

                String approvalKey = (String) redisTemplate.opsForValue().get("kis-websocket-access-token");

                if (approvalKey == null || approvalKey.isEmpty()) {
                    log.error("Redis에서 KIS 토큰을 찾을 수 없습니다.");
                    throw new DomainException(DomainErrorCode.TOKEN_NOT_FOUND);
                }

                Map<String, Object> message = Map.of(
                        "header", Map.of(
                                "approval_key", approvalKey,
                                "custtype", "P",
                                "tr_type", "1",
                                "content-type", "utf-8"
                        ),
                        "body", Map.of(
                                "input", Map.of(
                                        "tr_id", "H0STCNT0",
                                        "tr_key", code
                                )
                        )

                );

                String jsonMessage = objectMapper.writeValueAsString(message);
                session.sendMessage(new TextMessage(jsonMessage));
            } catch (Exception e) {
                log.error("KIS Developer 실시간 현재가 조회 시 요청 보내는 과정에서 에러 발생:{}", e.getMessage());
                throw new DomainException(DomainErrorCode.WEBSOCKET_SEND_FAILED);
            }
        }
    }

    // 구독 취소 요청
    public void unsubscribe(String code){
        sendUnsubscribeMessage(code);
    }

    public void sendUnsubscribeMessage(String code) {
        synchronized (sendLock) {
            try {
                if (session == null || !session.isOpen()) {
                    log.warn("WebSocket 세션이 닫혀있어 구독 해제 메시지를 보낼 수 없습니다. 종목: {}", code);
                    return; // 예외를 던지지 않고 그냥 리턴
                }

                String approvalKey = (String) redisTemplate.opsForValue().get("kis-websocket-access-token");

                if (approvalKey == null || approvalKey.isEmpty()) {
                    log.error("Redis에서 KIS 토큰을 찾을 수 없습니다.");
                    throw new DomainException(DomainErrorCode.TOKEN_NOT_FOUND);
                }

                Map<String, Object> message = Map.of(
                        "header", Map.of(
                                "approval_key", approvalKey,
                                "custtype", "P",
                                "tr_type", "2",
                                "content-type", "utf-8"
                        ),
                        "body", Map.of(
                                "input", Map.of(
                                        "tr_id", "H0STCNT0",
                                        "tr_key", code
                                )
                        )

                );
                String jsonMessage = objectMapper.writeValueAsString(message);
                session.sendMessage(new TextMessage(jsonMessage));
            } catch (Exception e) {
                log.error("KIS Developer 실시간 현재가 조회 세션 close시 요청 보내는 과정에서 에러 발생:{}", e.getMessage());
                throw new DomainException(DomainErrorCode.WEBSOCKET_SEND_FAILED);
            }
        }
    }

    //메시지를 받으면 실행되는 메소드
    public void handleTextMessage(WebSocketSession session, TextMessage message){
        String payload = message.getPayload();
        try{
            String[] fields = payload.split("\\^");
            if (fields.length < 40) return;

            String codeField = fields[0].trim();
            String code = codeField.contains("|")
                    ? codeField.split("\\|")[3]
                    : codeField;

            BigDecimal price = parseBigDecimal(fields[2]);
            BigDecimal change = parseBigDecimal(fields[4].replace("+", ""));
            BigDecimal changePercent = parseBigDecimal(fields[5].replace("%", "").replace("+", ""));

            LiveStockPriceStream stockData = new LiveStockPriceStream(
                    code, price, change, changePercent
            );

            executeCallbacks(stockData);

        }catch(Exception e){
            log.error("KIS Developer 실시간 현재가 조회 시 응답 받는 과정에서 에러 발생:{}",e.getMessage());
            throw new DomainException(DomainErrorCode.WEBSOCKET_MESSAGE_PARSE_FAILED);
        }
    }

    private BigDecimal parseBigDecimal(String value){
        try{
            return new BigDecimal(value);
        }catch(NumberFormatException e){
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
        log.error("국내 주식 웹소켓 전송 에러 발생: {}", exception.getMessage());
        isConnected.set(false);
    }

}