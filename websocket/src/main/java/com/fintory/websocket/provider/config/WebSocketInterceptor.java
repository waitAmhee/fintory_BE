package com.fintory.websocket.provider.config;

import com.fintory.websocket.monitoring.config.WebSocketMetrics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.server.ServerHttpRequest;
import org.springframework.http.server.ServerHttpResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.WebSocketHandler;
import org.springframework.web.socket.server.HandshakeInterceptor;

import java.util.Map;

@Component
@RequiredArgsConstructor
@Slf4j
public class WebSocketInterceptor implements HandshakeInterceptor {

    private final WebSocketMetrics webSocketMetrics;

    @Override
    public boolean beforeHandshake(ServerHttpRequest request, ServerHttpResponse response, WebSocketHandler wsHandler, Map<String, Object> attributes) throws Exception {

        //웹소켓 연결 성공시
        webSocketMetrics.incrementConnection();
        return true; //핸드셰이크 계속 진행

    }

    @Override
    public void afterHandshake(ServerHttpRequest request, ServerHttpResponse response, WebSocketHandler wsHandler, Exception exception) {
        if (exception != null) {
            log.error("WebSocket 핸드셰이크 실패: ", exception);
            //Handshake 실패시 카운트 롤백
            webSocketMetrics.decrementConnection();
        } else {
            log.info("WebSocket 핸드셰이크 성공!");
        }
    }
}
