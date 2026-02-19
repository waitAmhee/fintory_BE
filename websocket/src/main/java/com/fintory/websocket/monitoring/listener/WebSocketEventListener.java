package com.fintory.websocket.monitoring.listener;

import com.fintory.websocket.monitoring.config.WebSocketMetrics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.messaging.SessionDisconnectEvent;

@Component
@RequiredArgsConstructor
@Slf4j
public class WebSocketEventListener {

    private final WebSocketMetrics webSocketMetrics;

    @EventListener
    public void handleSessionDisconnect(SessionDisconnectEvent event){
        webSocketMetrics.decrementConnection(); //연결 종료
    }
}
