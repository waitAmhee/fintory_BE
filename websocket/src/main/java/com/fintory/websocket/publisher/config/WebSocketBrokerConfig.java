package com.fintory.websocket.publisher.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.web.socket.config.annotation.EnableWebSocketMessageBroker;
import org.springframework.web.socket.config.annotation.StompEndpointRegistry;
import org.springframework.web.socket.config.annotation.WebSocketMessageBrokerConfigurer;
import org.springframework.web.socket.server.HandshakeInterceptor;

//클라이언트들이 내 서버에 연결하도록 설정하는 코드
//REVIEW 일반적인 어플에서도 시세 데이터는 별도의 로그인 과정 없이도 조회가 가능해서 핸드셰이크 인터셉터 설정x(jwt 토큰 인증x)
@Configuration
@EnableWebSocketMessageBroker
public class WebSocketBrokerConfig implements WebSocketMessageBrokerConfigurer {

    private final HandshakeInterceptor handshakeInterceptor;

    public WebSocketBrokerConfig(HandshakeInterceptor handshakeInterceptor) {
        this.handshakeInterceptor = handshakeInterceptor;
    }

    @Bean(name = "webSocketTaskScheduler")
    public TaskScheduler messageBrokerTaskScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(1);
        scheduler.setThreadNamePrefix("webSocket-heartbeat-thread-");
        return scheduler;
    }
    @Override
    public void configureMessageBroker(MessageBrokerRegistry config) {
        config.enableSimpleBroker("/topic") // 서버 -> 클라이언트
              .setHeartbeatValue(new long[]{10000, 10000})
              .setTaskScheduler(messageBrokerTaskScheduler());

        config.setApplicationDestinationPrefixes("/app"); //클라이언트 -> 서버
    }

    @Override
    public void registerStompEndpoints(StompEndpointRegistry registry) {
        registry.addEndpoint("/ws")
                .setAllowedOriginPatterns("*")
                        .addInterceptors(handshakeInterceptor);

        registry.addEndpoint("/ws-sockjs") // 초기 웹소켓 연결을 위한 경로
                .setAllowedOriginPatterns("*") //cors 설정
                .addInterceptors(handshakeInterceptor)
                .withSockJS(); //구형 브라우저를 위한 폴백


    }
}
