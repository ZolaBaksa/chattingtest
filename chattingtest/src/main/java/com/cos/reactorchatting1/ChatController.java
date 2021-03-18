package com.cos.reactorchatting1;


import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

@CrossOrigin
@RestController
public class ChatController {

	// 중간에 데이터가 들어와도 지속적인 응답	
	Sinks.Many<String> sink;// 구독할 자료형을 String으로 지정
	
	public ChatController() {
		this.sink = Sinks.many().multicast().onBackpressureBuffer();
	}

	// mulitcast() 새로 들어온 데이터만 응답받음 hot (시퀀스 = 스트림)
	// replay() 기존 데이터 + 새로운 데이터 응답 cold 시퀀스
	
	@PostMapping("/chat")
	public void chat(@RequestBody ChatReqDto chatReqDto) {
	sink.tryEmitNext(chatReqDto.getNickname()+ " : "+chatReqDto.getChat());	
	}

	//구독하기
	// data : 실제값 \n\n
	@GetMapping(value = "/sse")
	public Flux<ServerSentEvent<String>> sse() {//ServerSendEvent의 ContentType은 text event Stream 
		return sink.asFlux().map(e -> ServerSentEvent.builder(e).build()).doOnCancel(()->{
System.out.println("SSE 종료됨");
sink.asFlux().blockLast();
		}); //구독
	}
	
}
