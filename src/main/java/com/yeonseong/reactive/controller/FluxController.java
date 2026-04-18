package com.yeonseong.reactive.controller;

import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
public class FluxController {

  @RequestMapping("/hello")
  public Publisher<String> hello(String name) {
    return new Publisher<String>() {
      @Override
      public void subscribe(Subscriber<? super String> subscriber) {

        subscriber.onSubscribe(new Subscription() {
          @Override
          public void request(long l) {
            subscriber.onNext("Hello " + name);
            subscriber.onComplete();
          }

          @Override
          public void cancel() {

          }
        });
      }
    };
  }
}
