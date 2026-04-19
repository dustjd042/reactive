package com.yeonseong.reactive;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;

@Slf4j
public class SchedulerTest {

  @Test
  public void Scheduler() {

    Publisher<Integer> publisher = subscriber -> {
      subscriber.onSubscribe(new Subscription() {
        @Override
        public void request(long l) {
          log.info("request");
          subscriber.onNext(1);
          subscriber.onNext(2);
          subscriber.onNext(3);
          subscriber.onNext(4);
          subscriber.onNext(5);
          subscriber.onComplete();
        }

        @Override
        public void cancel() {
          log.info("cancel");
        }
      });
    };

    Publisher<Integer> subscribeOn = subscriber -> {

      ExecutorService executorService = Executors.newSingleThreadExecutor(new CustomizableThreadFactory() {
          @Override
          public String getThreadNamePrefix() {return "subscribeOn-";}
      });
      executorService.execute(()->publisher.subscribe(subscriber));
    };

    Publisher<Integer> publishOn = subscriber -> {
      subscribeOn.subscribe(new Subscriber<Integer>() {
        ExecutorService executorService = Executors.newSingleThreadExecutor(new CustomizableThreadFactory() {
          @Override
          public String getThreadNamePrefix() {return "publishOn-";}
        });

        @Override
        public void onSubscribe(Subscription subscription) {
          subscriber.onSubscribe(subscription);
        }

        @Override
        public void onNext(Integer integer) {
          executorService.execute(()->subscriber.onNext(integer));
        }

        @Override
        public void onError(Throwable throwable) {
          executorService.execute(()->subscriber.onError(throwable));
        }

        @Override
        public void onComplete() {
          executorService.execute(subscriber::onComplete);
        }
      });
    };

    Subscriber<Integer> subscriber = new Subscriber<Integer>() {
      @Override
      public void onSubscribe(Subscription subscription) {
        log.info("onSubscribe");
        subscription.request(Long.MAX_VALUE);
      }

      @Override
      public void onNext(Integer integer) {
        log.info("onNext: {}", integer);

      }

      @Override
      public void onError(Throwable throwable) {
        log.info("onError");
      }

      @Override
      public void onComplete() {
        log.info("onComplete");
      }
    };

    publishOn.subscribe(subscriber);

    log.info("EXIT");
  }

}
