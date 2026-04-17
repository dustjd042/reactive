package com.yeonseong.reactive;

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
public class PubSubTest {

  @Test
  void PubSub() throws InterruptedException {

    Iterable<Integer> itr = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

    // 생성자
    Publisher<Integer> p = new Publisher() {
      @Override
      public void subscribe(Subscriber subscriber) {
        System.out.println("subscribe");

        Iterator<Integer> it = itr.iterator();
        subscriber.onSubscribe(new Subscription() {

          @Override
          public void request(long n) {

            int i = 0;
            try {
              System.out.println("request");

              while (i++ < n) {
                if (it.hasNext()) {
                  subscriber.onNext(it.next());
                } else {
                  subscriber.onComplete();
                  break;
                }
              }
            } catch (Exception exception) {
              subscriber.onError(exception);
            }
          }

          @Override
          public void cancel() {
            System.out.println("cancel");
          }
        });
      }
    };

    // 구독자
    Subscriber<Integer> s = new Subscriber<Integer>() {

      Subscription subscription;

      @Override
      public void onSubscribe(Subscription subscription) {
        System.out.println("onSubscribe");
        this.subscription = subscription;
        this.subscription.request(1);
      }

      @Override
      public void onNext(Integer item) {
        System.out.println(Thread.currentThread().getName() + " onNext " + item);
        this.subscription.request(1);
      }

      @Override
      public void onError(Throwable throwable) {
        System.out.println("onError");
      }

      @Override
      public void onComplete() {
        System.out.println("onComplete");
      }
    };

    p.subscribe(s);
  }


  @Test
  void PubSub2() throws InterruptedException {

    Publisher<Integer> pub = new Publisher<Integer>() {

      Iterable<Integer> iterable = Stream.iterate(1, a -> a + 1).limit(10).collect(Collectors.toList());

      @Override
      public void subscribe(Subscriber<? super Integer> subscriber) {
        subscriber.onSubscribe(new Subscription() {
          @Override
          public void request(long n) {
            try {
              iterable.forEach(s -> subscriber.onNext(s));
              subscriber.onComplete();
            } catch (Exception exception) {
              subscriber.onError(exception);
            }
          }

          @Override
          public void cancel() {

          }
        });
      }
    };

    Subscriber<Integer> sub = new Subscriber<Integer>() {
      @Override
      public void onSubscribe(Subscription subscription) {
        log.info("onSubscribe");
        subscription.request(Long.MAX_VALUE);
      }

      @Override
      public void onNext(Integer item) {
        log.info("onNext: {}", item);
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

    pub.subscribe(sub);
  }

}
