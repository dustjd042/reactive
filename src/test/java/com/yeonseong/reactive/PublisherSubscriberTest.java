package com.yeonseong.reactive;

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
public class PublisherSubscriberTest {

  @Test
  void publisherSubscriber() throws InterruptedException {

    Iterable<Integer> iterable = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

    // 생성자
    Publisher<Integer> publisher = new Publisher() {
      @Override
      public void subscribe(Subscriber subscriber) {
        System.out.println("subscribe");

        Iterator<Integer> iterator = iterable.iterator();
        subscriber.onSubscribe(new Subscription() {

          @Override
          public void request(long n) {

            int i = 0;
            try {
              System.out.println("request");

              while (i++ < n) {
                if (iterator.hasNext()) {
                  subscriber.onNext(iterator.next());
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
    Subscriber<Integer> subscriber = new Subscriber<Integer>() {

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

    publisher.subscribe(subscriber);
  }

}
