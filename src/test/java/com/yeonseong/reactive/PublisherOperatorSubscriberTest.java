package com.yeonseong.reactive;

import java.util.List;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
public class PublisherOperatorSubscriberTest {

  @Test
  void publisherOperatorSubscriber() throws InterruptedException {

    Publisher<Integer> publisher = publisher(Stream.iterate(1, a -> a + 1).limit(10).collect(Collectors.toList()));

    Publisher<String> mapOperator = mapOperator(publisher, s -> "[" + s + "]");
    mapOperator.subscribe(logSubscriber());

    Publisher<StringBuilder> reduceOperator = reduceOperator(publisher, new StringBuilder(), (a, b) -> a.append(b + ", "));
    reduceOperator.subscribe(logSubscriber());
  }

  private <T, R>Publisher<R> reduceOperator(Publisher<T> publisher, R init, BiFunction<R, T, R> biFunction) {
    return new Publisher<R>() {
      @Override
      public void subscribe(Subscriber<? super R> subscriber) {
        publisher.subscribe(new DelegateSubscriber<T, R>(subscriber){

          R result = init;

          @Override
          public void onNext(T item) {
            result = biFunction.apply(result, item);
          }

          @Override
          public void onComplete() {
            subscriber.onNext(result);
            subscriber.onComplete();
          }
        });
      }
    };
  }

  private <T, R> Publisher<R> mapOperator(Publisher<T> publisher, Function<T, R> function) {
    return new Publisher<R>() {
      @Override
      public void subscribe(Subscriber<? super R> subscriber) {
        publisher.subscribe(new DelegateSubscriber<T, R>(subscriber) {
          @Override
          public void onNext(T item) {
            subscriber.onNext(function.apply(item));
          }
        });
      }
    };
  }

  private <T>Subscriber<T> logSubscriber() {
    return new Subscriber<T>() {
      @Override
      public void onSubscribe(Subscription subscription) {
        log.info("onSubscribe");
        subscription.request(Long.MAX_VALUE);
      }

      @Override
      public void onNext(T item) {
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
  }

  private Publisher<Integer> publisher(List<Integer> list) {
    return new Publisher<Integer>() {
      @Override
      public void subscribe(Subscriber<? super Integer> subscriber) {
        subscriber.onSubscribe(new Subscription() {
          @Override
          public void request(long n) {
            try {
              list.forEach(subscriber::onNext);
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
  }

  static class DelegateSubscriber<T, R> implements Subscriber<T> {

    private Subscriber subscriber;

    public DelegateSubscriber(Subscriber<? super R> subscriber) {
      this.subscriber = subscriber;
    }

    @Override
    public void onSubscribe(Subscription subscription) {
      subscriber.onSubscribe(subscription);
    }

    @Override
    public void onNext(T item) {

    }

    @Override
    public void onError(Throwable throwable) {
      subscriber.onError(throwable);
    }

    @Override
    public void onComplete() {
      subscriber.onComplete();
    }
  }
}
