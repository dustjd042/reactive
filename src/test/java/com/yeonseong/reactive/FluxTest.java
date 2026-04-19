package com.yeonseong.reactive;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

@Slf4j
public class FluxTest {

  @Test
  public void fluxCreate() {
    Flux.<Integer>create(e -> {
          e.next(1);
          e.next(2);
          e.next(3);
          e.complete();
        })
        .log()
        .map(s -> s * 10)
        .log()
        .reduce(0, Integer::sum)
        .log()
        .subscribe(System.out::println);
  }

  @Test
  public void fluxRange() {
    Flux.range(1, 10)
        .log()
        .subscribe(System.out::println);
  }

  @Test
  public void fluxSubscribeOn() {
    Flux.range(1, 10)
        .log()
        .subscribeOn(Schedulers.newSingle("sub"))
        .subscribe(System.out::println);
    System.out.println("Exit");
  }

  @Test
  public void fluxPublishOn() {
    Flux.range(1, 10)
        .publishOn(Schedulers.newSingle("pub"))
        .log()
        .subscribe(System.out::println);
    System.out.println("Exit");
  }

  @Test
  public void fluxSubscribeOnPublishOn() {
    Flux.range(1, 10)
        .publishOn(Schedulers.newSingle("pub"))
        .log()
        .subscribeOn(Schedulers.newSingle("sub"))
        .subscribe(System.out::println);
    System.out.println("Exit");
  }

  @Test
  public void fluxInterval() throws InterruptedException {

    // DaemonThread
    Flux.interval(Duration.ofMillis(500))
        .take(10)
        .subscribe(s -> log.info("onNext: {}", s));

    log.info("exit");
    TimeUnit.SECONDS.sleep(10);
  }

}
