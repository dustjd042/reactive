package com.yeonseong.reactive;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

public class FluxTest {

  @Test
  void FluxCreate() {
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
  void FluxRange() {
    Flux.range(1, 10)
        .log()
        .subscribe(System.out::println);
  }

  @Test
  void FluxSubscribeOn() {
    Flux.range(1, 10)
        .log()
        .subscribeOn(Schedulers.newSingle("sub"))
        .subscribe(System.out::println);
    System.out.println("Exit");
  }

  @Test
  void FluxPublishOn() {
    Flux.range(1, 10)
        .publishOn(Schedulers.newSingle("pub"))
        .log()
        .subscribe(System.out::println);
    System.out.println("Exit");
  }

  @Test
  void FluxSubscribeOnPublishOn() {
    Flux.range(1, 10)
        .publishOn(Schedulers.newSingle("pub"))
        .log()
        .subscribeOn(Schedulers.newSingle("sub"))
        .subscribe(System.out::println);
    System.out.println("Exit");
  }
}
