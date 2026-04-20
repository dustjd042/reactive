package com.yeonseong.reactive;

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
public class AsyncTest {

  @Test
  void runnable () throws InterruptedException {
    ExecutorService es = Executors.newCachedThreadPool();
    es.submit(() -> {
      try {
        Thread.sleep(200);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      log.info("Async");
    });
    log.info("EXIT");

    Thread.sleep(500);
    es.shutdown();
  }

  @Test
  void futureGet () throws InterruptedException, ExecutionException {
    ExecutorService es = Executors.newCachedThreadPool();
    Future<String> future = es.submit(() -> {
      Thread.sleep(200);
      log.info("Async");
      return "Hello";
    });

    log.info(future.get()); // Blocking
    log.info("EXIT");

    Thread.sleep(500);
    es.shutdown();
  }

  @Test
  void futureIsDone () throws InterruptedException, ExecutionException {
    ExecutorService es = Executors.newCachedThreadPool();
    Future<String> future = es.submit(() -> {
      Thread.sleep(200);
      log.info("Async");
      return "Hello";
    });

    log.info("{}", future.isDone());
    log.info("EXIT");

    Thread.sleep(500);
    log.info("{}", future.isDone());
    log.info("{}", future.get());
    es.shutdown();
  }

  @Test
  void futureTask () throws InterruptedException, ExecutionException {
    ExecutorService es = Executors.newCachedThreadPool();
    FutureTask<String> futureTask = new FutureTask<String>(() -> {
      Thread.sleep(200);
      return "Hello";
    }) {
      @Override
      protected void done() {
        try {
          log.info(get());
        } catch (Exception exception) {
          throw new RuntimeException(exception);
        }
      }
    };
    es.execute(futureTask);

    Thread.sleep(500);
    es.shutdown();
  }

  interface SuccessCallback {
    void onSuccess(String result);
  }

  class CallbackFutureTask extends FutureTask<String> {
    SuccessCallback successCallback;
    public CallbackFutureTask(Callable<String> callable, SuccessCallback sc) {
      super(callable);
      this.successCallback = Objects.requireNonNull(sc);
    }
  }

}
