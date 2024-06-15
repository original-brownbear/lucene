/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.lucene.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.ThreadInterruptedException;

/**
 * Executor wrapper responsible for the execution of concurrent tasks. Used to parallelize search
 * across segments as well as query rewrite in some cases. Exposes a single {@link
 * #invokeAll(Collection)} method that takes a collection of {@link Callable}s and executes them
 * concurrently/ Once all tasks are submitted to the executor, it blocks and wait for all tasks to
 * be completed, and then returns a list with the obtained results.
 *
 * @lucene.experimental
 */
public final class TaskExecutor {
  private final Executor executor;

  /**
   * Creates a TaskExecutor instance
   *
   * @param executor the executor to be used for running tasks concurrently
   */
  public TaskExecutor(Executor executor) {
    this.executor = Objects.requireNonNull(executor, "Executor is null");
  }

  /**
   * Execute all the callables provided as an argument, wait for them to complete and return the
   * obtained results. If an exception is thrown by more than one callable, the subsequent ones will
   * be added as suppressed exceptions to the first one that was caught.
   *
   * @param callables the callables to execute
   * @return a list containing the results from the tasks execution
   * @param <T> the return type of the task execution
   */
  public <T> List<T> invokeAll(Collection<Callable<T>> callables) throws IOException {
    final int count = callables.size();
    if (count == 1) {
      final T res;
      try {
        res = callables.iterator().next().call();
      } catch (Exception e) {
        throw IOUtils.rethrowAlways(e);
      }
      return Collections.singletonList(res);
    }
    return invokeMultiple(callables, count);
  }

  private <T> List<T> invokeMultiple(Collection<Callable<T>> callables, int count)
      throws IOException {
    final List<RunnableFuture<T>> futures = new ArrayList<>(count);
    for (Callable<T> callable : callables) {
      futures.add(
          new FutureTask<>(callable) {

            @Override
            protected void setException(Throwable t) {
              super.setException(t);
              for (Future<T> future : futures) {
                future.cancel(false);
              }
            }

            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
              assert mayInterruptIfRunning == false
                  : "cancelling tasks that are running is not supported";
              if (isDone()) {
                return false;
              }
              set(null);
              return true;
            }
          });
    }

    final AtomicInteger taskId = new AtomicInteger(0);
    final Runnable work =
        () -> {
          int id;
          while ((id = taskId.getAndIncrement()) < count) {
            futures.get(id).run();
            if (id >= count - 1) {
              // save redundant CAS in case this was the last task
              break;
            }
          }
        };
    final Executor executorRef = executor;
    int forkCount = count - 1;
    if (executorRef instanceof ThreadPoolExecutor threadPoolExecutor) {
      forkCount = Math.min(threadPoolExecutor.getMaximumPoolSize(), forkCount);
    }
    for (int j = 0; j < forkCount; j++) {
      executorRef.execute(work);
    }
    work.run();
    Throwable exc = null;
    List<T> results = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      Future<T> future = futures.get(i);
      try {
        results.add(future.get());
      } catch (InterruptedException e) {
        exc = IOUtils.useOrSuppress(exc, new ThreadInterruptedException(e));
      } catch (ExecutionException e) {
        exc = IOUtils.useOrSuppress(exc, e.getCause());
      }
    }
    assert assertAllFuturesCompleted(futures) : "Some tasks are still running?";
    if (exc != null) {
      throw IOUtils.rethrowAlways(exc);
    }
    return results;
  }

  @Override
  public String toString() {
    return "TaskExecutor(" + "executor=" + executor + ')';
  }

  private static <T> boolean assertAllFuturesCompleted(Collection<RunnableFuture<T>> futures) {
    for (RunnableFuture<T> future : futures) {
      if (future.isDone() == false) {
        return false;
      }
    }
    return true;
  }
}
