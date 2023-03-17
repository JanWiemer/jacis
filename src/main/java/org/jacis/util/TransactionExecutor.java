package org.jacis.util;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.jacis.container.JacisContainer;
import org.jacis.exception.JacisStaleObjectException;
import org.jacis.plugin.txadapter.local.JacisLocalTransaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionExecutor {

  public static TransactionExecutor singleAttempt(JacisContainer container) {
    return new TransactionExecutor(container).withRetries(0).retryWhen(t -> false);
  }

  public static TransactionExecutor withRetries(JacisContainer container, int maxRetries) {
    return new TransactionExecutor(container).withRetries(maxRetries);
  }

  private static final Logger log = LoggerFactory.getLogger(JacisContainer.class);

  /** Reference to the JACIS container used to create this transaction executor. */
  private final JacisContainer container;

  /** The maximum number of retries if retry-able exceptions (e.g. stale object exceptions) are thrown (default 6). */
  private int maxRetries = 6;
  /** Predicate deciding if a retry is possible for an exception (default: true for JacisStaleObjectException). */
  private Predicate<Throwable> retryPossiblePredicate = t -> t instanceof JacisStaleObjectException;
  /** Handler performing additional actions on each failed attempt (default: only log the exception on WARN level). */
  private FailedAttemptHandler failedAttemptHandler = this::defaultHandleFailedAttempt;
  /** Function computing the delay in milliseconds before the task is retried (default: (attempt - 1) * 10) */
  private RetryDelayFunction retryDelayFunction = (taskName, attempt, last, exceptionHist) -> (attempt - 1) * 10L;

  public TransactionExecutor(JacisContainer container) {
    this.container = container;
  }

  public TransactionExecutor withRetries(int retries) {
    maxRetries = retries;
    return this;
  }

  public TransactionExecutor retryWhen(Predicate<Throwable> retryPossiblePredicate) {
    this.retryPossiblePredicate = retryPossiblePredicate;
    return this;
  }

  public TransactionExecutor onFailedAttempt(FailedAttemptHandler failedAttemptHandler) {
    this.failedAttemptHandler = failedAttemptHandler;
    return this;
  }

  public TransactionExecutor withDelays(RetryDelayFunction retryDelayFunction) {
    this.retryDelayFunction = retryDelayFunction;
    return this;
  }

  public void execute(Runnable task, String taskName) throws IllegalStateException {
    execute(() -> {
      task.run();
      return null;
    }, taskName);
  }

  public <R> R execute(Supplier<R> task, String taskName) throws IllegalStateException {
    List<Throwable> exceptions = null;
    for (int attempt = 1; attempt <= maxRetries + 1; attempt++) {
      boolean lastAttempt = attempt >= maxRetries;
      if (attempt > 1) {
        long delay = retryDelayFunction == null ? 0 : retryDelayFunction.computeDelayBeforeRetry(taskName, attempt, lastAttempt, exceptions);
        if (delay > 0) {
          try {
            Thread.sleep(delay);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
      }
      try {
        return executeSingleAttempt(task, taskName);
      } catch (Throwable e) {
        if (retryPossiblePredicate == null || !retryPossiblePredicate.test(e)) {
          throw wrap(e, taskName, attempt, exceptions);
        } else {
          exceptions = exceptions != null ? exceptions : new ArrayList<>(maxRetries);
          if (failedAttemptHandler != null) {
            failedAttemptHandler.handleFailedAttempt(taskName, attempt, lastAttempt, e, exceptions);
          }
          exceptions.add(e);
        }
      }
    }
    if (exceptions == null) {
      throw new IllegalStateException("Illegal retry configuration (executing task >" + (taskName == null ? "?" : taskName) + "<!");
    }
    throw wrap(exceptions.get(exceptions.size() - 1), taskName, maxRetries + 1, exceptions);
  }

  protected <R> R executeSingleAttempt(Supplier<R> task, String taskName) throws IllegalStateException {
    JacisLocalTransaction tx = container.beginLocalTransaction(taskName);
    Throwable txException = null;
    try {
      R result = task.get();
      tx.prepare(); // phase 1 of the two phase internalCommit protocol
      tx.commit(); // phase 2 of the two phase internalCommit protocol
      tx = null;
      return result;
    } catch (Throwable e) {
      txException = e;
      throw e;
    } finally {
      if (tx != null) { // if not committed roll it back
        try {
          tx.rollback();
        } catch (Throwable rollbackException) {
          RuntimeException exceptionToThrow = new RuntimeException("Rollback failed after " + txException, txException);
          exceptionToThrow.addSuppressed(rollbackException);
          // noinspection ThrowFromFinallyBlock
          throw exceptionToThrow;
        }
      }
    }
  }

  protected void defaultHandleFailedAttempt(String taskName, int attemptNr, boolean lastAttempt, Throwable exception, List<Throwable> prevExceptions) {
    log.warn("task >{}<: attempt {}/{} failed with exception {}", taskName == null ? "?" : taskName, attemptNr, maxRetries, String.valueOf(exception));
  }

  protected RuntimeException wrap(Throwable e, String taskName, int attempt, List<Throwable> exceptions) { // wrap the exception
    if (exceptions != null && !exceptions.isEmpty()) {
      e.addSuppressed(new PreviousAttemptExceptions(exceptions, taskName));
    }
    if (e instanceof RuntimeException) {
      return (RuntimeException) e;
    }
    return new RuntimeException(e.toString(), e);
  }

  /** Exception summarizing the information of all previous attempts (as suppressed exceptions). */
  public static class PreviousAttemptExceptions extends Exception {

    public PreviousAttemptExceptions(List<Throwable> exceptions, String taskName) {
      super("Previously " + exceptions.size() + " attempts executing task >" + taskName + "< failed (see suppressed exceptions).");
      exceptions.forEach(this::addSuppressed);
    }

    private static final long serialVersionUID = 1L;

  }

  /** Functional interface for a handler executing some additional logic after each failed attempt. */
  @FunctionalInterface
  public interface FailedAttemptHandler {
    void handleFailedAttempt(String taskName, int attemptNr, boolean lastAttempt, Throwable exception, List<Throwable> prevExceptions);
  }

  /** Functional interface for a method computing the delay before executing a retry. */
  @FunctionalInterface
  public interface RetryDelayFunction {
    long computeDelayBeforeRetry(String taskName, int attempt, boolean lastAttempt, List<Throwable> exceptions);
  }
}
