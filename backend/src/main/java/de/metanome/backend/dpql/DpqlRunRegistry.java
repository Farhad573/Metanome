package de.metanome.backend.dpql;

import de.metanome.engine.api.SimpleCancellationToken;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Simple in-memory registry for async DPQL runs.
 *
 * Note: this is not persisted; on backend restart, in-flight runs are lost.
 */
public final class DpqlRunRegistry {

    public enum Status {
        QUEUED,
        RUNNING,
        FINISHED,
        FAILED,
        CANCELED
    }

    static final class Entry {
        final String executionId;
        final long createdAt;
        final SimpleCancellationToken cancellationToken = new SimpleCancellationToken();
        volatile long startedAt;
        volatile long finishedAt;
        volatile Status status;
        volatile String message;
        volatile String error;
        volatile Future<?> future;

        Entry(String executionId) {
            this.executionId = executionId;
            this.createdAt = System.currentTimeMillis();
            this.status = Status.QUEUED;
        }

        DpqlRunStatusDto toDto() {
            DpqlRunStatusDto dto = new DpqlRunStatusDto();
            dto.setExecutionId(executionId);
            dto.setStatus(status != null ? status.name() : null);
            dto.setCreatedAt(createdAt);
            dto.setStartedAt(startedAt > 0 ? startedAt : null);
            dto.setFinishedAt(finishedAt > 0 ? finishedAt : null);
            dto.setMessage(message);
            dto.setError(error);
            return dto;
        }
    }

    private static final Map<String, Entry> RUNS = new ConcurrentHashMap<>();

    // Keep concurrency low by default (DPQL runs can be heavy).
    private static final ExecutorService EXEC = Executors.newFixedThreadPool(2);

    private DpqlRunRegistry() {
    }

    public static Entry register(String executionId) {
        Entry e = new Entry(executionId);
        RUNS.put(executionId, e);
        return e;
    }

    public static Entry get(String executionId) {
        if (executionId == null) {
            return null;
        }
        return RUNS.get(executionId);
    }

    public static ExecutorService executor() {
        return EXEC;
    }

    public static boolean cancel(String executionId) {
        Entry e = get(executionId);
        if (e == null) {
            return false;
        }
        if (e.status == Status.FINISHED || e.status == Status.FAILED || e.status == Status.CANCELED) {
            return false;
        }
        e.cancellationToken.cancel();
        e.status = Status.CANCELED;
        e.finishedAt = System.currentTimeMillis();
        e.message = "Canceled";
        Future<?> f = e.future;
        if (f != null) {
            f.cancel(true);
        }
        return true;
    }
}
