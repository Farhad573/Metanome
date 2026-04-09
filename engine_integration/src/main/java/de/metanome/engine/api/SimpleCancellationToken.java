package de.metanome.engine.api;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Simple, thread-safe cancellation token implementation.
 */
public final class SimpleCancellationToken implements CancellationToken {
    private final AtomicBoolean canceled = new AtomicBoolean(false);

    public void cancel() {
        canceled.set(true);
    }

    @Override
    public boolean isCanceled() {
        return canceled.get();
    }
}

