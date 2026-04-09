package de.metanome.engine.api;

/**
 * Cooperative cancellation signal for engines.
 *
 * Engines should check this periodically (especially in long loops) and abort promptly.
 */
public interface CancellationToken {

    boolean isCanceled();

    default void throwIfCanceled() throws EngineException {
        if (isCanceled()) {
            throw new EngineException("Execution canceled");
        }
    }
}

