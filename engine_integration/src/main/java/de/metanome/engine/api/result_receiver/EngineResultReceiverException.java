package de.metanome.engine.api.result_receiver;

/**
 * Thrown when the Metanome backend cannot persist or post-process an engine result.
 */
public class EngineResultReceiverException extends Exception {

    public EngineResultReceiverException(String message) {
        super(message);
    }

    public EngineResultReceiverException(String message, Throwable cause) {
        super(message, cause);
    }
}
