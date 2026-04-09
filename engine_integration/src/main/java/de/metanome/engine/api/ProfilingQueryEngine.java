package de.metanome.engine.api;

import de.metanome.engine.api.result_receiver.EngineResultReceiver;
import de.metanome.engine.api.result_receiver.EngineResultReceiverException;

import java.util.ArrayList;

public interface ProfilingQueryEngine {
    void execute(String query,
                 EngineExecutionContext context,
                 EngineResultReceiver receiver)
            throws EngineException, EngineResultReceiverException;

    /**
     * Engines can override this to declare which execution parameters they need.
     * This enables a dynamic DPQL UI (fields appear based on selected engine).
     *
     * Default: no parameters. Engines must explicitly declare any parameters
     * they want to expose via the UI / API.
     */
    default ArrayList<EngineParameterSpec> getParameterSpecifications() {
        return new ArrayList<>();
    }

    String getName();
}
