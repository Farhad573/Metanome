package de.metanome.engine.api;

import java.util.List;
import java.util.Map;

/**
 * Full result returned from a ProfilingQueryEngine execution.
 * Contains one or more tabular results (tables).
 */
public final class EngineResult {

    /** All tables produced by this engine call (at least one). */
    private List<EngineTable> tables;

    /** Optional information about the whole query execution. */
    private Map<String, String> metadata; // e.g. engineName, executionTimeMs, queryLanguage, etc.

    // getters/setters

    public List<EngineTable> getTables() {
        return tables;
    }

    public void setTables(List<EngineTable> tables) {
        this.tables = tables;
    }

    public Map<String, String> getMetadata() {
        return metadata;
    }

    public void setMetadata(Map<String, String> metadata) {
        this.metadata = metadata;
    }

    /**
     * Convenience: returns the first table or null if none.
     * Backend can use this as the "main" table for now.
     */
    public EngineTable getPrimaryTable() {
        if (tables == null || tables.isEmpty()) {
            return null;
        }
        return tables.get(0);
    }
}
