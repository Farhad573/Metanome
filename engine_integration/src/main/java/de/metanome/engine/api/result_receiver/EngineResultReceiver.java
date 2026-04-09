package de.metanome.engine.api.result_receiver;

import de.metanome.engine.api.EngineTable;

import java.util.List;

/**
 * Receives tabular results produced by {@link de.metanome.engine.api.ProfilingQueryEngine}.
 *
 * Engines stream their results table-by-table and row-by-row into a receiver so that
 * Metanome can persist, stream, or post-process the output without buffering the full
 * result set in memory.
 */
public interface EngineResultReceiver {

    /**
     * Called before any rows are streamed so that the backend can allocate
     * resources, register metadata, etc.
     *
     * @param executionMetadata user/engine specific metadata for this run
     */
    void start(EngineResultMetadata executionMetadata);

    /**
     * Signals the start of a logical table produced by the engine.
     *
     * The passed {@link EngineTable} is treated as a header (kind/name/columns/metadata);
     * receivers should not assume that {@link EngineTable#getRows()} is populated.
     */
    void startTable(EngineTable table) throws EngineResultReceiverException;

    /**
     * Receives a single row for the current table.
     */
    void receiveRow(List<String> row) throws EngineResultReceiverException;

    /**
     * Convenience: receives multiple rows for the current table.
     */
    default void receiveRows(List<List<String>> rows) throws EngineResultReceiverException {
        if (rows == null) {
            return;
        }
        for (List<String> row : rows) {
            receiveRow(row);
        }
    }

    /**
     * Signals that the current table is complete.
     */
    void endTable() throws EngineResultReceiverException;

    /**
     * Signals that an engine finished streaming its results successfully.
     */
    void finish();
}
