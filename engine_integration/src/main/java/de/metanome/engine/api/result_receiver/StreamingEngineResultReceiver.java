package de.metanome.engine.api.result_receiver;

import de.metanome.engine.api.EngineTable;

import java.util.List;
import java.util.function.Consumer;

/**
 * Lightweight functional adapter so that engines can plug-in simple callbacks
 * without implementing the full {@link EngineResultReceiver} contract.
 */
public class StreamingEngineResultReceiver implements EngineResultReceiver {

    private final Consumer<EngineTable> onTableStart;
    private final Consumer<List<String>> onRow;
    private final Runnable onTableEnd;

    public StreamingEngineResultReceiver(Consumer<EngineTable> onTableStart,
                                         Consumer<List<String>> onRow,
                                         Runnable onTableEnd) {
        this.onTableStart = onTableStart;
        this.onRow = onRow;
        this.onTableEnd = onTableEnd;
    }

    @Override
    public void start(EngineResultMetadata executionMetadata) {
        // no-op by default
    }

    @Override
    public void startTable(EngineTable table) {
        if (onTableStart != null) {
            onTableStart.accept(table);
        }
    }

    @Override
    public void receiveRow(List<String> row) {
        if (onRow != null) {
            onRow.accept(row);
        }
    }

    @Override
    public void endTable() {
        if (onTableEnd != null) {
            onTableEnd.run();
        }
    }

    @Override
    public void finish() {
        // no-op by default
    }
}
