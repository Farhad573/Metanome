package de.metanome.backend.dpql.result;

import de.metanome.engine.api.EngineResult;
import de.metanome.engine.api.EngineTable;
import de.metanome.engine.api.result_receiver.EngineResultMetadata;
import de.metanome.engine.api.result_receiver.EngineResultReceiver;
import de.metanome.engine.api.result_receiver.EngineResultReceiverException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Simple in-memory receiver that collects all streamed tables and exposes them
 * as a single {@link EngineResult} snapshot.
 */
public final class EngineResultCollector implements EngineResultReceiver {

    private final List<EngineTable> tables = new ArrayList<>();
    private final Map<String, String> metadata = new HashMap<>();
    private EngineTable currentTable;

    // Safety: allow optional caps to avoid unbounded memory growth.
    // -1 means unlimited.
    private final long maxTotalRows;
    private final long maxTotalCells;
    private long totalRows = 0;
    private long totalCells = 0;

    public EngineResultCollector() {
        this(-1, -1);
    }

    public EngineResultCollector(long maxTotalRows, long maxTotalCells) {
        this.maxTotalRows = maxTotalRows;
        this.maxTotalCells = maxTotalCells;
    }

    @Override
    public void start(EngineResultMetadata executionMetadata) {
        if (executionMetadata != null) {
            metadata.putAll(executionMetadata.asMap());
        }
    }

    @Override
    public void startTable(EngineTable table) throws EngineResultReceiverException {
        if (currentTable != null) {
            throw new EngineResultReceiverException("Previous table not closed (missing endTable)");
        }
        if (table == null) {
            throw new EngineResultReceiverException("Table must not be null");
        }
        EngineTable t = new EngineTable();
        t.setKind(table.getKind());
        t.setName(table.getName());
        t.setColumns(table.getColumns());
        t.setMetadata(table.getMetadata());
        t.setRows(new ArrayList<>());
        currentTable = t;
    }

    @Override
    public void receiveRow(List<String> row) throws EngineResultReceiverException {
        if (currentTable == null) {
            throw new EngineResultReceiverException("No active table (missing startTable)");
        }

        totalRows++;
        if (maxTotalRows >= 0 && totalRows > maxTotalRows) {
            throw new EngineResultReceiverException("DPQL result too large for in-memory collection (rows limit exceeded)");
        }
        int rowCells = row == null ? 0 : row.size();
        totalCells += rowCells;
        if (maxTotalCells >= 0 && totalCells > maxTotalCells) {
            throw new EngineResultReceiverException("DPQL result too large for in-memory collection (cells limit exceeded)");
        }

        if (currentTable.getRows() == null) {
            currentTable.setRows(new ArrayList<>());
        }
        currentTable.getRows().add(row);
    }

    @Override
    public void endTable() throws EngineResultReceiverException {
        if (currentTable == null) {
            throw new EngineResultReceiverException("No active table to end");
        }
        tables.add(currentTable);
        currentTable = null;
    }

    @Override
    public void finish() {
        // no-op
    }

    public EngineResult toEngineResult() {
        EngineResult result = new EngineResult();
        result.setTables(new ArrayList<>(tables));
        result.setMetadata(new HashMap<>(metadata));
        return result;
    }
}
