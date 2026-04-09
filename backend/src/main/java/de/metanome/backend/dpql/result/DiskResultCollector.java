package de.metanome.backend.dpql.result;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.metanome.engine.api.EngineTable;
import de.metanome.engine.api.result_receiver.EngineResultMetadata;
import de.metanome.engine.api.result_receiver.EngineResultReceiver;
import de.metanome.engine.api.result_receiver.EngineResultReceiverException;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class DiskResultCollector implements EngineResultReceiver, AutoCloseable {

    private final String executionId;
    private final File resultFile;
    private final PrintWriter writer;
    private final ObjectMapper mapper;
    private int tableCounter = 0;
    private Integer currentTableId = null;
    private int rowsSinceFlush = 0;

    public DiskResultCollector(String resultDirectory) throws IOException {
        this(resultDirectory, UUID.randomUUID().toString());
    }

    public DiskResultCollector(String resultDirectory, String executionId) throws IOException {
        this.executionId = executionId;
        this.resultFile = new File(resultDirectory, executionId + ".ndjson");
        this.writer = new PrintWriter(new BufferedWriter(new FileWriter(resultFile, false)));
        this.mapper = new ObjectMapper();
    }

    public String getExecutionId() {
        return executionId;
    }

    @Override
    public void start(EngineResultMetadata executionMetadata) {
        try {
            Map<String, Object> msg = new HashMap<>();
            msg.put("type", "metadata");
            msg.put("payload", executionMetadata != null ? executionMetadata.asMap() : new HashMap<>());
            writer.println(mapper.writeValueAsString(msg));
            writer.flush();
        } catch (IOException e) {
            throw new RuntimeException("Failed to write metadata to disk", e);
        }
    }

    @Override
    public void startTable(EngineTable table) throws EngineResultReceiverException {
        if (table == null) {
            throw new EngineResultReceiverException("Table must not be null");
        }
        if (currentTableId != null) {
            throw new EngineResultReceiverException("Previous table not closed (missing endTable)");
        }

        int tableId = ++tableCounter;
        currentTableId = tableId;
        try {
            Map<String, Object> header = new HashMap<>();
            header.put("type", "table-start");
            header.put("tableId", tableId);
            header.put("name", table.getName());
            header.put("kind", table.getKind());
            header.put("columns", table.getColumns());
            header.put("metadata", table.getMetadata());
            writer.println(mapper.writeValueAsString(header));
            writer.flush();
        } catch (IOException e) {
            throw new RuntimeException("Failed to write table to disk", e);
        }
    }

    @Override
    public void receiveRow(List<String> row) throws EngineResultReceiverException {
        if (currentTableId == null) {
            throw new EngineResultReceiverException("No active table (missing startTable)");
        }
        try {
            Map<String, Object> rowMsg = new HashMap<>();
            rowMsg.put("type", "row");
            rowMsg.put("tableId", currentTableId);
            rowMsg.put("data", row);
            writer.println(mapper.writeValueAsString(rowMsg));
            rowsSinceFlush++;
            if (rowsSinceFlush >= 1000) {
                writer.flush();
                rowsSinceFlush = 0;
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to write row to disk", e);
        }
    }

    @Override
    public void endTable() throws EngineResultReceiverException {
        if (currentTableId == null) {
            throw new EngineResultReceiverException("No active table to end");
        }
        try {
            Map<String, Object> footer = new HashMap<>();
            footer.put("type", "table-end");
            footer.put("tableId", currentTableId);
            writer.println(mapper.writeValueAsString(footer));
            writer.flush();
            rowsSinceFlush = 0;
        } catch (IOException e) {
            throw new RuntimeException("Failed to write table footer to disk", e);
        } finally {
            currentTableId = null;
        }
    }

    @Override
    public void finish() {
        if (currentTableId != null) {
            try {
                endTable();
            } catch (Exception ignored) {
                // ignore
            }
        }
        close();
    }

    @Override
    public void close() {
        if (writer != null) {
            writer.close();
        }
    }
}
