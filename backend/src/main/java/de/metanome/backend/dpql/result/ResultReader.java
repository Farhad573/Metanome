package de.metanome.backend.dpql.result;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ResultReader {

    private final File resultFile;
    private final ObjectMapper mapper;

    public ResultReader(String resultDirectory, String executionId) {
        this.resultFile = new File(resultDirectory, executionId + ".ndjson");
        this.mapper = new ObjectMapper();
    }

    public boolean exists() {
        return resultFile.exists() && resultFile.isFile();
    }

    /**
     * Reads a page of rows for a specific table.
     * 
     * @param targetTableId The ID of the table to fetch rows for (1-based).
     * @param offset Number of rows to skip (within that table).
     * @param limit Max number of rows to return.
     * @param searchQuery Optional string to filter rows by (case-insensitive).
     * @return A DTO containing the table header (if found) and the requested rows.
     */
    public Map<String, Object> readTablePage(int targetTableId, int offset, int limit, String searchQuery) throws IOException {
        Map<String, Object> result = new HashMap<>();
        List<List<String>> rows = new ArrayList<>();
        Map<String, Object> tableHeader = null;
        
        int matchingRowsFound = 0;
        boolean insideTargetTable = false;
        String lowerQuery = (searchQuery != null) ? searchQuery.toLowerCase() : null;

        try (BufferedReader reader = new BufferedReader(new FileReader(resultFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                JsonNode node = mapper.readTree(line);
                String type = node.get("type").asText();

                if ("table-start".equals(type)) {
                    int id = node.get("tableId").asInt();
                    if (id == targetTableId) {
                        insideTargetTable = true;
                        tableHeader = mapper.convertValue(node, new TypeReference<Map<String, Object>>() {});
                    } else {
                        insideTargetTable = false;
                    }
                } else if ("table-end".equals(type)) {
                    if (insideTargetTable) {
                        break; // Finished reading the target table
                    }
                } else if ("row".equals(type)) {
                    if (insideTargetTable) {
                        JsonNode dataNode = node.get("data");
                        List<String> rowData = mapper.convertValue(dataNode, new TypeReference<List<String>>() {});
                        
                        boolean matches = true;
                        if (lowerQuery != null && !lowerQuery.isEmpty()) {
                            matches = false;
                            for (String cell : rowData) {
                                if (cell != null && cell.toLowerCase().contains(lowerQuery)) {
                                    matches = true;
                                    break;
                                }
                            }
                        }

                        if (matches) {
                            if (matchingRowsFound >= offset && rows.size() < limit) {
                                rows.add(rowData);
                            }
                            matchingRowsFound++;
                        }
                        
                        if (rows.size() >= limit) {
                            break; 
                        }
                    }
                }
            }
        }

        if (tableHeader != null) {
            result.put("table", tableHeader);
            result.put("rows", rows);
            
            Map<String, Object> pagination = new HashMap<>();
            pagination.put("offset", offset);
            pagination.put("limit", limit);
            pagination.put("returned", rows.size());
            result.put("pagination", pagination);
        }
        
        return result;
    }
    
    /**
     * Returns just the metadata and list of tables (without rows).
     */
    public Map<String, Object> readOverview() throws IOException {
        Map<String, Object> result = new HashMap<>();
        List<Map<String, Object>> tables = new ArrayList<>();
        Map<String, Object> metadata = new HashMap<>();

        try (BufferedReader reader = new BufferedReader(new FileReader(resultFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                JsonNode node = mapper.readTree(line);
                String type = node.get("type").asText();

                if ("metadata".equals(type)) {
                    metadata = mapper.convertValue(node.get("payload"), new TypeReference<Map<String, Object>>() {});
                } else if ("table-start".equals(type)) {
                    Map<String, Object> tableInfo = mapper.convertValue(node, new TypeReference<Map<String, Object>>() {});
                    // Remove potentially large fields if any, though header is usually small
                    tables.add(tableInfo);
                }
            }
        }
        
        result.put("metadata", metadata);
        result.put("tables", tables);
        return result;
    }

    /**
     * Streams a full table as CSV, optionally filtered by the same searchQuery semantics
     * as {@link #readTablePage(int, int, int, String)}.
     *
     * This is OOM-safe: it reads the NDJSON file line-by-line and writes CSV rows as it goes.
     */
    public void writeTableCsv(int targetTableId, String searchQuery, OutputStream out) throws IOException {
        boolean insideTargetTable = false;
        boolean wroteHeader = false;
        String lowerQuery = (searchQuery != null) ? searchQuery.toLowerCase() : null;

        try (BufferedReader reader = new BufferedReader(new FileReader(resultFile));
             Writer w = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isEmpty()) continue;
                JsonNode node = mapper.readTree(line);
                JsonNode typeNode = node.get("type");
                if (typeNode == null) continue;
                String type = typeNode.asText();

                if ("table-start".equals(type)) {
                    int id = node.hasNonNull("tableId") ? node.get("tableId").asInt() : -1;
                    if (id == targetTableId) {
                        insideTargetTable = true;
                        if (!wroteHeader) {
                            List<String> columns = List.of();
                            try {
                                JsonNode colsNode = node.get("columns");
                                if (colsNode != null && colsNode.isArray()) {
                                    columns = mapper.convertValue(colsNode, new TypeReference<List<String>>() {});
                                }
                            } catch (Exception ignored) {
                                columns = List.of();
                            }
                            if (columns != null && !columns.isEmpty()) {
                                writeCsvRow(columns, w);
                            }
                            wroteHeader = true;
                        }
                    } else {
                        insideTargetTable = false;
                    }
                } else if ("table-end".equals(type)) {
                    if (insideTargetTable) {
                        break;
                    }
                } else if ("row".equals(type)) {
                    if (!insideTargetTable) continue;
                    JsonNode dataNode = node.get("data");
                    if (dataNode == null) continue;

                    List<String> rowData;
                    try {
                        rowData = mapper.convertValue(dataNode, new TypeReference<List<String>>() {});
                    } catch (Exception ex) {
                        continue;
                    }

                    boolean matches = true;
                    if (lowerQuery != null && !lowerQuery.isEmpty()) {
                        matches = false;
                        for (String cell : rowData) {
                            if (cell != null && cell.toLowerCase().contains(lowerQuery)) {
                                matches = true;
                                break;
                            }
                        }
                    }

                    if (matches) {
                        writeCsvRow(rowData, w);
                    }
                }
            }
            w.flush();
        }
    }

    private static void writeCsvRow(List<String> cells, Writer w) throws IOException {
        if (cells == null || cells.isEmpty()) {
            w.write("\n");
            return;
        }
        for (int i = 0; i < cells.size(); i++) {
            if (i > 0) w.write(',');
            writeCsvCell(cells.get(i), w);
        }
        w.write("\n");
    }

    private static void writeCsvCell(String value, Writer w) throws IOException {
        if (value == null) {
            return;
        }
        boolean needsQuotes = false;
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == ',' || c == '"' || c == '\n' || c == '\r') {
                needsQuotes = true;
                break;
            }
        }
        if (!needsQuotes) {
            w.write(value);
            return;
        }

        w.write('"');
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == '"') {
                w.write("\"\"");
            } else {
                w.write(c);
            }
        }
        w.write('"');
    }
}
