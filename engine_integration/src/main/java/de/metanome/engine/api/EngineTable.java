package de.metanome.engine.api;

import java.util.List;
import java.util.Map;

/**
 * One tabular result from an engine (e.g. one ResultSet).
 */
public final class EngineTable {

    /** High-level nature of this table: table, FDs, INDs, etc. */
    private ResultKind kind;

    /** Optional logical name (e.g. "main", "fds", "inds") */
    private String name;

    /** Column headers for this table. */
    private List<String> columns;

    /** Tabular data rows. Each row is a list of string cells. */
    private List<List<String>> rows;

    /** Optional metadata specific to this table. */
    private Map<String, String> metadata;

    // getters / setters

    public ResultKind getKind() {
        return kind;
    }

    public void setKind(ResultKind kind) {
        this.kind = kind;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<String> getColumns() {
        return columns;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }

    public List<List<String>> getRows() {
        return rows;
    }

    public void setRows(List<List<String>> rows) {
        this.rows = rows;
    }

    public Map<String, String> getMetadata() {
        return metadata;
    }

    public void setMetadata(Map<String, String> metadata) {
        this.metadata = metadata;
    }
}
