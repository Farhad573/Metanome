package de.metanome.backend.dpql;

import java.util.List;

public class DpqlNormalizedTablePageResponseDto {
    private String executionId;
    private Long tableId;

    private String kind;
    private String name;

    private List<String> columns;

    /**
     * Row IDs as stored in DPQLNORMCELL (0-based row index).
     * Parallel to {@link #rows}.
     */
    private List<Integer> rowIds;

    private List<List<String>> rows;

    private Integer offset;
    private Integer limit;
    private String search;

    private Integer totalRows;

    public String getExecutionId() {
        return executionId;
    }

    public void setExecutionId(String executionId) {
        this.executionId = executionId;
    }

    public Long getTableId() {
        return tableId;
    }

    public void setTableId(Long tableId) {
        this.tableId = tableId;
    }

    public String getKind() {
        return kind;
    }

    public void setKind(String kind) {
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

    public List<Integer> getRowIds() {
        return rowIds;
    }

    public void setRowIds(List<Integer> rowIds) {
        this.rowIds = rowIds;
    }

    public List<List<String>> getRows() {
        return rows;
    }

    public void setRows(List<List<String>> rows) {
        this.rows = rows;
    }

    public Integer getOffset() {
        return offset;
    }

    public void setOffset(Integer offset) {
        this.offset = offset;
    }

    public Integer getLimit() {
        return limit;
    }

    public void setLimit(Integer limit) {
        this.limit = limit;
    }

    public String getSearch() {
        return search;
    }

    public void setSearch(String search) {
        this.search = search;
    }

    public Integer getTotalRows() {
        return totalRows;
    }

    public void setTotalRows(Integer totalRows) {
        this.totalRows = totalRows;
    }
}

