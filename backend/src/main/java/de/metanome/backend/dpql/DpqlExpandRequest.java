package de.metanome.backend.dpql;

import java.util.List;

public class DpqlExpandRequest {
    private String executionId;

    /**
     * The AND-only predicate string, e.g. "FD(X,Y) AND IND(X,Y)".
     *
     * Can also be a full DPQL query; the service will use the substring after "WHERE".
     */
    private String where;

    private String anchorKind;
    private Integer anchorRowId;
    
    /** Multiple anchor row IDs for multi-row selection. Takes precedence over anchorRowId if non-empty. */
    private List<Integer> anchorRowIds;

    private Integer offset;
    private Integer limit;

    public String getExecutionId() {
        return executionId;
    }

    public void setExecutionId(String executionId) {
        this.executionId = executionId;
    }

    public String getWhere() {
        return where;
    }

    public void setWhere(String where) {
        this.where = where;
    }

    public String getAnchorKind() {
        return anchorKind;
    }

    public void setAnchorKind(String anchorKind) {
        this.anchorKind = anchorKind;
    }

    public Integer getAnchorRowId() {
        return anchorRowId;
    }

    public void setAnchorRowId(Integer anchorRowId) {
        this.anchorRowId = anchorRowId;
    }

    public List<Integer> getAnchorRowIds() {
        return anchorRowIds;
    }

    public void setAnchorRowIds(List<Integer> anchorRowIds) {
        this.anchorRowIds = anchorRowIds;
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
}
