package de.metanome.backend.dpql;

import java.util.List;
import java.util.Map;

public class DpqlExpandResponseDto {
    private List<String> variables;

    /**
     * Each entry contains:
     * - "bindings": Map<String,String> variable -> value
     * - "rowIds": Map<String,Integer> kind -> rowId
     */
    private List<Map<String, Object>> tuples;

    private Integer offset;
    private Integer limit;

    public List<String> getVariables() {
        return variables;
    }

    public void setVariables(List<String> variables) {
        this.variables = variables;
    }

    public List<Map<String, Object>> getTuples() {
        return tuples;
    }

    public void setTuples(List<Map<String, Object>> tuples) {
        this.tuples = tuples;
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
