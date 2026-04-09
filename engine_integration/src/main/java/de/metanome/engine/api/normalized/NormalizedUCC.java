package de.metanome.engine.api.normalized;

import java.util.List;

/**
 * Normalized unique column combination representation.
 * Example: R[A,B] is unique
 */
public class NormalizedUCC {

    private String relation;
    private List<String> columns;

    public String getRelation() {
        return relation;
    }

    public void setRelation(String relation) {
        this.relation = relation;
    }

    public List<String> getColumns() {
        return columns;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }
}
