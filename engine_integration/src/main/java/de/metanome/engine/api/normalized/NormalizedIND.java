package de.metanome.engine.api.normalized;

import java.util.List;

/**
 * Normalized inclusion dependency representation.
 * Example: R[A,B] ⊆ S[C,D]
 */
public class NormalizedIND {

    private String dependentRelation;
    private List<String> dependentColumns;

    private String referencedRelation;
    private List<String> referencedColumns;

    public String getDependentRelation() {
        return dependentRelation;
    }

    public void setDependentRelation(String dependentRelation) {
        this.dependentRelation = dependentRelation;
    }

    public List<String> getDependentColumns() {
        return dependentColumns;
    }

    public void setDependentColumns(List<String> dependentColumns) {
        this.dependentColumns = dependentColumns;
    }

    public String getReferencedRelation() {
        return referencedRelation;
    }

    public void setReferencedRelation(String referencedRelation) {
        this.referencedRelation = referencedRelation;
    }

    public List<String> getReferencedColumns() {
        return referencedColumns;
    }

    public void setReferencedColumns(List<String> referencedColumns) {
        this.referencedColumns = referencedColumns;
    }
}
