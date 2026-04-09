package de.metanome.engine.api.normalized;

import java.util.List;

/**
 * Normalized functional dependency representation.
 * Example: [A,B] -> C
 */
public class NormalizedFD {

    private String relation;
    private List<String> lhs;
    private String rhs;

    public String getRelation() {
        return relation;
    }

    public void setRelation(String relation) {
        this.relation = relation;
    }

    public List<String> getLhs() {
        return lhs;
    }

    public void setLhs(List<String> lhs) {
        this.lhs = lhs;
    }

    public String getRhs() {
        return rhs;
    }

    public void setRhs(String rhs) {
        this.rhs = rhs;
    }
}
