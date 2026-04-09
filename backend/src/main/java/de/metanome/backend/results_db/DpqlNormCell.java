package de.metanome.backend.results_db;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.ForeignKey;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import java.io.Serializable;

/**
 * Relational shadow table that stores DPQL normalized results as row/cell tuples.
 *
 * This is additive to {@link DpqlNormalizedTable}: JSON storage stays as-is for compatibility,
 * while this table enables fast SQL joins (no JSON parsing) for pattern expansion.
 */
@Entity
@Table(name = "DPQLNORMCELL",
    indexes = {
        @Index(name = "IDX_CELL_LOOKUP", columnList = "EXECUTION_ID,TABLE_ID,COL_NAME,VALUE"),
        @Index(name = "IDX_ROW_LOOKUP", columnList = "EXECUTION_ID,TABLE_ID,ROW_ID"),
        @Index(name = "IDX_KIND_COL", columnList = "EXECUTION_ID,TABLE_ID,COL_NAME")})
public class DpqlNormCell implements Serializable {

  private static final long serialVersionUID = 1L;

  private long id;

  private String executionId;
  private DpqlExecution execution;
  private Integer tableId;
  private String kind;
  private int rowId;
  private String colName;
  private String value;

  public DpqlNormCell() {
    // for hibernate
  }

  public DpqlNormCell(String executionId, Integer tableId, String kind, int rowId, String colName,
      String value) {
    this.executionId = executionId;
    this.tableId = tableId;
    this.kind = kind;
    this.rowId = rowId;
    this.colName = colName;
    this.value = value;
  }

  @Id
  @GeneratedValue
  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  @Column(name = "EXECUTION_ID", length = 64, nullable = false)
  public String getExecutionId() {
    return executionId;
  }

  public void setExecutionId(String executionId) {
    this.executionId = executionId;
  }

  @ManyToOne(fetch = FetchType.LAZY, optional = false)
  @JoinColumn(name = "EXECUTION_ID", referencedColumnName = "ID", insertable = false,
      updatable = false, foreignKey = @ForeignKey(name = "FK_DPQLNORMCELL_EXECUTION"))
  public DpqlExecution getExecution() {
    return execution;
  }

  public void setExecution(DpqlExecution execution) {
    this.execution = execution;
  }

  @Column(name = "TABLE_ID")
  public Integer getTableId() {
    return tableId;
  }

  public void setTableId(Integer tableId) {
    this.tableId = tableId;
  }

  @Column(name = "KIND", length = 64, nullable = false)
  public String getKind() {
    return kind;
  }

  public void setKind(String kind) {
    this.kind = kind;
  }

  @Column(name = "ROW_ID", nullable = false)
  public int getRowId() {
    return rowId;
  }

  public void setRowId(int rowId) {
    this.rowId = rowId;
  }

  @Column(name = "COL_NAME", length = 64, nullable = false)
  public String getColName() {
    return colName;
  }

  public void setColName(String colName) {
    this.colName = colName;
  }

  @Column(name = "VALUE", columnDefinition = "LONGVARCHAR")
  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
  }
}
