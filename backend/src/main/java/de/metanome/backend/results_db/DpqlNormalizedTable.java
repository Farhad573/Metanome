package de.metanome.backend.results_db;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Lob;
import jakarta.persistence.ManyToOne;
import java.io.Serializable;

@Entity
public class DpqlNormalizedTable implements Serializable {

  private static final long serialVersionUID = 1L;

  private long id;
  private DpqlExecution execution;

  private String kind;
  private String name;
  private Integer sourceTableId;

  private String columnsJson;
  private String rowsJson;

  public DpqlNormalizedTable() {
    // for hibernate
  }

  @Id
  @GeneratedValue
  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  @ManyToOne(optional = false)
  public DpqlExecution getExecution() {
    return execution;
  }

  public void setExecution(DpqlExecution execution) {
    this.execution = execution;
  }

  @Column(length = 64)
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

  @Column(name = "SOURCE_TABLE_ID")
  public Integer getSourceTableId() {
    return sourceTableId;
  }

  public void setSourceTableId(Integer sourceTableId) {
    this.sourceTableId = sourceTableId;
  }

  @Lob
  @Column(columnDefinition = "LONGVARCHAR")
  public String getColumnsJson() {
    return columnsJson;
  }

  public void setColumnsJson(String columnsJson) {
    this.columnsJson = columnsJson;
  }

  @Lob
  @Column(columnDefinition = "LONGVARCHAR")
  public String getRowsJson() {
    return rowsJson;
  }

  public void setRowsJson(String rowsJson) {
    this.rowsJson = rowsJson;
  }
}
