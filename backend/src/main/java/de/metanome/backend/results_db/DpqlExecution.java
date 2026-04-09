package de.metanome.backend.results_db;

import jakarta.persistence.CascadeType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.Lob;
import jakarta.persistence.NamedQueries;
import jakarta.persistence.NamedQuery;
import jakarta.persistence.OneToMany;
import jakarta.persistence.Temporal;
import jakarta.persistence.TemporalType;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@NamedQueries(@NamedQuery(name = "get all dpql executions",
    query = "from DpqlExecution e order by e.createdAt desc"))
@Entity
public class DpqlExecution implements Serializable {

  private static final long serialVersionUID = 1L;

  private String id; // executionId (UUID)
  private Date createdAt;

  private String query;

  private Long engineId;
  private String engineFileName;
  private boolean normalizedOnly;

  private List<DpqlNormalizedTable> normalizedTables = new ArrayList<>();

  protected DpqlExecution() {
    // for hibernate
  }

  public DpqlExecution(String id) {
    this.id = id;
  }

  @Id
  @Column(length = 64)
  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  @Temporal(TemporalType.TIMESTAMP)
  public Date getCreatedAt() {
    return createdAt;
  }

  public void setCreatedAt(Date createdAt) {
    this.createdAt = createdAt;
  }

  @Lob
  @Column(columnDefinition = "LONGVARCHAR")
  public String getQuery() {
    return query;
  }

  public void setQuery(String query) {
    this.query = query;
  }

  public Long getEngineId() {
    return engineId;
  }

  public void setEngineId(Long engineId) {
    this.engineId = engineId;
  }

  public String getEngineFileName() {
    return engineFileName;
  }

  public void setEngineFileName(String engineFileName) {
    this.engineFileName = engineFileName;
  }

  public boolean isNormalizedOnly() {
    return normalizedOnly;
  }

  public void setNormalizedOnly(boolean normalizedOnly) {
    this.normalizedOnly = normalizedOnly;
  }

  @OneToMany(mappedBy = "execution", cascade = CascadeType.ALL, orphanRemoval = true,
      fetch = FetchType.EAGER)
  public List<DpqlNormalizedTable> getNormalizedTables() {
    return normalizedTables;
  }

  public void setNormalizedTables(List<DpqlNormalizedTable> normalizedTables) {
    this.normalizedTables = normalizedTables;
  }

  public void addNormalizedTable(DpqlNormalizedTable table) {
    if (table == null) {
      return;
    }
    table.setExecution(this);
    this.normalizedTables.add(table);
  }
}
