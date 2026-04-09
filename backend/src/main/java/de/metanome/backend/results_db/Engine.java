package de.metanome.backend.results_db;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.NamedQueries;
import jakarta.persistence.NamedQuery;
import java.io.Serializable;

/**
 * Represents a profiling query engine JAR registered in the database.
 *
 * Engines are loaded dynamically from the writable engines directory (see EngineFinder).
 */
@NamedQueries(@NamedQuery(name = "get all engines", query = "from Engine"))
@Entity
public class Engine implements Serializable {

  private static final long serialVersionUID = 1L;

  protected long id;
  protected String fileName;
  protected String name;
  protected String implementationTitle;
  protected String implementationVersion;

  protected Engine() {
    // for hibernate
  }

  public Engine(String fileName) {
    this.fileName = fileName;
  }

  @Id
  @GeneratedValue
  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  @Column(name = "fileName", unique = true)
  public String getFileName() {
    return fileName;
  }

  public Engine setFileName(String fileName) {
    this.fileName = fileName;
    return this;
  }

  public String getName() {
    return name;
  }

  public Engine setName(String name) {
    this.name = name;
    return this;
  }

  public String getImplementationTitle() {
    return implementationTitle;
  }

  public Engine setImplementationTitle(String implementationTitle) {
    this.implementationTitle = implementationTitle;
    return this;
  }

  public String getImplementationVersion() {
    return implementationVersion;
  }

  public Engine setImplementationVersion(String implementationVersion) {
    this.implementationVersion = implementationVersion;
    return this;
  }
}
