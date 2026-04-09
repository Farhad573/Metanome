/**
 * Copyright 2014-2016 by Metanome Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package de.metanome.backend.results_db;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.exception.ConstraintViolationException;
import org.hibernate.query.NativeQuery;
import org.hibernate.query.Query;
import org.hibernate.service.ServiceRegistry;

import java.io.Serializable;
import java.util.List;

import jakarta.persistence.Entity;
import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.Predicate;
import jakarta.persistence.criteria.Root;

/**
 * Used to perform low level database operations like storage and retrieval of objects.
 *
 * @author Jakob Zwiener
 */
public class HibernateUtil {

  public static final class PropertyFilter {
    private final String propertyName;
    private final Object value;

    private PropertyFilter(String propertyName, Object value) {
      this.propertyName = propertyName;
      this.value = value;
    }
  }

  public static PropertyFilter eq(String propertyName, Object value) {
    return new PropertyFilter(propertyName, value);
  }

  private static SessionFactory sessionFactory = null;

  /**
   * @return the singleton {@link SessionFactory}
   */
  public static synchronized SessionFactory getSessionFactory() {
    if (sessionFactory == null) {
      sessionFactory = buildSessionFactory();
      // hbm2ddl.auto=update does not reliably widen existing VARCHAR columns.
      // Our DPQL history stores potentially large JSON blobs; ensure columns are wide enough.
      ensureDpqlHistorySchema(sessionFactory);
    }

    return sessionFactory;
  }

  private static void ensureDpqlHistorySchema(SessionFactory sf) {
    Session session = null;
    try {
      session = sf.openSession();
      session.beginTransaction();

      // HSQLDB is case-insensitive but stores identifiers uppercased by default.
      // Use LONGVARCHAR to avoid truncation for large JSON payloads.
      tryExecuteAlter(session,
          "ALTER TABLE DPQLNORMALIZEDTABLE ALTER COLUMN COLUMNSJSON SET DATA TYPE LONGVARCHAR");
      tryExecuteAlter(session,
          "ALTER TABLE DPQLNORMALIZEDTABLE ALTER COLUMN ROWSJSON SET DATA TYPE LONGVARCHAR");
      tryExecuteAlter(session,
          "ALTER TABLE DPQLEXECUTION ALTER COLUMN QUERY SET DATA TYPE LONGVARCHAR");
      tryExecuteAlter(session,
          "ALTER TABLE DPQLNORMCELL ALTER COLUMN VALUE SET DATA TYPE LONGVARCHAR");

      session.getTransaction().commit();
    } catch (Exception ignored) {
      // Best-effort migration: older DBs may not have these tables yet, or syntax may differ.
      // DPQL persistence will still work on freshly created schemas.
      if (session != null) {
        try {
          if (session.getTransaction() != null && session.getTransaction().isActive()) {
            session.getTransaction().rollback();
          }
        } catch (Exception ignoredRollback) {
          // ignore
        }
      }
    } finally {
      if (session != null) {
        try {
          session.close();
        } catch (Exception ignoredClose) {
          // ignore
        }
      }
    }
  }

  private static void tryExecuteAlter(Session session, String sql) {
    try {
      NativeQuery<?> q = session.createNativeQuery(sql);
      q.executeUpdate();
    } catch (Exception ignored) {
      // ignore - column/table might not exist yet or already be correct type.
    }
  }

  /**
   * @return a fresh db session
   */
  public static Session openNewSession() {
    return getSessionFactory().openSession();
  }

  /**
   * Stores an entity in the database.
   *
   * @param entity the entity to store
   * @throws de.metanome.backend.results_db.EntityStorageException if constraints are violated or
   *         the entity is missing the Entity annotation
   */
  public static void store(Object entity) throws EntityStorageException {
    if (!entity.getClass().isAnnotationPresent(Entity.class)) {
      throw new EntityStorageException("Entity to store is missing the Entity annotation.");
    }

    Session session = openNewSession();

    session.beginTransaction();
    try {
      session.save(entity);
      session.getTransaction().commit();
    } catch (ConstraintViolationException e) {
      session.getTransaction().rollback();
      throw new EntityStorageException(
          "Could not store object because of a constraint violation exception", e);
    } finally {
      session.close();
    }
  }

  /**
   * Deletes an entity from the database.
   *
   * @param entity the entity to delete
   * @throws de.metanome.backend.results_db.EntityStorageException if the entity is missing the
   *         Entity annotation
   */
  public static void delete(Object entity) throws EntityStorageException {
    if (!entity.getClass().isAnnotationPresent(Entity.class)) {
      throw new EntityStorageException("Entity to delete is missing the Entity annotation.");
    }

    Session session = openNewSession();

    session.beginTransaction();
    session.delete(entity);
    session.getTransaction().commit();

    session.close();
  }

  /**
   * Update an entity from the database.
   *
   * @param entity the entity to update
   * @throws de.metanome.backend.results_db.EntityStorageException if the entity is missing the
   *         Entity annotation
   */
  public static void update(Object entity) throws EntityStorageException {
    if (!entity.getClass().isAnnotationPresent(Entity.class)) {
      throw new EntityStorageException("Entity to delete is missing the Entity annotation.");
    }

    Session session = openNewSession();

    session.beginTransaction();
    session.update(entity);
    session.getTransaction().commit();

    session.close();
  }


  /**
   * Retrieves an entity of the given class and with the given id from the database.
   *
   * @param clazz the class of the entity to retrieve
   * @param id the id of the entity to retrieve
   * @return the requested entity
   * @throws de.metanome.backend.results_db.EntityStorageException if the entity is missing the
   *         Entity annotation
   */
  public static Object retrieve(Class<?> clazz, Serializable id) throws EntityStorageException {
    if (!clazz.isAnnotationPresent(Entity.class)) {
      throw new EntityStorageException("Queried class is missing the Entity annotation.");
    }

    Session session = openNewSession();

    Object value = session.get(clazz, id);

    session.close();

    return value;
  }

  /**
   * Executes a named query and returns the result as {@link java.util.List}
   *
   * @param queryName the name of the query to execute
   * @return the query result as list
   */
  public static List<?> executeNamedQuery(String queryName) {
    Session session = openNewSession();

    Query<?> query = session.createNamedQuery(queryName);

    List<?> result = query.getResultList();

    session.close();

    return result;
  }

  /**
   * Creates and executes a criteria query of the type of the persistent class, after attaching all
   * filters in the array.
   *
   * @param persistentClass the type of {@link jakarta.persistence.Entity} to query
   * @param filters all the filters the results should match
   * @return the matching {@link jakarta.persistence.Entity}s
   * @throws de.metanome.backend.results_db.EntityStorageException if the entity is missing the
   *         Entity annotation
   */
  public static <T> List<T> queryCriteria(Class<T> persistentClass, PropertyFilter... filters)
      throws EntityStorageException {
    if (!persistentClass.isAnnotationPresent(Entity.class)) {
      throw new EntityStorageException("Class is missing the Entity annotation.");
    }

    Session session = openNewSession();

    CriteriaBuilder cb = session.getCriteriaBuilder();
    CriteriaQuery<T> cq = cb.createQuery(persistentClass);
    Root<T> root = cq.from(persistentClass);
    if (filters != null && filters.length > 0) {
      Predicate[] predicates = new Predicate[filters.length];
      for (int i = 0; i < filters.length; i++) {
        PropertyFilter filter = filters[i];
        predicates[i] = cb.equal(root.get(filter.propertyName), filter.value);
      }
      cq.where(predicates);
    }
    cq.select(root);

    List<T> results = session.createQuery(cq).getResultList();

    session.close();

    return results;
  }

  /**
   * Shuts down the database.
   */
  public static void shutdown() {
    getSessionFactory().close();
    sessionFactory = null;
  }

  /**
   * Clears the default schema (public).
   */
  public static void clear() {
    Session session = openNewSession();
    try {
      session.beginTransaction();
      NativeQuery<?> query = session.createNativeQuery("TRUNCATE SCHEMA public AND COMMIT");
      query.executeUpdate();
      session.getTransaction().commit();
    } catch (Exception ex) {
      if (session.getTransaction() != null && session.getTransaction().isActive()) {
        session.getTransaction().rollback();
      }
      throw ex;
    } finally {
      session.close();
    }
  }

  protected static SessionFactory buildSessionFactory() {
    Configuration configuration = new Configuration().configure();

    configuration.addAnnotatedClass(Algorithm.class);
    configuration.addAnnotatedClass(Engine.class);
    configuration.addAnnotatedClass(DatabaseConnection.class);
    configuration.addAnnotatedClass(Execution.class);
    configuration.addAnnotatedClass(FileInput.class);
    configuration.addAnnotatedClass(Input.class);
    configuration.addAnnotatedClass(Result.class);
    configuration.addAnnotatedClass(TableInput.class);
    configuration.addAnnotatedClass(ResultType.class);
    configuration.addAnnotatedClass(ExecutionSetting.class);
    configuration.addAnnotatedClass(DpqlExecution.class);
    configuration.addAnnotatedClass(DpqlNormalizedTable.class);
    configuration.addAnnotatedClass(DpqlNormCell.class);

    // Allow dynamic override of connection URL to match overridden DB port/name when running
    // parallel instances.
    String dbPort =
        firstNonEmpty(System.getProperty("metanome.db.port"), System.getenv("METANOME_DB_PORT"));
    String dbName =
        firstNonEmpty(System.getProperty("metanome.db.name"), System.getenv("METANOME_DB_NAME"));
    if (dbPort != null || dbName != null) {
      if (dbPort == null || dbPort.trim().isEmpty())
        dbPort = "9001";
      if (dbName == null || dbName.trim().isEmpty())
        dbName = "metanomedb";
      String overrideUrl = "jdbc:hsqldb:hsql://localhost:" + dbPort + "/" + dbName;
      configuration.setProperty("hibernate.connection.url", overrideUrl);
    }

    ServiceRegistry serviceRegistry =
        new StandardServiceRegistryBuilder().applySettings(configuration.getProperties()).build();
    return configuration.buildSessionFactory(serviceRegistry);
  }

  private static String firstNonEmpty(String... values) {
    for (String v : values) {
      if (v != null && !v.trim().isEmpty())
        return v.trim();
    }
    return null;
  }
}
