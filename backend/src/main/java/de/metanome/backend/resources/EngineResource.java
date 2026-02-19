package de.metanome.backend.resources;

import de.metanome.backend.algorithm_loading.FileUpload;
import de.metanome.backend.constants.Constants;
import de.metanome.backend.engine_loading.EngineFinder;
import de.metanome.backend.engine_loading.EngineJarLoader;
import de.metanome.backend.engine_loading.EngineJarMetadata;
import de.metanome.backend.results_db.Engine;
import de.metanome.backend.results_db.HibernateUtil;
import de.metanome.engine.api.EngineParameterSpec;
import de.metanome.engine.api.EngineProvider;
import de.metanome.engine.api.ProfilingQueryEngine;

import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.net.URLDecoder;

@Path("/engines")
public class EngineResource implements Resource<Engine> {

  private final EngineFinder engineFinder = new EngineFinder();
  private final EngineJarLoader jarLoader = new EngineJarLoader();

  /**
   * retrieve all available engines from the database.
   * @return a list of all available engines, or an empty list if no engines are found
   * @throws WebException if a database error occurs, with HTTP status BAD_REQUEST
   */
  @GET
  @Produces(Constants.APPLICATION_JSON_RESOURCE_PATH)
  @Override
  public List<Engine> getAll() {
    try {
      @SuppressWarnings(Constants.SUPPRESS_WARNINGS_UNCHECKED)
      List<Engine> engines = (List<Engine>) HibernateUtil.executeNamedQuery("get all engines");
      if (engines == null) {
        return Collections.emptyList();
      }

      // refresh metadata from the JAR for cases when older versions accidentally stored the wrong provider name.
      for (Engine engine : engines) {
        try {
          refreshMetadataFromJarIfPossible(engine);
        } catch (Exception ignore) {
          
        }
      }
      return engines;
    } catch (Exception e) {
      throw new WebException(e, Response.Status.BAD_REQUEST);
    }
  }

  /**
   * retrieve an Engine resource by its id
   * @param id the unique identifier of the Engine to retrieve
   * @return the Engine object associated with the given id, or null if not found
   * @throws WebException if an error occurs during retrieval with HTTP 400 Bad Request status
   */
  @GET
  @Path("/get/{id}")
  @Produces(Constants.APPLICATION_JSON_RESOURCE_PATH)
  @Override
  public Engine get(@PathParam("id") long id) {
    try {
      @SuppressWarnings(Constants.SUPPRESS_WARNINGS_UNCHECKED)
      Engine engine = (Engine) HibernateUtil.retrieve(Engine.class, id);
      if (engine != null) {
        try {
          refreshMetadataFromJarIfPossible(engine);
        } catch (Exception ignore) {
    
        }
      }
      return engine;
    } catch (Exception e) {
      throw new WebException(e, Response.Status.BAD_REQUEST);
    }
  }

  /**
   * uploads an engine jar into the engines directory and store in db
   * @throws WebException if the upload fails or the jar cannot be loaded, with HTTP status BAD_REQUEST
   */
  @POST
  @Path(Constants.STORE_RESOURCE_PATH)
  @Consumes("multipart/form-data")
  @Produces(Constants.APPLICATION_JSON_RESOURCE_PATH)
  public void uploadAndStore(@FormDataParam("file") InputStream uploadedInputStream,
      @FormDataParam("file") FormDataContentDisposition fileDetail) {
    try {
      // write uploaded file to disk (engine directory)
      String dir = engineFinder.getEngineDirectory();
      FileUpload uploader = new FileUpload();
      uploader.writeFileToDisk(uploadedInputStream, fileDetail, dir);

      // store in db
      Engine engine = new Engine(fileDetail.getFileName());
      store(engine);
    } catch (Exception e) {
      throw new WebException(e, Response.Status.BAD_REQUEST);
    }
  }

  /**
   * registers an existing engine jar file already present in the engines directory.
   * @throws WebException if storing the engine fails, with HTTP status BAD_REQUEST
   */
  @POST
  @Path(Constants.STORE_RESOURCE_PATH)
  @Consumes(Constants.APPLICATION_JSON_RESOURCE_PATH)
  @Produces(Constants.APPLICATION_JSON_RESOURCE_PATH)
  public void executeDatabaseStore(Engine engine) {
    try {
      store(engine);
    } catch (Exception e) {
      throw new WebException(e, Response.Status.BAD_REQUEST);
    }
  }

  /**
   * stores or updates an Engine resource in the database.
   * 
   * @param engine the engine object to store or update.
   * @return the stored or updated Engine object with populated metadata fields
   * @throws WebException if engine is null, fileName is null or empty, if the engine
   *                      JAR file cannot be loaded, or if any other error occurs during
   *                      processing with status BAD_REQUEST
   */
  @Override
  public Engine store(Engine engine) {
    if (engine == null || engine.getFileName() == null || engine.getFileName().trim().isEmpty()) {
      throw new WebException("Missing engine fileName", Response.Status.BAD_REQUEST);
    }

    try {
      // Load engine (via ServiceLoader) to capture display name
      EngineJarLoader.LoadedEngine loadedengine = jarLoader.loadEngine(engine.getFileName());
      try {
        String name = loadedengine.getEngine() != null ? loadedengine.getEngine().getName() : null;
        engine.setName(name != null && !name.trim().isEmpty() ? name : engine.getFileName());

        EngineJarMetadata meta = EngineJarMetadata.fromJarFile(loadedengine.getJarFile());
        engine.setImplementationTitle(meta.getImplementationTitle());
        engine.setImplementationVersion(meta.getImplementationVersion());
      } finally {
        loadedengine.close();
      }

      // Upsert by fileName
      Engine existing = findByFileName(engine.getFileName());
      if (existing != null) {
        existing.setName(engine.getName()).setImplementationTitle(engine.getImplementationTitle())
            .setImplementationVersion(engine.getImplementationVersion());
        HibernateUtil.update(existing);
        return existing;
      }

      HibernateUtil.store(engine);
      return engine;
    } catch (WebException e) {
      throw e;
    } catch (Exception e) {
      throw new WebException(e, Response.Status.BAD_REQUEST);
    }
  }

  /**
   * deletes an Engine resource by its id
   *
   * @param id the unique id of the Engine to be deleted
   * @throws WebException if the Engine cannot be found or if an error occurs during deletion, with HTTP status BAD_REQUEST
   */
  @DELETE
  @Path("/delete/{id}")
  @Override
  public void delete(@PathParam("id") long id) {
    try {
      @SuppressWarnings(Constants.SUPPRESS_WARNINGS_UNCHECKED)
      Engine engine = (Engine) HibernateUtil.retrieve(Engine.class, id);
      HibernateUtil.delete(engine);
    } catch (Exception e) {
      throw new WebException(e, Response.Status.BAD_REQUEST);
    }
  }

  /**
   * removes an engine and attempts to delete its physical JAR file from the engines directory. if the file is already absent, the DB entry will just be removed).
   *
   * @param id engine id
   * @return JSON summary containing id, fileName, fileDeleted flag
   */
  @DELETE
  @Path("/remove-with-file/{id}")
  @Produces(Constants.APPLICATION_JSON_RESOURCE_PATH)
  public Response deleteWithFile(@PathParam("id") long id) {
    try {
      @SuppressWarnings(Constants.SUPPRESS_WARNINGS_UNCHECKED)
      Engine engine = (Engine) HibernateUtil.retrieve(Engine.class, id);
      if (engine == null) {
        throw new WebException("Engine id " + id + " not found", Response.Status.NOT_FOUND);
      }

      String fileName = engine.getFileName();
      boolean fileDeleted = false;

      try {
        if (fileName != null && !fileName.trim().isEmpty()) {
          String enginesDir = engineFinder.getEngineDirectory();
          String decodedDir = URLDecoder.decode(enginesDir, Constants.FILE_ENCODING);
          java.nio.file.Path enginesDirPath = Paths.get(decodedDir).normalize();
          java.nio.file.Path jarPath = enginesDirPath.resolve(fileName).normalize();
          fileDeleted = Files.deleteIfExists(jarPath);
        }
      } catch (Exception ignore) {
        
      }

      HibernateUtil.delete(engine);

      java.util.Map<String, Object> summary = new java.util.HashMap<>();
      summary.put("id", id);
      summary.put("fileName", fileName);
      summary.put("fileDeleted", fileDeleted);
      return Response.ok(summary).build();
    } catch (WebException e) {
      throw e;
    } catch (Exception e) {
      throw new WebException(e, Response.Status.BAD_REQUEST);
    }
  }

  
  /**
   * Retrieves the parameter specifications for a specific engine.
   *
   * @param id the unique id of the engine
   * @return a list of {@link EngineParameterSpec} objects representing the parameter specifications of the engine, or an empty list if the engine has no parameters
   * @throws WebException if the engine with the given id is not found (HTTP 404) or if an error occurs while loading the engine (HTTP 400)
   */
  @GET
  @Path("/parameters/{id}")
  @Produces(Constants.APPLICATION_JSON_RESOURCE_PATH)
  public List<EngineParameterSpec> getEngineParameterSpecs(@PathParam("id") long id) {
    try {
      @SuppressWarnings(Constants.SUPPRESS_WARNINGS_UNCHECKED)
      Engine engineEntity = (Engine) HibernateUtil.retrieve(Engine.class, id);
      if (engineEntity == null || engineEntity.getFileName() == null) {
        throw new WebException("Engine id not found: " + id, Response.Status.NOT_FOUND);
      }

      try (EngineJarLoader.LoadedEngine loaded = jarLoader.loadEngine(engineEntity.getFileName())) {
        ProfilingQueryEngine engine = loaded.getEngine();
        if (engine == null) {
          return Collections.emptyList();
        }
        return engine.getParameterSpecifications();
      }
    } catch (WebException e) {
      throw e;
    } catch (Exception e) {
      throw new WebException(e, Response.Status.BAD_REQUEST);
    }
  }

  /**
   * Returns the DPQL parameter specifications for the default (classpath) engine.
   * @return a list of {@link EngineParameterSpec} objects representing the default engine's parameter specifications, or an empty list if the default engine is not available
   * @throws WebException if an error occurs while loading the engine or retrieving its parameter specifications, with a BAD_REQUEST HTTP status
   */
  @GET
  @Path("/parameters/default")
  @Produces(Constants.APPLICATION_JSON_RESOURCE_PATH)
  public List<EngineParameterSpec> getDefaultEngineParameterSpecs() {
    try {
      ProfilingQueryEngine engine = EngineProvider.loadDefaultEngine();
      if (engine == null) {
        return Collections.emptyList();
      }
      return engine.getParameterSpecifications();
    } catch (Exception e) {
      throw new WebException(e, Response.Status.BAD_REQUEST);
    }
  }

  /**
   * refreshes an engine's metadata by re-loading its JAR
   * Updates an existing engine resource.
   * @param engine the engine object containing the ID of the engine to update.
   * @return the updated engine object with all changes persisted.
   * @throws WebException with status BAD_REQUEST if the engine is null or has an invalid ID.
   */
  @POST
  @Path("/update")
  @Consumes(Constants.APPLICATION_JSON_RESOURCE_PATH)
  @Produces(Constants.APPLICATION_JSON_RESOURCE_PATH)
  @Override
  public Engine update(Engine engine) {
    if (engine == null || engine.getId() <= 0) {
      throw new WebException("Missing engine id", Response.Status.BAD_REQUEST);
    }
    try {
      @SuppressWarnings(Constants.SUPPRESS_WARNINGS_UNCHECKED)
      Engine existing = (Engine) HibernateUtil.retrieve(Engine.class, engine.getId());
      if (existing == null) {
        throw new WebException("Engine id not found: " + engine.getId(), Response.Status.NOT_FOUND);
      }
      // Re-use store() upsert logic by fileName
      Engine refreshed = new Engine(existing.getFileName());
      refreshed.setId(existing.getId());
      Engine updated = store(refreshed);
      return updated;
    } catch (WebException e) {
      throw e;
    } catch (Exception e) {
      throw new WebException(e, Response.Status.BAD_REQUEST);
    }
  }

  /**
   * Lists all engine jar files located in the engines directory.
   * @return list of all available engine file names.
   * @throws WebException with status BAD_REQUEST if an error occurs while retrieving the available engine file names.
   */
  @GET
  @Path("/available-engine-files/")
  @Produces(Constants.APPLICATION_JSON_RESOURCE_PATH)
  public List<String> listAvailableEngineFiles() {
    try {
      List<String> files = new ArrayList<>();
      Collections.addAll(files, engineFinder.getAvailableEngineFileNames());
      return files;
    } catch (Exception e) {
      throw new WebException(e, Response.Status.BAD_REQUEST);
    }
  }

  private Engine findByFileName(String fileName) {
    try {
      @SuppressWarnings(Constants.SUPPRESS_WARNINGS_UNCHECKED)
      List<Engine> engines = (List<Engine>) HibernateUtil.queryCriteria(Engine.class,
          HibernateUtil.eq("fileName", fileName));
      if (engines == null || engines.isEmpty()) {
        return null;
      }
      return engines.get(0);
    } catch (Exception e) {
      return null;
    }
  }

  private void refreshMetadataFromJarIfPossible(Engine engine) throws Exception {
    if (engine == null || engine.getFileName() == null || engine.getFileName().trim().isEmpty()) {
      return;
    }

    EngineJarMetadata meta;
    String resolvedName;
    try (EngineJarLoader.LoadedEngine loaded = jarLoader.loadEngine(engine.getFileName())) {
      ProfilingQueryEngine jarEngine = loaded.getEngine();
      String name = jarEngine != null ? jarEngine.getName() : null;
      resolvedName = (name != null && !name.trim().isEmpty()) ? name : engine.getFileName();
      meta = EngineJarMetadata.fromJarFile(loaded.getJarFile());
    }

    boolean changed = false;
    if (resolvedName != null && !resolvedName.equals(engine.getName())) {
      engine.setName(resolvedName);
      changed = true;
    }
    if (meta != null) {
      if (meta.getImplementationTitle() != null
          && !meta.getImplementationTitle().equals(engine.getImplementationTitle())) {
        engine.setImplementationTitle(meta.getImplementationTitle());
        changed = true;
      }
      if (meta.getImplementationVersion() != null
          && !meta.getImplementationVersion().equals(engine.getImplementationVersion())) {
        engine.setImplementationVersion(meta.getImplementationVersion());
        changed = true;
      }
    }

    if (changed) {
      HibernateUtil.update(engine);
    }
  }
}
