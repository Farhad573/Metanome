/**
 * Copyright 2014-2017 by Metanome Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.metanome.backend.resources;

import de.metanome.backend.algorithm_loading.AlgorithmAnalyzer;
import de.metanome.backend.algorithm_loading.AlgorithmFinder;
import de.metanome.backend.algorithm_loading.AlgorithmJarLoader;
import de.metanome.backend.algorithm_loading.FileUpload;
import de.metanome.backend.constants.Constants;
import de.metanome.backend.results_db.Algorithm;
import de.metanome.backend.results_db.AlgorithmType;
import de.metanome.backend.results_db.EntityStorageException;
import de.metanome.backend.results_db.HibernateUtil;

import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;
import de.metanome.backend.results_db.HibernateUtil.PropertyFilter;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.net.URLDecoder;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

/**
 * Responsible for the database communication for algorithm and for handling all restful calls of
 * algorithms.
 *
 * @author Tanja Bergmann
 */
@Path(Constants.ALGORITHMS_RESOURCE_NAME)
public class AlgorithmResource implements Resource<Algorithm> {

  /**
   * Adds a algorithm to the database for file already existing in the algorithms directory.
   *
   * @param algorithm the algorithm stored in the application
   */
  @POST
  @Path(Constants.STORE_RESOURCE_PATH)
  @Consumes(Constants.APPLICATION_JSON_RESOURCE_PATH)
  @Produces(Constants.APPLICATION_JSON_RESOURCE_PATH)
  public void executeDatabaseStore(Algorithm algorithm) {
    try {
      store(algorithm);
    } catch (Exception e) {
      throw new WebException(e, Response.Status.BAD_REQUEST);
    }
  }



  /**
   * Uploads algorithm into the algorithms directory and adds the algorithm to the database
   *
   * @param uploadedInputStream Stream of File send to Backend
   * @param fileDetail Additional Meta-Information about the uploaded file
   */

  @POST
  @Path(Constants.STORE_RESOURCE_PATH)
  @Consumes("multipart/form-data")
  @Produces(Constants.APPLICATION_JSON_RESOURCE_PATH)
  public void uploadAndExecuteStore(@FormDataParam("file") InputStream uploadedInputStream,
                              @FormDataParam("file") FormDataContentDisposition fileDetail) {

    try {
      /* Check if Algorithm already exist */
      AlgorithmFinder jarFinder = new AlgorithmFinder();

      /* Upload file to algorithm directory */

      FileUpload fileToDisk= new FileUpload();
      Boolean fileExist =
            fileToDisk.writeFileToDisk(
                    uploadedInputStream,
                    fileDetail,
                    jarFinder.getAlgorithmDirectory());

      /* Add Algorithm to the Database if newly added using Store function*/
      if(!fileExist) {
        Algorithm algorithm = new Algorithm(fileDetail.getFileName());
        store(algorithm);
      }
    } catch(Exception e){
      throw new WebException(e, Response.Status.BAD_REQUEST);
    }
  }



  /**
   * Adds a algorithm to the database for file already existing in the algorithms directory.
   *
   * @param algorithm the algorithm stored in the application
   * @return the stored algorithm
   */
  public Algorithm store(Algorithm algorithm) {
    try {
      // Load the jar and get the author and description from the algorithm
      AlgorithmJarLoader loader = new AlgorithmJarLoader();
      de.metanome.algorithm_integration.Algorithm jarAlgorithm =
                                              loader.loadAlgorithm(algorithm.getFileName());
      String authors = jarAlgorithm.getAuthors();
      String description = jarAlgorithm.getDescription();

      algorithm = setAlgorithmTypes(algorithm);
      algorithm.setAuthor(authors);
      algorithm.setDescription(description);

      HibernateUtil.store(algorithm);
      return algorithm;
    } catch (Exception e) {
      throw new WebException(e, Response.Status.BAD_REQUEST);
    }
  }


  /**
   * Deletes the algorithm, which has the given id, from the database.
   *
   * @param id the id of the algorithm, which should be deleted
   */
  @DELETE
  @Path("/delete/{id}")
  @Override
  public void delete(@PathParam("id") long id) {
    try {
      Algorithm algorithm = (Algorithm) HibernateUtil.retrieve(Algorithm.class, id);
      HibernateUtil.delete(algorithm);
    } catch (Exception e) {
      throw new WebException(e, Response.Status.BAD_REQUEST);
    }
  }

  /**
   * deletes the algorithm (DB record) and removes the physical JAR file from the
   * algorithms directory. if the file is already absent, it just removes the DB entry.
   *
   * @param id algorithm id
   * @return JSON summary containing id, fileName, fileDeleted flag
   */
  @DELETE
  @Path("/remove-with-file/{id}")
  @Produces(Constants.APPLICATION_JSON_RESOURCE_PATH)
  public Response deleteWithFile(@PathParam("id") long id) {
    try {
      Algorithm algorithm = (Algorithm) HibernateUtil.retrieve(Algorithm.class, id);
      if (algorithm == null) {
        throw new WebException("Algorithm id " + id + " not found", Response.Status.NOT_FOUND);
      }
      String fileName = algorithm.getFileName();
      boolean fileDeleted = false;
      try {
        if (fileName != null && !fileName.isEmpty()) {
          AlgorithmFinder finder = new AlgorithmFinder();
          String dir = finder.getAlgorithmDirectory();
          // normaliz potential leading slash before windows drive letter
          if (dir.matches("^/[A-Za-z]:/.*")) {
            dir = dir.substring(1);
          }
          String decodedDir = URLDecoder.decode(dir,Constants.FILE_ENCODING);
          java.nio.file.Path algorithmsDirPath = java.nio.file.Paths.get(decodedDir).normalize();
          java.nio.file.Path jarPath = algorithmsDirPath.resolve(fileName);
          java.nio.file.Path normalized = jarPath.normalize();
          fileDeleted = java.nio.file.Files.deleteIfExists(normalized);
        }
      } catch (Exception fileEx) {
        // continue even if physical deletion fails
      }
      HibernateUtil.delete(algorithm);
      Map<String, Object> payload = new HashMap<>();
      payload.put("id", id);
      payload.put("fileName", fileName);
      payload.put("fileDeleted", fileDeleted);
      payload.put("fileDeletedAlgorithmsDir", fileDeleted);
      return Response.ok(payload).build();
    } catch (Exception e) {
      throw new WebException(e, Response.Status.BAD_REQUEST);
    }
  }

  /**
   * removes all algorithms from db and their physical JAR files where present. returns counts for
   * processed algorithms and deleted files.
   */
  @DELETE
  @Path("/remove-all-with-files")
  @Produces(Constants.APPLICATION_JSON_RESOURCE_PATH)
  public Response deleteAllWithFiles() {
    try {
      @SuppressWarnings(Constants.SUPPRESS_WARNINGS_UNCHECKED)
      List<Algorithm> all = (List<Algorithm>) HibernateUtil.queryCriteria(Algorithm.class);
      int fileDeletedCount = 0;
      AlgorithmFinder finder = new AlgorithmFinder();
      String dir = finder.getAlgorithmDirectory();
      // normaliz potential leading slash before windows drive letter
      if (dir.matches("^/[A-Za-z]:/.*")) {
        dir = dir.substring(1);
      }
      String decodedDir = URLDecoder.decode(dir, Constants.FILE_ENCODING);
      java.nio.file.Path algorithmsDirPath = java.nio.file.Paths.get(decodedDir).normalize();
      for (Algorithm algorithm : all) {
        String fileName = algorithm.getFileName();
        if (fileName != null && !fileName.isEmpty()) {
          try {
            java.nio.file.Path p = algorithmsDirPath.resolve(fileName).normalize();
            if (java.nio.file.Files.deleteIfExists(p)) {
              fileDeletedCount++;
            }
          } catch (Exception ignore) {
            
          }
        }
        try {
          HibernateUtil.delete(algorithm);
        } catch (Exception exception) {
          
        }
      }
      Map<String, Object> payload = new HashMap<>();
      payload.put("algorithmsProcessed", all.size());
      payload.put("filesDeleted", fileDeletedCount);
      return Response.ok(payload).build();
    } catch (Exception e) {
      throw new WebException(e, Response.Status.BAD_REQUEST);
    }
  }

  /**
   * Retrieves an Algorithm from the database.
   *
   * @param id the Algorithm's id
   * @return the algorithm
   */
  @GET
  @Path("/get/{id}")
  @Produces(Constants.APPLICATION_JSON_RESOURCE_PATH)
  @Override
  public Algorithm get(@PathParam("id") long id) {
    try {
      return (Algorithm) HibernateUtil.retrieve(Algorithm.class, id);
    } catch (Exception e) {
      throw new WebException(e, Response.Status.BAD_REQUEST);
    }
  }

  /**
   * @return all algorithms in the database
   */
  @GET
  @Produces(Constants.APPLICATION_JSON_RESOURCE_PATH)
  @SuppressWarnings(Constants.SUPPRESS_WARNINGS_UNCHECKED)
  @Override
  public List<Algorithm> getAll() {
    try {
      List<Algorithm> algorithms = (List<Algorithm>) HibernateUtil.queryCriteria(Algorithm.class);
      return algorithms;
    } catch (Exception e) {
      throw new WebException(e, Response.Status.BAD_REQUEST);
    }
  }

  /**
   * @return all inclusion dependency algorithms in the database
   */
  @GET
  @Path("/inclusion-dependency-algorithms/")
  @Produces(Constants.APPLICATION_JSON_RESOURCE_PATH)
  public List<Algorithm> listInclusionDependencyAlgorithms() {
    try {
      return listAlgorithms(AlgorithmType.IND.getAlgorithmClass());
    } catch (Exception e) {
      e.printStackTrace();
      throw new WebException(e, Response.Status.BAD_REQUEST);
    }
  }

  /**
   * @return all unique column combination algorithms in the database
   */
  @GET
  @Path("/unique-column-combination-algorithms/")
  @Produces(Constants.APPLICATION_JSON_RESOURCE_PATH)
  public List<Algorithm> listUniqueColumnCombinationsAlgorithms() {
    try {
      return listAlgorithms(AlgorithmType.UCC.getAlgorithmClass());
    } catch (Exception e) {
      e.printStackTrace();
      throw new WebException(e, Response.Status.BAD_REQUEST);
    }
  }

  /**
   * @return all conditional unique column combination algorithms in the database
   */
  @GET
  @Path("/conditional-unique-column-combination-algorithms/")
  @Produces(Constants.APPLICATION_JSON_RESOURCE_PATH)
  public List<Algorithm> listConditionalUniqueColumnCombinationsAlgorithms() {
    try {
      return listAlgorithms(AlgorithmType.CUCC.getAlgorithmClass());
    } catch (Exception e) {
      e.printStackTrace();
      throw new WebException(e, Response.Status.BAD_REQUEST);
    }
  }

  /**
   * @return all functional dependency algorithms in the database
   */
  @GET
  @Path("/functional-dependency-algorithms/")
  @Produces(Constants.APPLICATION_JSON_RESOURCE_PATH)
  public List<Algorithm> listFunctionalDependencyAlgorithms() {
    try {
      return listAlgorithms(AlgorithmType.FD.getAlgorithmClass());
    } catch (Exception e) {
      e.printStackTrace();
      throw new WebException(e, Response.Status.BAD_REQUEST);
    }
  }

  /**
   * @return all conditional inclusion dependency algorithms in the database
   */
  @GET
  @Path("/conditional-inclusion-dependency-algorithms/")
  @Produces(Constants.APPLICATION_JSON_RESOURCE_PATH)
  public List<Algorithm> listConditionalInclusionDependencyAlgorithms() {
    try {
      return listAlgorithms(AlgorithmType.CID.getAlgorithmClass());
    } catch (Exception e) {
      e.printStackTrace();
      throw new WebException(e, Response.Status.BAD_REQUEST);
    }
  }

  /**
   * @return all matching dependency algorithms in the database
   */
  @GET
  @Path("/matching-dependency-algorithms/")
  @Produces(Constants.APPLICATION_JSON_RESOURCE_PATH)
  public List<Algorithm> listMatchingDependencyAlgorithms() {
    try {
      return listAlgorithms(AlgorithmType.MD.getAlgorithmClass());
    } catch (Exception e) {
      e.printStackTrace();
      throw new WebException(e, Response.Status.BAD_REQUEST);
    }
  }

  /**
   * @return all conditional functional dependency algorithms in the database
   */
  @GET
  @Path("/conditional-functional-dependency-algorithms/")
  @Produces(Constants.APPLICATION_JSON_RESOURCE_PATH)
  public List<Algorithm> listConditionalFunctionalDependencyAlgorithms() {
    try {
      return listAlgorithms(AlgorithmType.CFD.getAlgorithmClass());
    } catch (Exception e) {
      e.printStackTrace();
      throw new WebException(e, Response.Status.BAD_REQUEST);
    }
  }

  /**
   * @return all order dependency algorithms in the database
   */
  @GET
  @Path("/order-dependency-algorithms/")
  @Produces(Constants.APPLICATION_JSON_RESOURCE_PATH)
  public List<Algorithm> listOrderDependencyAlgorithms() {
    try {
      return listAlgorithms(AlgorithmType.OD.getAlgorithmClass());
    } catch (Exception e) {
      e.printStackTrace();
      throw new WebException(e, Response.Status.BAD_REQUEST);
    }
  }
  
  /**
   * @return all multivalued dependency algorithms in the database
   */
  @GET
  @Path("/multivalued-dependency-algorithms/")
  @Produces(Constants.APPLICATION_JSON_RESOURCE_PATH)
  public List<Algorithm> listMultivaluedDependencyAlgorithms() {
    try {
      return listAlgorithms(AlgorithmType.MVD.getAlgorithmClass());
    } catch (Exception e) {
      e.printStackTrace();
      throw new WebException(e, Response.Status.BAD_REQUEST);
    }
  }

  /**
   * @return all basic statistics algorithms in the database
   */
  @GET
  @Path("/basic-statistics-algorithms/")
  @Produces(Constants.APPLICATION_JSON_RESOURCE_PATH)
  public List<Algorithm> listBasicStatisticsAlgorithms() {
    try {
      return listAlgorithms(AlgorithmType.BASIC_STAT.getAlgorithmClass());
    } catch (Exception e) {
      e.printStackTrace();
      throw new WebException(e, Response.Status.BAD_REQUEST);
    }
  }

  /**
   * @return all denial constraint algorithms in the database
   */
  @GET
  @Path("/denial-constraint-algorithms/")
  @Produces(Constants.APPLICATION_JSON_RESOURCE_PATH)
  public List<Algorithm> listDenialConstraintAlgorithms() {
    try {
      return listAlgorithms(AlgorithmType.DC.getAlgorithmClass());
    } catch (Exception e) {
      e.printStackTrace();
      throw new WebException(e, Response.Status.BAD_REQUEST);
    }
  }

  /**
   * Lists all algorithms from the database that implement a certain interface, or all if algorithm
   * class is null.
   *
   * @param algorithmClass the implemented algorithm interface.
   * @return the algorithms
   * @throws de.metanome.backend.results_db.EntityStorageException if algorithms could not be listed
   */
  @SuppressWarnings(Constants.SUPPRESS_WARNINGS_UNCHECKED)
  protected List<Algorithm> listAlgorithms(Class<?>... algorithmClass)
    throws EntityStorageException {
    Set<Class<?>> interfaces = new HashSet<>(Arrays.asList(algorithmClass));
    
    // Cannot directly use array, as some interfaces might not be relevant for query.
    PropertyFilter[] criteria = AlgorithmType.asStream().filter(type -> type.hasResult())
        .filter(executableType -> interfaces.contains(executableType.getAlgorithmClass()))
        .map(containedType -> HibernateUtil.eq(containedType.getAbbreviation(), true))
        .toArray(PropertyFilter[]::new);
    @SuppressWarnings(Constants.SUPPRESS_WARNINGS_UNCHECKED)
    List<Algorithm> algorithms =
        (List<Algorithm>) HibernateUtil.queryCriteria(Algorithm.class, criteria);
    return algorithms;
  }

  /**
   * Lists all algorithm file names located in the algorithm folder.
   *
   * @return list of algorithm file names
   */
  @GET
  @Path("/available-algorithm-files/")
  @Produces(Constants.APPLICATION_JSON_RESOURCE_PATH)
  public List<String> listAvailableAlgorithmFiles() {
    try {
      AlgorithmFinder algorithmFinder = new AlgorithmFinder();
      List<String> files = new ArrayList<>();
      Collections.addAll(files, algorithmFinder.getAvailableAlgorithmFileNames(null));
      return files;
    } catch (Exception e) {
      e.printStackTrace();
      throw new WebException(e, Response.Status.BAD_REQUEST);
    }
  }

  /**
   * Updates an algorithm in the database.
   *
   * @param algorithm the algorithm
   * @return the updated algorithm
   */
  @POST
  @Path("/update")
  @Consumes(Constants.APPLICATION_JSON_RESOURCE_PATH)
  @Produces(Constants.APPLICATION_JSON_RESOURCE_PATH)
  @Override
  public Algorithm update(Algorithm algorithm) {
    try {
      // Load the jar and get the author and description from the algorithm
      AlgorithmJarLoader loader = new AlgorithmJarLoader();
      de.metanome.algorithm_integration.Algorithm jarAlgorithm = loader.loadAlgorithm(algorithm.getFileName());
      String authors = jarAlgorithm.getAuthors();
      String description = jarAlgorithm.getDescription();

      algorithm = setAlgorithmTypes(algorithm);
      algorithm.setAuthor(authors);
      algorithm.setDescription(description);

      HibernateUtil.update(algorithm);
      return algorithm;
    } catch (Exception e) {
      e.printStackTrace();
      throw new WebException(e, Response.Status.BAD_REQUEST);
    }
  }

  private Algorithm setAlgorithmTypes(Algorithm algorithm) throws Exception {
    AlgorithmAnalyzer analyzer = new AlgorithmAnalyzer(algorithm.getFileName());

    AlgorithmType.asStream()
            .forEach( type -> algorithm.setAlgorithmType(type, analyzer.hasType(type)));

    return algorithm;
  }

  @GET
  @Path("/algorithms-for-file-inputs")
  @Produces(Constants.APPLICATION_JSON_RESOURCE_PATH)
  @SuppressWarnings(Constants.SUPPRESS_WARNINGS_UNCHECKED)
  public List<Algorithm> getAlgorithmsForFileInputs() {
    try {
      ArrayList<PropertyFilter> criteria = new ArrayList<>();
      criteria.add(HibernateUtil.eq("fileInput", true));

      List<Algorithm> algorithms = (List<Algorithm>) HibernateUtil.queryCriteria(Algorithm.class,
          criteria.toArray(new PropertyFilter[criteria.size()]));

      criteria = new ArrayList<>();
      criteria.add(HibernateUtil.eq("relationalInput", true));
      List<Algorithm> storedAlgorithms = (List<Algorithm>) HibernateUtil
          .queryCriteria(Algorithm.class, criteria.toArray(new PropertyFilter[criteria.size()]));
      if (algorithms != null) {
        algorithms.addAll(storedAlgorithms);
      }
      return algorithms;

    } catch (Exception e) {
      e.printStackTrace();
      throw new WebException(e, Response.Status.BAD_REQUEST);
    }
  }

  @GET
  @Path("/algorithms-for-table-inputs")
  @Produces(Constants.APPLICATION_JSON_RESOURCE_PATH)
  @SuppressWarnings(Constants.SUPPRESS_WARNINGS_UNCHECKED)
  public List<Algorithm> getAlgorithmsForTableInputs() {
    try {
      ArrayList<PropertyFilter> criteria = new ArrayList<>();
      criteria.add(HibernateUtil.eq("fileInput", true));

      List<Algorithm> algorithms = (List<Algorithm>) HibernateUtil.queryCriteria(Algorithm.class,
          criteria.toArray(new PropertyFilter[criteria.size()]));

      criteria = new ArrayList<>();
      criteria.add(HibernateUtil.eq("relationalInput", true));

      List<Algorithm> storedAlgorithms = (List<Algorithm>) HibernateUtil
          .queryCriteria(Algorithm.class, criteria.toArray(new PropertyFilter[criteria.size()]));
      if (algorithms != null) {
        algorithms.addAll(storedAlgorithms);
      }

      return algorithms;
    } catch (Exception e) {
      e.printStackTrace();
      throw new WebException(e, Response.Status.BAD_REQUEST);
    }
  }

  @GET
  @Path("/algorithms-for-database-connections")
  @Produces(Constants.APPLICATION_JSON_RESOURCE_PATH)
  @SuppressWarnings(Constants.SUPPRESS_WARNINGS_UNCHECKED)
  public List<Algorithm> getAlgorithmsForDatabaseConnections() {
    try {
      ArrayList<PropertyFilter> criteria = new ArrayList<>();
      criteria.add(HibernateUtil.eq("databaseConnection", true));

      List<Algorithm> algorithms = (List<Algorithm>) HibernateUtil.queryCriteria(Algorithm.class,
          criteria.toArray(new PropertyFilter[criteria.size()]));
      return algorithms;
    } catch (EntityStorageException e) {
      e.printStackTrace();
      throw new WebException(e, Response.Status.BAD_REQUEST);
    }
  }


}
