/**
 * Copyright 2014-2016 by Metanome Project
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

import de.metanome.backend.algorithm_loading.FileUpload;
import de.metanome.backend.algorithm_loading.InputDataFinder;
import de.metanome.backend.constants.Constants;
import de.metanome.backend.results_db.EntityStorageException;
import de.metanome.backend.results_db.FileInput;
import de.metanome.backend.results_db.HibernateUtil;
import de.metanome.algorithm_integration.configuration.*;
import de.metanome.backend.input.file.DefaultFileInputGenerator;
import de.metanome.algorithm_integration.input.RelationalInput;


import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataParam;
import java.io.File;
import java.io.InputStream;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.net.URLDecoder;
import java.nio.file.InvalidPathException;
import java.nio.file.Paths;
import java.util.ArrayList;

import java.util.List;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

@Path("file-inputs")
public class FileInputResource implements Resource<FileInput> {

  private final InputDataFinder inputDataFinder;
  private static final boolean is_windows = System.getProperty("os.name").toLowerCase().contains("win");

  public FileInputResource() {
    inputDataFinder = new InputDataFinder();
  }

  @GET
  @Path("/available-input-files")
  @Produces(Constants.APPLICATION_JSON_RESOURCE_PATH)
  public List<String> listAvailableInputFiles() {
    try {
      File[] csvFiles = inputDataFinder.getAvailableFiles(true);

      List<String> csvInputFilePaths = new ArrayList<>();
      for (int i = 0; i < csvFiles.length; i++) {
        csvInputFilePaths.add(i, csvFiles[i].getPath());
      }

      return csvInputFilePaths;
    } catch (Exception e) {
      e.printStackTrace();
      throw new WebException(e, Response.Status.BAD_REQUEST);
    }
  }

  /**
   * @return all FileInputs in the database
   */
  @GET
  @Produces(Constants.APPLICATION_JSON_RESOURCE_PATH)
  @SuppressWarnings(Constants.SUPPRESS_WARNINGS_UNCHECKED)
  @Override
  public List<FileInput> getAll() {
    try {
      return (List<FileInput>) HibernateUtil.queryCriteria(FileInput.class);
    } catch (Exception e) {
      e.printStackTrace();
      throw new WebException(e, Response.Status.BAD_REQUEST);
    }
  }

  /**
   * @return all Paths of FileInputs in the database as Strings
   */
  public List<String> getAllPaths() {
    List<String> pathList = new ArrayList<>();
    List<FileInput> inputList = getAll();
    for (FileInput elem : inputList) {
      pathList.add(elem.getFileName());
    }
    return pathList;
  }

  /**
   * Updates a file input in the database.
   *
   * @param fileInput the file input
   * @return the updated file input
   */
  @POST
  @Path("/update")
  @Consumes(Constants.APPLICATION_JSON_RESOURCE_PATH)
  @Produces(Constants.APPLICATION_JSON_RESOURCE_PATH)
  @Override
  public FileInput update(FileInput fileInput) {
    try {
      HibernateUtil.update(fileInput);
    } catch (EntityStorageException e1) {
      e1.printStackTrace();
    } catch (Exception e) {
      e.printStackTrace();
      throw new WebException(e, Response.Status.BAD_REQUEST);
    }
    return fileInput;
  }

  /**
   * retrieves a FileInput from the Database
   *
   * @param id the id of the FileInput
   * @return the retrieved FileInput
   */
  @GET
  @Path("/get/{id}")
  @Produces(Constants.APPLICATION_JSON_RESOURCE_PATH)
  @Override
  public FileInput get(@PathParam("id") long id) {
    try {
      FileInput fileInput = (FileInput) HibernateUtil.retrieve(FileInput.class, id);
      if (fileInput != null) {
        File resolved = findphysicalfile(fileInput);
        if (resolved != null && resolved.isFile()) {
          fileInput.setFileName(resolved.getAbsolutePath());
        }
      }
      return fileInput;
    } catch (EntityStorageException e1) {
      e1.printStackTrace();
      throw new WebException(e1, Response.Status.BAD_REQUEST);
    } catch (Exception e) {
      e.printStackTrace();
      throw new WebException(e, Response.Status.BAD_REQUEST);
    }
  }

  /**
   * if file contains a Directory it returns the files in that directory
   * if file contains a file it returns this file
   *
   * @param file FileInput that is either a directory or a file
   * stores FileInputs of the retrieved Files
   */
  @POST
  @Path("/get-directory-files")
  @Consumes(Constants.APPLICATION_JSON_RESOURCE_PATH)
  @Produces(Constants.APPLICATION_JSON_RESOURCE_PATH)
  public void getDirectoryFiles(FileInput file) {
    try {
      FileInput newFile = store(file);
      File inpFile = new File(newFile.getFileName());
      List<String> pathList = getAllPaths();

      if (inpFile.isDirectory()) {
        File[] directoryFiles = inpFile.listFiles(new FilenameFilter() {
          @Override
          public boolean accept(File file, String name) {
            for (String fileEnding : Constants.ACCEPTED_FILE_ENDINGS_ARRAY) {
              if (name.endsWith(fileEnding)) {
                return true;
              }
            }
            return false;
          }
        });
        delete(newFile.getId());
        for (File curFile : directoryFiles) {
          if (!pathList.contains(curFile.getAbsolutePath())) {
            store(new FileInput(curFile.getAbsolutePath()));
          }
        }
      } else if (inpFile.isFile()) {
        delete(newFile.getId());
        store(newFile);
      } else {
        throw new FileNotFoundException();
      }
    } catch (Exception e) {
      e.printStackTrace();
      throw new WebException(e, Response.Status.BAD_REQUEST);
    }
  }

  /**
   * Passes parameter to store function.
   *
   * @param file FileInput to store
   */
  @POST
  @Path(Constants.STORE_RESOURCE_PATH)
  @Consumes(Constants.APPLICATION_JSON_RESOURCE_PATH)
  @Produces(Constants.APPLICATION_JSON_RESOURCE_PATH)
  public void executeDatabaseStore(FileInput file) {
    try {
      store(file);
    } catch (Exception e) {
      throw new WebException(e, Response.Status.BAD_REQUEST);
    }
  }


  @POST
  @Path(Constants.STORE_RESOURCE_PATH)
  @Consumes("multipart/form-data")
  @Produces(Constants.APPLICATION_JSON_RESOURCE_PATH)
  public List<FileInput> uploadAndExecuteStore(@FormDataParam("file") List<FormDataBodyPart> fileParts) {

    try {
      if (fileParts == null || fileParts.isEmpty()) {
        throw new WebException("No files uploaded", Response.Status.BAD_REQUEST);
      }

      InputDataFinder localFinder = new InputDataFinder();
      String targetDirectory = localFinder.getFileDirectory();
      FileUpload fileToDisk = new FileUpload();

      List<FileInput> stored = new ArrayList<>();
      for (FormDataBodyPart part : fileParts) {
        if (part == null) {
          continue;
        }
        FormDataContentDisposition fileDetail = part.getFormDataContentDisposition();
        String fileName = fileDetail != null ? fileDetail.getFileName() : null;
        if (fileName == null || fileName.trim().isEmpty()) {
          continue;
        }

        try (InputStream uploadedInputStream = part.getValueAs(InputStream.class)) {
          if (uploadedInputStream == null) {
            continue;
          }

          Boolean fileExist = fileToDisk.writeFileToDisk(
            uploadedInputStream,
            fileDetail,
            targetDirectory);

          java.nio.file.Path storedPath = Paths.get(targetDirectory).normalize().resolve(fileName).normalize();
          boolean alreadyRegistered = isPathRegistered(storedPath.normalize().toString());

          if (!fileExist || !alreadyRegistered) {
            FileInput file = new FileInput(storedPath.toString());
            file.setName(fileName);
            store(file);
            stored.add(file);
          }
        }
      }

      return stored;
    } catch(Exception e){
      e.printStackTrace();
      throw new WebException(e, Response.Status.BAD_REQUEST);
    }
  }

  /**
   * Stores FileInput into the Database
   *
   * @param file FileInput to store
   * @return stored FileInput
   */

  public FileInput store(FileInput file) {
    try {
      HibernateUtil.store(file);
    } catch (EntityStorageException e1) {
      e1.printStackTrace();
    } catch (Exception e) {
      e.printStackTrace();
      throw new WebException(e, Response.Status.BAD_REQUEST);
    }
    return file;
  }

  /**
   * Deletes the FileInput, which has the given id, from the database.
   *
   * @param id the id of the FileInput, which should be deleted
   */
  @DELETE
  @Path("/delete/{id}")
  @Override
  public void delete(@PathParam("id") long id) {
    try {
      FileInput fileInput = (FileInput) HibernateUtil.retrieve(FileInput.class, id);
      if (fileInput == null) {
        return;
      }
      File physicalFile = findphysicalfile(fileInput);
      if (physicalFile != null && physicalFile.isFile() && !physicalFile.delete()) {
        throw new WebException("Could not delete dataset file: " + physicalFile.getPath(),
            Response.Status.INTERNAL_SERVER_ERROR);
      }
      HibernateUtil.delete(fileInput);
    } catch (EntityStorageException e1) {
      e1.printStackTrace();
    } catch (Exception e) {
      e.printStackTrace();
      throw new WebException(e, Response.Status.BAD_REQUEST);
    }
  }

  /**
   * Returns a small preview of the dataset content (headers and first N rows).
   */
  @SuppressWarnings("resource")
  @GET
  @Path("/preview/{id}/{lines}")
  @Produces(Constants.APPLICATION_JSON_RESOURCE_PATH)
  public Map<String, Object> preview(@PathParam("id") long id, @PathParam("lines") int lines) {
      if (lines <= 0) lines = 50;
      FileInput fileinput;
      try{
      fileinput = (FileInput) HibernateUtil.retrieve(FileInput.class, id);
      } catch (EntityStorageException es) {
        es.printStackTrace();
        throw new WebException(es, Response.Status.BAD_REQUEST);
      } catch (Exception e) {
        e.printStackTrace();
        throw new WebException(e, Response.Status.BAD_REQUEST);
      }
      if (fileinput == null) {
        throw new WebException("FileInput not found", Response.Status.NOT_FOUND);
      }  
      // Build setting from stored FileInput
      ConfigurationSettingFileInput setting = new ConfigurationSettingFileInput();
      setting.setFileName(fileinput.getFileName());
      setting.setSeparatorChar(fileinput.getSeparator());
      setting.setQuoteChar(fileinput.getQuoteChar());
      setting.setEscapeChar(fileinput.getEscapeChar());
      setting.setSkipLines(fileinput.getSkipLines() != null ? fileinput.getSkipLines() : 0);
      setting.setStrictQuotes(fileinput.isStrictQuotes());
      setting.setIgnoreLeadingWhiteSpace(fileinput.isIgnoreLeadingWhiteSpace());
      setting.setHeader(fileinput.isHasHeader());
      setting.setSkipDifferingLines(fileinput.isSkipDifferingLines());
      setting.setNullValue(fileinput.getNullValue());

      // get physical file path
      File physicalFile = getpathfromfilename(fileinput.getFileName());
      if (!physicalFile.isFile()) {
        String name = fileinput.getFileName() != null ? new File(fileinput.getFileName()).getName() : null;
        File filefrominputdirectory = getfilefromfolder(inputDataFinder.getFileDirectory(), name);
        if (filefrominputdirectory != null) {
          physicalFile = filefrominputdirectory;
        }
      }
      if (!physicalFile.isFile()) {
        throw new WebException("Underlying file not found for preview: " + fileinput.getFileName(), Response.Status.BAD_REQUEST);
      }

      // Use file iterator to read preview rows
      List<String> headers;
      List<List<String>> rows = new ArrayList<>();
      int count = 0;
      DefaultFileInputGenerator generator;
      try{
        generator = new DefaultFileInputGenerator(physicalFile,setting);
      } catch (Exception e) {
        e.printStackTrace();
        throw new WebException(e, Response.Status.BAD_REQUEST);
      }
      RelationalInput relationalinput;
      try {
        relationalinput = generator.generateNewCopy();
      } catch (Exception e) {
        e.printStackTrace();
        throw new WebException(e, Response.Status.BAD_REQUEST);
      }
      headers = relationalinput.columnNames();
      try {
      while (relationalinput.hasNext() && count < lines) {
        List<String> row = relationalinput.next();
        rows.add(row);
        count++;
      }
      } catch (Exception e) {
        e.printStackTrace();
        throw new WebException(e, Response.Status.BAD_REQUEST);
      }
      try {
        relationalinput.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
      
      Map<String, Object> result = new HashMap<>();
      result.put("headers", headers);
      result.put("rows", rows);
      result.put("lineCount", count);
      return result;
  }

  /**
   * convert a normalized file path to a File object.
   *
   * @param path the file path string to convert
   * @return a File object representing the given path
   *         
   */
  private File getpathfromfilename(String path) {
    String normalized = normalizewindowspath(path);
    if (normalized == null || normalized.trim().isEmpty()) {
      return new File("");
    }
    return new File(normalized);
  }

  /**
   * get a file from the given directory with the given file name.
   * 
   * @param directory the directory path to search in
   * @param fileName the name of the file to get
   * @return the File object if a valid file exists at the given path
   */
  private File getfilefromfolder(String directory, String fileName) {
    if (directory == null || fileName == null || fileName.trim().isEmpty()) {
      return null;
    }
    String normalizedDir = normalizewindowspath(directory);
    try {
      java.nio.file.Path path = Paths.get(normalizedDir).normalize().resolve(fileName).normalize();
      File file = path.toFile();
      return file.isFile() ? file : null;
    } catch (InvalidPathException ex) {
      return null;
    }
  }

  /**
   * check if the given path is already registered in the system
   * 
   * @param normalizedPath the path to check for if registered. can be null
   * @return true if the path matches any of the existing registered paths, false otherwise.
   */
  private boolean isPathRegistered(String normalizedPath) {
    if (normalizedPath == null || normalizedPath.trim().isEmpty()) {
      return false;
    }
    Set<String> existingPaths = new HashSet<>();
    for (String storedpath : getAllPaths()) {
      if (storedpath == null) {
        continue;
      }
      addpathtoset(existingPaths, getpathfromfilename(storedpath));
      String nameOnly = new File(storedpath).getName();
      addpathtoset(existingPaths, getfilefromfolder(inputDataFinder.getFileDirectory(), nameOnly));
    }
    for (String candidate : existingPaths) {
      if (pathsEqual(candidate, normalizedPath)) {
        return true;
      }
    }
    return false;
  }

  /**
   * add the normalized path of given file to the given set if the file is valid.
   * 
   * @param paths_set the set to which the file path will be added; can be null
   * @param filetocheck the file whose path should be added; if null or not a file, the method returns without action
   */
  private void addpathtoset(Set<String> paths_set, File filetocheck) {
    if (paths_set == null || filetocheck == null || !filetocheck.isFile()) {
      return;
    }
    String normalized_path = filetocheck.toPath().normalize().toString();
    paths_set.add(normalized_path);
    
  }

  /**
   * compares two file paths for equality
   *
   * @param a the first path 
   * @param b the second path
   * @return {@code true} if the paths are equal {@code false} if either path is null or if the paths are not equal
   *         
   */
  private boolean pathsEqual(String a, String b) {
    if (a == null || b == null) {
      return false;
    }
    // windows paths are case-insensitive. so we compare case-insensitively on windows and case-sensitively on other OSes.
    return is_windows ? a.equalsIgnoreCase(b) : a.equals(b);
  }

  /**
   * normalizes file paths for Windows systems by removing leading slash from windows drive letters.
   *  
   * @param path the file path to normalize, may be null
   * @return the normalized path, or null if the input path is null
   */
  private String normalizewindowspath(String path) {
    System.out.println("Normalizing path: " + path);
    if (path == null) {
      return null;
    }
    String normalized = path;
    if (is_windows && normalized.matches("^/[A-Za-z]:/.*")) {
      normalized = normalized.substring(1);
    }
    System.out.println("Normalized path: " + normalized);
    try {
      // to make sure url-encoded paths are also handled. 
      normalized = URLDecoder.decode(normalized, Constants.FILE_ENCODING);
    } catch (Exception e) {
      
    }
    return normalized;
  }


  /**
   * finds the physical file corresponding to the given FileInput.
   * 
   * @param fileInput the FileInput object containing the file name to get.
   *                  Can be null.
   * @return the File object representing the physical file if found, or null if the input fileInput is null
   *         
   */
  private File findphysicalfile(FileInput fileInput) {
    if (fileInput == null) {
      return null;
    }
    File filename = getpathfromfilename(fileInput.getFileName());
    if (filename.isFile()) {
      System.out.println("Resolved physical file for FileInput " + fileInput.getId() + ": " + filename.getAbsolutePath());
      return filename;
    }
    String name = filename.getName();
    File fromInputDir = getfilefromfolder(inputDataFinder.getFileDirectory(), name);
    if (fromInputDir != null) {
      System.out.println("Resolved physical file from input directory for FileInput " + fileInput.getId() + ": " + fromInputDir.getAbsolutePath());
      return fromInputDir;
    }
    return filename;
  }
}
