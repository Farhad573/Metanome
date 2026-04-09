package de.metanome.backend.engine_loading;

import de.metanome.backend.constants.Constants;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

/**
 * Utilities to locate and list engine JARs that can be loaded at runtime.
 */
public class EngineFinder {

  public static final String ENGINES_RESOURCE_NAME = "engines";

  /**
   * Returns the engines directory from the classpath, creating it if needed.
   * Always includes a trailing file separator.
   */
  public String getEngineDirectory() throws IOException {
    URL resource = Thread.currentThread().getContextClassLoader().getResource(ENGINES_RESOURCE_NAME);
    if (resource == null) {
      throw new IOException("Engines directory is missing!");
    }
    if (!"file".equalsIgnoreCase(resource.getProtocol())) {
      throw new IOException("Engines directory is not a writable filesystem path: " + resource);
    }

    Path dir;
    try {
      dir = Paths.get(resource.toURI()).normalize();
    } catch (URISyntaxException | RuntimeException e) {
      dir = Paths.get(resource.getPath()).normalize();
    }
    Files.createDirectories(dir);

    String path = dir.toString();
    if (!path.endsWith(Constants.FILE_SEPARATOR)) {
      path = path + Constants.FILE_SEPARATOR;
    }
    return path;
  }

  /**
   * Lists all *.jar files in the engines directory.
   */
  public String[] getAvailableEngineFileNames() throws IOException {
    String dir = getEngineDirectory();
    File folder = new File(dir);
    if (!folder.isDirectory()) {
      return new String[]{};
    }

    FilenameFilter jarsOnly = (file, name) -> name != null && name.toLowerCase().endsWith(Constants.JAR_FILE_ENDING);
    String[] jars = folder.list(jarsOnly);
    if (jars == null) {
      return new String[]{};
    }

    Arrays.sort(jars);
    return jars;
  }
}
