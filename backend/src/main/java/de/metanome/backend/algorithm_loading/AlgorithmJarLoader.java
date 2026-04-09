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
package de.metanome.backend.algorithm_loading;

import de.metanome.algorithm_integration.Algorithm;
import de.metanome.backend.constants.Constants;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

import com.ibm.db2.jcc.am.s;

public class AlgorithmJarLoader {
  protected Algorithm algorithmSubclass;

  /**
   * Loads a jar file containing an algorithm and returns an instance of the bootstrap class.
   *
   * @param filePath the file path to the algorithm jar
   * @return runnable algorithm
   * @throws IOException if the algorithm could not be loaded
   * @throws ClassNotFoundException if the algorithm could not be loaded
   * @throws InstantiationException if the algorithm could not be loaded
   * @throws IllegalAccessException if the algorithm could not be loaded
   * @throws IllegalArgumentException if the algorithm could not be loaded
   * @throws InvocationTargetException if the algorithm could not be loaded
   * @throws NoSuchMethodException if the algorithm could not be loaded
   * @throws SecurityException if the algorithm could not be loaded
   */
  public Algorithm loadAlgorithm(String filePath)
    throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException,
    IllegalArgumentException, InvocationTargetException, NoSuchMethodException,
    SecurityException {
    // Try to resolve the algorithm JAR from the classpath resource first
    System.out.println("Inside loadAlgorithm with file path: " + filePath);
    File file;
    URL resource = Thread.currentThread().getContextClassLoader()
      .getResource(Constants.ALGORITHMS_RESOURCE_NAME + Constants.FILE_SEPARATOR + filePath);
    if (resource != null) {
      String pathToFile = resource.getPath();
      System.out.println("Resolved algorithm jar file path from classpath resource not normalized: " + pathToFile);
      // On Windows, classloader may prepend a leading slash to drive letters
      if (System.getProperty("os.name").toLowerCase().contains("win") && pathToFile.matches("^/[A-Za-z]:/.*")) {
        pathToFile = pathToFile.substring(1);
        System.out.println("Adjusted Windows path: " + pathToFile);
      }
      file = new File(URLDecoder.decode(pathToFile, Constants.FILE_ENCODING));
    } else {
      // Fallback: look into the algorithms directory and build the file path manually
      System.out.println("Algorithm jar not found as classpath resource, falling back to algorithm directory: " + filePath);
      String algorithmsDir;
      try {
        algorithmsDir = new AlgorithmFinder().getAlgorithmDirectory();
        System.out.println("Algorithm directory resolved from resource URI: " + algorithmsDir);
      } catch (NullPointerException e) {
        algorithmsDir = null;
      }
      if (algorithmsDir != null) {
        String decodedDir = URLDecoder.decode(algorithmsDir, Constants.FILE_ENCODING);
        System.out.println("Decoded algorithm directory: " + decodedDir);
        file = new File(decodedDir, filePath);
      } else {
        file = new File(filePath);
      }
      if (!file.exists()) {
        System.out.println("Algorithm jar file not found at resolved path: " + file.getAbsolutePath());
        System.out.println("Attempting to find algorithm jar file in alternative locations...");
        File alternative = findAlternativeJar(file, filePath, algorithmsDir);
        if (alternative != null && alternative.isFile()) {
          file = alternative;
        } else {
          System.out.println("Algorithm jar file not found in alternative locations.");
          // Prepare a helpful error message with diagnostics
          String userDir = System.getProperty("user.dir");
          File dir = file.getParentFile() != null ? file.getParentFile() : new File(algorithmsDir == null ? "" : algorithmsDir);
          String[] available = (dir != null && dir.isDirectory()) ? dir.list((d, name) -> name.toLowerCase().endsWith(".jar")) : new String[0];
          String availableList = (available != null && available.length > 0) ? String.join(", ", available) : "<none>";
          throw new IOException("Algorithm jar not found: '" + filePath + "'\n"
            + "algorithmsDir='" + (algorithmsDir == null ? "<unknown>" : algorithmsDir) + "'\n"
            + "resolvedPath='" + file.getAbsolutePath() + "'\n"
            + "user.dir='" + userDir + "'\n"
            + "availableJars=" + availableList);
        }
      }
    }

    JarFile jar = new JarFile(file);

    Manifest man = jar.getManifest();
    Attributes attr = man.getMainAttributes();
    String className = attr.getValue(Constants.BOOTRSTAP_CLASS_TAG_NAME);
    if (className == null || className.trim().isEmpty()) {
      jar.close();
      throw new IOException("Algorithm jar '" + file.getName() + "' is missing manifest entry '" + Constants.BOOTRSTAP_CLASS_TAG_NAME + "'");
    }

    URL[] url = {file.toURI().toURL()};
    ClassLoader loader = new URLClassLoader(url, Algorithm.class.getClassLoader());

    Class<? extends Algorithm> algorithmClass =
      Class.forName(className, true, loader).asSubclass(Algorithm.class);

    jar.close();

    return algorithmClass.getConstructor().newInstance();
  }

  private File findAlternativeJar(File originalCandidate, String requestedPath, String algorithmsDir) {
    try {
      System.out.println("inside findAlternativeJar with requestedPath: " + requestedPath + ", algorithmsDir: " + algorithmsDir);
      String jarName = new File(requestedPath).getName();
      List<File> searchDirs = new ArrayList<>();
      LinkedHashSet<String> seen = new LinkedHashSet<>();

      if (originalCandidate != null && originalCandidate.getParentFile() != null) {
        addSearchDir(searchDirs, seen, originalCandidate.getParentFile());
      }

      if (algorithmsDir != null) {
        try {
          String decodedDir = URLDecoder.decode(algorithmsDir, Constants.FILE_ENCODING);
          addSearchDir(searchDirs, seen, new File(decodedDir));
        } catch (Exception ignore) {
          addSearchDir(searchDirs, seen, new File(algorithmsDir));
        }
      }

      String userDir = System.getProperty("user.dir");
      if (userDir != null) {
        addSearchDir(searchDirs, seen, new File(userDir));
      }

      for (File dir : searchDirs) {
        if (dir == null || !dir.isDirectory()) {
          continue;
        }
        File direct = new File(dir, jarName);
        if (direct.isFile()) {
          return direct;
        }
        File[] matches = dir.listFiles((d, name) -> name.equalsIgnoreCase(jarName));
        if (matches != null && matches.length > 0) {
          return matches[0];
        }
      }
    } catch (Exception ignore) {
      // Best-effort search only; fall through to return null.
    }
    return null;
  }

  private void addSearchDir(List<File> dirs, LinkedHashSet<String> seen, File dir) {
    if (dir == null) {
      return;
    }
    try {
      String key = dir.getCanonicalPath();
      if (seen.add(key)) {
        dirs.add(dir);
      }
    } catch (IOException e) {
      // Fall back to absolute path if canonical lookup fails
      String key = dir.getAbsolutePath();
      if (seen.add(key)) {
        dirs.add(dir);
      }
    }
  }

}
