/**
 * Copyright 2016 by Metanome Project
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
package de.metanome.backend.helper;


import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.configuration.ConfigurationSettingDatabaseConnection;
import de.metanome.algorithm_integration.configuration.ConfigurationSettingFileInput;
import de.metanome.algorithm_integration.configuration.ConfigurationSettingTableInput;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.backend.input.database.DefaultTableInputGenerator;
import de.metanome.backend.input.file.DefaultFileInputGenerator;
import de.metanome.backend.results_db.DatabaseConnection;
import de.metanome.backend.results_db.FileInput;
import de.metanome.backend.results_db.Input;
import de.metanome.backend.results_db.TableInput;
import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class InputToGeneratorConverter {

  public static RelationalInputGenerator convertInput(Input input)
    throws AlgorithmConfigurationException {
    if (input instanceof FileInput) {
      return new DefaultFileInputGenerator(convertInputToSetting((FileInput) input));
    } else if (input instanceof TableInput) {
      return new DefaultTableInputGenerator(convertInputToSetting((TableInput) input));
    } else if (input instanceof DatabaseConnection) {
      // we do not know which table was used for profiling, thus we can not compute
      // ranking results for results on database connections
      return null;
    }

    return null;
  }

  /**
   * Converts the given file input to a configuration setting.
   *
   * @param input file input
   * @return the configuration setting
   */
  public static ConfigurationSettingFileInput convertInputToSetting(FileInput input) {
    String filename = input.getFileName();
    if (filename == null || filename.trim().isEmpty()){
      filename = input.getName();
    } 
    String normalized = normalizePath(filename);
    return new ConfigurationSettingFileInput()
      .setEscapeChar(input.getEscapeChar())
      .setFileName(normalized)
      .setHeader(input.isHasHeader())
      .setIgnoreLeadingWhiteSpace(input.isIgnoreLeadingWhiteSpace())
      .setNullValue(input.getNullValue())
      .setQuoteChar(input.getQuoteChar())
      .setSeparatorChar(input.getSeparator())
      .setSkipDifferingLines(input.isSkipDifferingLines())
      .setSkipLines(input.getSkipLines())
      .setStrictQuotes(input.isStrictQuotes());
  }

  /**
   * normalize and resolves a potentially non-resolvable file path into an existing absolute path when possible.
   *
   * @param path the input path to normalize; may be null
   * @return a normalized existing absolute path when resolvable, null if input is path null}, otherwise the original path
   */
  private static String normalizePath(String path) {
    if (path == null){
      return null;
    } 
    try {
      // remove leading slash before drive letter for windows (e.g. /C:/...)
      if (path.matches("^/[A-Za-z]:.*")) path = path.substring(1);

      // Map /WEB-INF/classes/... to actual classes folder on disk
      final String classespath = "/WEB-INF/classes/";
      if (path.contains(classespath)) {
        String relativepath = path.substring(path.indexOf(classespath) + classespath.length());
        String classesFolder = resolveClassesFolder();
        if (classesFolder != null) {
          File finalpath = new File(classesFolder, relativepath);
          if (finalpath.exists()) return finalpath.getAbsolutePath();
        }
      }

      // If absolute and exists, return
      Path p = Paths.get(path);
      if (p.isAbsolute() && Files.exists(p)) return p.toAbsolutePath().toString();

      // relative path to user.dir
      Path relativepath = Paths.get(System.getProperty("user.dir")).resolve(path).normalize();
      if (Files.exists(relativepath)) return relativepath.toAbsolutePath().toString();

      // Fallback: search under classes/inputData by file leaf name
      String classesFolder = resolveClassesFolder();
      if (classesFolder != null) {
        Path root = Paths.get(classesFolder, "inputData");
        if (Files.exists(root)) {
          final String leaf = new File(path).getName();
          Path found = Files.walk(root).filter(Files::isRegularFile)
            .filter(x -> x.getFileName().toString().equalsIgnoreCase(leaf))
            .findFirst().orElse(null);
          if (found != null) return found.toAbsolutePath().toString();
        }
      }

      // Return original if we couldn't normalize
      return path;
    } catch (Exception e) {
      return path;
    }
  }

  /**
   * resolves the absolute path to the application classes directory (typically {@code WEB-INF/classes})
   * based on the runtime location of {@link DefaultFileInputGenerator}.
   *
   * <p>The method inspects the code source URL of {@code DefaultFileInputGenerator}, converts it to a
   * {@link java.io.File}, moves two directory levels up (to {@code WEB-INF}), and then appends
   * {@code "classes"} to build the target path.
   *
   * @return the absolute path to the resolved classes folder, or {@code null} if the path cannot be
   *         resolved (for example, when URI conversion fails or expected parent directories are missing)
   */
  private static String resolveClassesFolder() {
    try {
      URL baseUrl = DefaultFileInputGenerator.class.getProtectionDomain().getCodeSource().getLocation();
      File file = new File(baseUrl.toURI());
      // classes are under .../WEB-INF/classes
      File webInf = file.getAbsoluteFile().getParentFile().getParentFile();
      if (webInf != null) {
        return new File(webInf, "classes").getAbsolutePath();
      }
    } catch (URISyntaxException ignored) {
    }
    return null;
  }

  /**
   * Converts the given table input to a configuration setting.
   *
   * @param input table input
   * @return the configuration setting
   */
  public static ConfigurationSettingTableInput convertInputToSetting(TableInput input) {
    return new ConfigurationSettingTableInput()
      .setDatabaseConnection(convertInputToSetting(input.getDatabaseConnection()))
      .setTable(input.getTableName());
  }

  /**
   * Converts the given database connection input to a configuration setting.
   *
   * @param input database connection
   * @return the configuration setting
   */
  public static ConfigurationSettingDatabaseConnection convertInputToSetting(
    DatabaseConnection input) {
    return new ConfigurationSettingDatabaseConnection()
      .setDbUrl(input.getUrl())
      .setPassword(input.getPassword())
      .setSystem(input.getSystem())
      .setUsername(input.getUsername());
  }

}
