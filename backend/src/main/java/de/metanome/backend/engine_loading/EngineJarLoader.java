package de.metanome.backend.engine_loading;

import de.metanome.backend.constants.Constants;
import de.metanome.engine.api.ProfilingQueryEngine;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.charset.StandardCharsets;

/**
 * Dynamically loads a {@link ProfilingQueryEngine} implementation from a JAR file.
 *
 * Engine JARs are discovered via ServiceLoader, i.e. the JAR must contain:
 *   META-INF/services/de.metanome.engine.api.ProfilingQueryEngine
 */
public class EngineJarLoader {

  /**
   * ClassLoader that prefers classes from the engine JAR over the parent classpath.
   *
   * This avoids the situation where an older engine implementation on the backend classpath
   * shadows an updated implementation shipped via upload.
   *
   * We still delegate "API" packages parent-first to prevent duplicate interface classes
   * (which would lead to ClassCastException).
   */
  static final class ChildFirstEngineClassLoader extends URLClassLoader {
    ChildFirstEngineClassLoader(URL[] urls, ClassLoader parent) {
      super(urls, parent);
    }

    URL findChildResource(String name) {
      return super.findResource(name);
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
      synchronized (getClassLoadingLock(name)) {
        Class<?> c = findLoadedClass(name);
        if (c == null) {
          // Always parent-first for core/platform + Metanome APIs
          if (isParentFirst(name)) {
            c = getParent().loadClass(name);
          } else {
            try {
              c = findClass(name);
            } catch (ClassNotFoundException e) {
              c = getParent().loadClass(name);
            }
          }
        }

        if (resolve) {
          resolveClass(c);
        }
        return c;
      }
    }

    private boolean isParentFirst(String className) {
      if (className == null) {
        return true;
      }
      return className.startsWith("java.")
        || className.startsWith("javax.")
        || className.startsWith("jakarta.")
        || className.startsWith("sun.")
        || className.startsWith("org.w3c.")
        || className.startsWith("org.xml.")
        || className.startsWith("de.metanome.engine.api.")
        || className.startsWith("de.metanome.algorithm_integration.");
    }
  }

  public LoadedEngine loadEngine(String jarFileName) throws IOException {
    if (jarFileName == null || jarFileName.trim().isEmpty()) {
      throw new IOException("Missing engine jar fileName");
    }

    Path jarPath = resolveJarPath(jarFileName);
    if (!Files.exists(jarPath)) {
      throw new IOException("Engine jar not found: '" + jarFileName + "' (resolvedPath='" + jarPath.toAbsolutePath() + "')");
    }
    if (!jarPath.getFileName().toString().toLowerCase().endsWith(Constants.JAR_FILE_ENDING)) {
      throw new IOException("Engine file must be a .jar: '" + jarFileName + "'");
    }

    URL jarUrl = jarPath.toUri().toURL();
    ChildFirstEngineClassLoader loader = new ChildFirstEngineClassLoader(new URL[]{jarUrl}, ProfilingQueryEngine.class.getClassLoader());

    try {
      // IMPORTANT: Avoid ServiceLoader picking up providers from the *parent* classpath.
      // We want the provider declared inside the uploaded jar.
      String servicePath = "META-INF/services/" + ProfilingQueryEngine.class.getName();
      URL providerUrl = loader.findChildResource(servicePath);
      if (providerUrl == null) {
        throw new IOException(
          "No ProfilingQueryEngine implementation found in engine jar '" + jarPath.getFileName() + "'. "
            + "Ensure the jar contains " + servicePath);
      }

      String providerClassName;
      try (InputStream in = providerUrl.openStream();
           BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
        providerClassName = reader.lines()
          .map(String::trim)
          .filter(line -> !line.isEmpty())
          .filter(line -> !line.startsWith("#"))
          .findFirst()
          .orElse(null);
      }
      if (providerClassName == null || providerClassName.isEmpty()) {
        throw new IOException("Empty provider configuration in " + servicePath + " inside '" + jarPath.getFileName() + "'");
      }

      Class<?> raw = Class.forName(providerClassName, true, loader);
      Object instance = raw.getDeclaredConstructor().newInstance();
      if (!(instance instanceof ProfilingQueryEngine)) {
        throw new IOException("Provider '" + providerClassName + "' does not implement " + ProfilingQueryEngine.class.getName());
      }

      ProfilingQueryEngine engine = (ProfilingQueryEngine) instance;
      return new LoadedEngine(engine, loader, jarPath.toFile());
    } catch (RuntimeException e) {
      try { loader.close(); } catch (IOException ignore) {}
      throw e;
    } catch (ReflectiveOperationException e) {
      try { loader.close(); } catch (IOException ignore) {}
      throw new IOException("Could not instantiate engine provider from jar '" + jarPath.getFileName() + "': " + e.getMessage(), e);
    } catch (IOException e) {
      try { loader.close(); } catch (IOException ignore) {}
      throw e;
    }
  }

  private Path resolveJarPath(String jarFileName) throws IOException {
    // Absolute path passed in
    Path candidate = Paths.get(jarFileName);
    if (candidate.isAbsolute()) {
      return candidate.normalize();
    }

    // Default engines directory
    String enginesDir = new EngineFinder().getEngineDirectory();
    return Paths.get(enginesDir, jarFileName).normalize();
  }

  public static final class LoadedEngine implements AutoCloseable {
    private final ProfilingQueryEngine engine;
    private final URLClassLoader classLoader;
    private final File jarFile;

    private LoadedEngine(ProfilingQueryEngine engine, URLClassLoader classLoader, File jarFile) {
      this.engine = engine;
      this.classLoader = classLoader;
      this.jarFile = jarFile;
    }

    public ProfilingQueryEngine getEngine() {
      return engine;
    }

    public File getJarFile() {
      return jarFile;
    }

    @Override
    public void close() throws IOException {
      classLoader.close();
    }
  }
}
