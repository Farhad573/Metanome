package de.metanome.backend.engine_loading;

import java.io.File;
import java.io.IOException;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

/**
 * Best-effort metadata extracted from an engine JAR.
 */
public final class EngineJarMetadata {

  private final String implementationTitle;
  private final String implementationVersion;

  public EngineJarMetadata(String implementationTitle, String implementationVersion) {
    this.implementationTitle = implementationTitle;
    this.implementationVersion = implementationVersion;
  }

  public String getImplementationTitle() {
    return implementationTitle;
  }

  public String getImplementationVersion() {
    return implementationVersion;
  }

  public static EngineJarMetadata fromJarFile(File jarFile) throws IOException {
    if (jarFile == null) {
      return new EngineJarMetadata(null, null);
    }

    try (JarFile jar = new JarFile(jarFile)) {
      Manifest manifest = jar.getManifest();
      if (manifest == null) {
        return new EngineJarMetadata(null, null);
      }
      Attributes attr = manifest.getMainAttributes();
      String title = attr.getValue("Implementation-Title");
      String version = attr.getValue("Implementation-Version");
      return new EngineJarMetadata(title, version);
    }
  }
}
