package de.metanome.backend.algorithm_loading;

import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import static org.junit.Assert.assertTrue;

public class AlgorithmFinderTest {

  @Test
  public void getAvailableAlgorithmFileNames_FindsJarsInAlgorithmsResourceDirectory() throws Exception {
    AlgorithmFinder finder = new AlgorithmFinder();
    Path dir = Path.of(finder.getAlgorithmDirectory());
    Files.createDirectories(dir);

    Path jar = dir.resolve("demo-algorithm-finder-test.jar");
    try {
      Files.write(jar, new byte[]{0x00});
      assertTrue(Files.exists(jar));

    String[] files = finder.getAvailableAlgorithmFileNames(null);
    Arrays.sort(files);
      assertTrue("Expected uploaded jar to be listed", Arrays.asList(files).contains("demo-algorithm-finder-test.jar"));
    } finally {
      try { Files.deleteIfExists(jar); } catch (Exception ignore) {}
    }
  }
}
