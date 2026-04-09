package de.metanome.backend.resources;

import de.metanome.backend.results_db.*;

import java.io.File;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * Utility that re-hydrates execution metadata from result files on disk. This allows the frontend
 * to browse historical executions even if the embedded database was cleared after a restart.
 */
public final class DiskResultImporter {

  private static final DateTimeFormatter IDENTIFIER_TIMESTAMP =
      DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS");
  private static final Map<String, ResultType> SUFFIX_TO_TYPE = buildSuffixMap();

  private DiskResultImporter() {}

  public static synchronized void importMissingExecutionsFromDisk() {
    File dir = new File("results");
    if (!dir.exists() || !dir.isDirectory()) {
      return;
    }

    File[] files = dir.listFiles();
    if (files == null || files.length == 0) {
      return;
    }

    for (File file : files) {
      if (!file.isFile()) {
        continue;
      }
      ResultType type = resolveTypeFromFile(file.getName());
      if (type == null) {
        continue;
      }
      DiskResultDescriptor descriptor = parseDescriptor(file.getName(), type);
      if (descriptor == null) {
        continue;
      }
      ensureExecution(descriptor, file.getAbsoluteFile(), type);
    }
  }

  public static Execution ensureExecutionForIdentifier(String identifier) {
    if (identifier == null || identifier.trim().isEmpty()) {
      return null;
    }
    try {
      Execution execution = findExecutionByIdentifier(identifier);
      if (execution != null) {
        return execution;
      }
      File resultFile = findFileForIdentifier(identifier);
      if (resultFile == null) {
        return null;
      }
      ResultType type = resolveTypeFromFile(resultFile.getName());
      if (type == null) {
        return null;
      }
      DiskResultDescriptor descriptor = parseDescriptor(resultFile.getName(), type);
      if (descriptor == null) {
        return null;
      }
      ensureExecution(descriptor, resultFile.getAbsoluteFile(), type);
      return findExecutionByIdentifier(identifier);
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }

  private static void ensureExecution(DiskResultDescriptor descriptor, File file, ResultType type) {
    if (descriptor == null) {
      return;
    }
    try {
      Execution existing = findExecutionByIdentifier(descriptor.identifier);
      Algorithm algorithm = ensureAlgorithm(descriptor.algorithmFileName, type);
      boolean algorithmUpdated = false;
      if (algorithm != null) {
        algorithmUpdated = applyTypeToAlgorithm(algorithm, type);
      }

      if (existing == null) {
        Execution execution = buildExecution(descriptor, algorithm);
        attachResult(execution, file, type);
        HibernateUtil.store(execution);
      } else {
        boolean needsUpdate = false;
        if (existing.getAlgorithm() == null && algorithm != null) {
          existing.setAlgorithm(algorithm);
          needsUpdate = true;
        }
        if (existing.getIdentifier() == null) {
          existing.setIdentifier(descriptor.identifier);
          needsUpdate = true;
        }
        if (existing.getBegin() == 0L && descriptor.timestampMillis > 0L) {
          existing.setBegin(descriptor.timestampMillis);
          existing.setEnd(descriptor.timestampMillis);
          needsUpdate = true;
        }
        needsUpdate |= attachResult(existing, file, type);
        if (existing.getExecutionSetting() == null) {
          ExecutionSetting setting = buildExecutionSetting(descriptor.identifier);
          existing.setExecutionSetting(setting);
          needsUpdate = true;
        }
        if (existing.getCountResult() == null) {
          existing.setCountResult(Boolean.TRUE);
          needsUpdate = true;
        }
        if (existing.isRunning()) {
          existing.setRunning(false);
          needsUpdate = true;
        }
        if (needsUpdate) {
          HibernateUtil.update(existing);
        }
      }

      if (algorithm != null && algorithmUpdated) {
        HibernateUtil.update(algorithm);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static ExecutionSetting buildExecutionSetting(String identifier) {
    ExecutionSetting setting =
        new ExecutionSetting(Collections.emptyList(), Collections.emptyList(), identifier);
    setting.setCacheResults(Boolean.TRUE);
    setting.setCountResults(Boolean.TRUE);
    setting.setWriteResults(Boolean.FALSE);
    return setting;
  }

  private static Execution buildExecution(DiskResultDescriptor descriptor, Algorithm algorithm) {
    Execution execution = new Execution();
    execution.setAlgorithm(algorithm);
    execution.setIdentifier(descriptor.identifier);
    execution.setBegin(descriptor.timestampMillis);
    execution.setEnd(descriptor.timestampMillis);
    execution.setRunning(false);
    execution.setAborted(false);
    execution.setCountResult(Boolean.TRUE);
    execution.setExecutionSetting(buildExecutionSetting(descriptor.identifier));
    execution.setResults(new HashSet<>());
    return execution;
  }

  private static boolean attachResult(Execution execution, File file, ResultType type) {
    if (execution == null || file == null) {
      return false;
    }
    String absolutePath = normalizePath(file);
    for (Result existing : execution.getResults()) {
      if (absolutePath.equalsIgnoreCase(normalizePath(existing.getFileName()))) {
        if (existing.getType() == null) {
          existing.setType(type);
          existing.setTypeName(type.getName());
          return true;
        }
        return false;
      }
    }
    Result result = new Result();
    result.setFileName(absolutePath);
    result.setType(type);
    result.setTypeName(type.getName());
    execution.addResult(result);
    return true;
  }

  private static Algorithm ensureAlgorithm(String fileName, ResultType type) {
    try {
      @SuppressWarnings("unchecked")
      List<Algorithm> algorithms = (List<Algorithm>) HibernateUtil.queryCriteria(Algorithm.class,
          HibernateUtil.eq("fileName", fileName));
      Algorithm algorithm = algorithms.isEmpty() ? null : algorithms.get(0);
      if (algorithm == null) {
        algorithm = new Algorithm(fileName);
        algorithm.setName(fileName);
        algorithm.setRelationalInput(true);
        algorithm.setFileInput(true);
        algorithm.setAuthor("Recovered");
        algorithm.setDescription("Recovered from results on disk");
        applyTypeToAlgorithm(algorithm, type);
        HibernateUtil.store(algorithm);
        return algorithm;
      }
      return algorithm;
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }

  private static boolean applyTypeToAlgorithm(Algorithm algorithm, ResultType type) {
    if (algorithm == null || type == null) {
      return false;
    }
    boolean changed = false;
    switch (type) {
      case FD:
      case RFD:
      case CFD:
        if (!algorithm.isFd()) {
          algorithm.setFd(true);
          changed = true;
        }
        break;
      case IND:
      case RIND:
      case CID:
        if (!algorithm.isInd()) {
          algorithm.setInd(true);
          changed = true;
        }
        break;
      case UCC:
      case RUCC:
        if (!algorithm.isUcc()) {
          algorithm.setUcc(true);
          changed = true;
        }
        break;
      case CUCC:
        if (!algorithm.isCucc()) {
          algorithm.setCucc(true);
          changed = true;
        }
        break;
      case BASIC_STAT:
        if (!algorithm.isBasicStat()) {
          algorithm.setBasicStat(true);
          changed = true;
        }
        break;
      case OD:
        if (!algorithm.isOd()) {
          algorithm.setOd(true);
          changed = true;
        }
        break;
      case MVD:
        if (!algorithm.isMvd()) {
          algorithm.setMvd(true);
          changed = true;
        }
        break;
      case MD:
        if (!algorithm.isMd()) {
          algorithm.setMd(true);
          changed = true;
        }
        break;
      case DC:
        if (!algorithm.isDc()) {
          algorithm.setDc(true);
          changed = true;
        }
        break;
      default:
        break;
    }
    if (!algorithm.isRelationalInput()) {
      algorithm.setRelationalInput(true);
      changed = true;
    }
    if (!algorithm.isFileInput()) {
      algorithm.setFileInput(true);
      changed = true;
    }
    return changed;
  }

  private static Execution findExecutionByIdentifier(String identifier) {
    try {
      @SuppressWarnings("unchecked")
      List<Execution> executions = (List<Execution>) HibernateUtil.queryCriteria(Execution.class,
          HibernateUtil.eq("identifier", identifier));
      return executions.isEmpty() ? null : executions.get(0);
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }

  private static ResultType resolveTypeFromFile(String fileName) {
    if (fileName == null) {
      return null;
    }
    for (Map.Entry<String, ResultType> entry : SUFFIX_TO_TYPE.entrySet()) {
      if (fileName.endsWith(entry.getKey())) {
        return entry.getValue();
      }
    }
    return null;
  }

  private static DiskResultDescriptor parseDescriptor(String fileName, ResultType type) {
    if (fileName == null || type == null) {
      return null;
    }
    String suffix = type.getEnding();
    if (!fileName.endsWith(suffix)) {
      return null;
    }
    String withoutSuffix = fileName.substring(0, fileName.length() - suffix.length());
    int lastUnderscore = withoutSuffix.lastIndexOf('_');
    if (lastUnderscore < 0) {
      return null;
    }
    String timestampString = withoutSuffix.substring(lastUnderscore + 1);
    String algorithmFileName = withoutSuffix.substring(0, lastUnderscore);
    if (!timestampString.matches("\\d{17}")) {
      return null;
    }
    long millis = parseTimestamp(timestampString);
    String identifier = algorithmFileName + "_" + timestampString;
    return new DiskResultDescriptor(identifier, algorithmFileName, millis);
  }

  private static long parseTimestamp(String timestampString) {
    try {
      LocalDateTime localDateTime = LocalDateTime.parse(timestampString, IDENTIFIER_TIMESTAMP);
      return localDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
    } catch (Exception e) {
      return 0L;
    }
  }

  private static File findFileForIdentifier(String identifier) {
    File dir = new File("results");
    if (!dir.exists() || !dir.isDirectory()) {
      return null;
    }
    File[] files = dir.listFiles();
    if (files == null) {
      return null;
    }
    for (File file : files) {
      if (file.getName().startsWith(identifier)) {
        return file.getAbsoluteFile();
      }
    }
    return null;
  }

  private static String normalizePath(String path) {
    if (path == null) {
      return null;
    }
    try {
      return Paths.get(path).toAbsolutePath().normalize().toString();
    } catch (Exception e) {
      return path;
    }
  }

  private static Map<String, ResultType> buildSuffixMap() {
    Map<String, ResultType> map = new LinkedHashMap<>();
    for (ResultType type : ResultType.values()) {
      map.put(type.getEnding(), type);
    }
    return map;
  }

  private static String normalizePath(File file) {
    return file == null ? null : normalizePath(file.getPath());
  }

  private static class DiskResultDescriptor {
    final String identifier;
    final String algorithmFileName;
    final long timestampMillis;

    DiskResultDescriptor(String identifier, String algorithmFileName, long timestampMillis) {
      this.identifier = identifier;
      this.algorithmFileName = algorithmFileName;
      this.timestampMillis = timestampMillis;
    }
  }
}
