package de.metanome.backend.dpql;

import de.metanome.backend.results_db.FileInput;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Prepares engine input datasets for DPQL execution.
 *
 * Requirement: engines read their input datasets from Metanome DB.
 * Therefore, before executing a query, copy all required input files
 * (derived from the FROM part via CC(...)) into the engine's dataset directory.
 */
final class DpqlDatasetPreparer {

    private DpqlDatasetPreparer() {
    }

    static class RequiredFiles {
        final boolean all;
        final Set<String> names;

        RequiredFiles(boolean all, Set<String> names) {
            this.all = all;
            this.names = names;
        }
    }

    static RequiredFiles parseRequiredFilesFromQuery(String query) {
        if (query == null) {
            return new RequiredFiles(false, new HashSet<String>());
        }
        String q = query;
        int n = q.length();

        boolean all = false;
        Set<String> names = new HashSet<>();

        for (int i = 0; i < n; i++) {
            // case-insensitive match for "CC"
            if (i + 1 >= n) break;
            char c1 = q.charAt(i);
            char c2 = q.charAt(i + 1);
            if (!((c1 == 'C' || c1 == 'c') && (c2 == 'C' || c2 == 'c'))) {
                continue;
            }

            int j = i + 2;
            while (j < n && Character.isWhitespace(q.charAt(j))) j++;
            if (j >= n || q.charAt(j) != '(') {
                continue;
            }
            int start = j + 1;
            int end = findMatchingParen(q, j);
            if (end < 0) {
                continue;
            }

            String inside = q.substring(start, end).trim();
            if (inside.equals("*")) {
                all = true;
                // We can stop early: star means copy everything.
                break;
            }

            for (String token : splitCommaList(inside)) {
                String cleaned = cleanupIdentifier(token);
                if (!cleaned.isEmpty()) {
                    names.add(cleaned);
                }
            }

            i = end; // continue after ')'
        }

        return new RequiredFiles(all, names);
    }

    static int copyRequiredFiles(List<FileInput> allInputs,
                                 RequiredFiles required,
                                 Path destinationDatasetDir) throws IOException {
        if (allInputs == null || allInputs.isEmpty()) {
            return 0;
        }
        if (required == null) {
            return 0;
        }

        Files.createDirectories(destinationDatasetDir);

        List<FileInput> toCopy;
        if (required.all) {
            toCopy = allInputs;
        } else {
            Map<String, FileInput> index = buildIndex(allInputs);
            toCopy = new ArrayList<>();
            for (String name : required.names) {
                FileInput match = index.get(normalize(name));
                if (match == null) {
                    // Try a second pass with extension heuristics.
                    match = index.get(normalize(stripExtension(name)));
                }
                if (match == null) {
                    throw new IllegalArgumentException("Unknown input file referenced in FROM/CC(): '" + name + "'");
                }
                toCopy.add(match);
            }
        }

        int copied = 0;
        for (FileInput fi : toCopy) {
            if (fi == null || fi.getFileName() == null || fi.getFileName().trim().isEmpty()) {
                continue;
            }
            File source = resolveExistingFile(fi.getFileName().trim());
            Path src = source.toPath();

            String destName = src.getFileName().toString();
            Path dest = destinationDatasetDir.resolve(destName);

            if (Files.exists(dest)) {
                try {
                    long srcSize = Files.size(src);
                    long destSize = Files.size(dest);
                    long srcMtime = Files.getLastModifiedTime(src).toMillis();
                    long destMtime = Files.getLastModifiedTime(dest).toMillis();
                    if (srcSize == destSize && destMtime >= srcMtime) {
                        continue;
                    }
                } catch (Exception ignored) {
                    // If any stat fails, just overwrite.
                }
            }

            Files.copy(src, dest, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.COPY_ATTRIBUTES);
            copied++;
        }
        return copied;
    }

    private static int findMatchingParen(String s, int openIndex) {
        int depth = 0;
        for (int i = openIndex; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '(') depth++;
            else if (c == ')') {
                depth--;
                if (depth == 0) return i;
            }
        }
        return -1;
    }

    private static List<String> splitCommaList(String inside) {
        List<String> out = new ArrayList<>();
        if (inside == null || inside.trim().isEmpty()) {
            return out;
        }
        // Minimal split: comma-separated identifiers (no quoting/escaping needed for current syntax).
        String[] parts = inside.split(",");
        for (String p : parts) {
            if (p == null) continue;
            String t = p.trim();
            if (!t.isEmpty()) out.add(t);
        }
        return out;
    }

    private static String cleanupIdentifier(String token) {
        if (token == null) return "";
        String t = token.trim();
        if (t.isEmpty()) return "";
        // Remove surrounding quotes/backticks
        if ((t.startsWith("\"") && t.endsWith("\"")) || (t.startsWith("'") && t.endsWith("'")) || (t.startsWith("`") && t.endsWith("`"))) {
            t = t.substring(1, t.length() - 1).trim();
        }
        // If user writes dataset.table, use table part for file matching.
        int dot = t.lastIndexOf('.');
        if (dot > 0 && dot < t.length() - 1) {
            // Keep extension like .csv by checking last dot against common endings.
            String suffix = t.substring(dot + 1);
            if (!("csv".equalsIgnoreCase(suffix) || "tsv".equalsIgnoreCase(suffix) || "txt".equalsIgnoreCase(suffix))) {
                t = t.substring(dot + 1);
            }
        }
        return t;
    }

    private static Map<String, FileInput> buildIndex(List<FileInput> allInputs) {
        Map<String, FileInput> index = new HashMap<>();
        for (FileInput fi : allInputs) {
            if (fi == null || fi.getFileName() == null) continue;
            String raw = fi.getFileName().trim();
            if (raw.isEmpty()) continue;
            Path p = Path.of(raw);
            String fileName = p.getFileName() != null ? p.getFileName().toString() : raw;
            String base = stripExtension(fileName);

            // Prefer first match; keep stable behavior.
            index.putIfAbsent(normalize(fileName), fi);
            index.putIfAbsent(normalize(base), fi);
        }
        return index;
    }

    private static String stripExtension(String name) {
        if (name == null) return null;
        int dot = name.lastIndexOf('.');
        if (dot > 0) return name.substring(0, dot);
        return name;
    }

    private static String normalize(String s) {
        return s == null ? "" : s.trim().toLowerCase(Locale.ROOT);
    }

    private static File resolveExistingFile(String raw) throws FileNotFoundException {
        if (raw == null || raw.trim().isEmpty()) {
            throw new FileNotFoundException("Empty file path");
        }
        String adjusted = raw;
        // Handle Windows '/C:/' style
        if (adjusted.matches("^/[A-Za-z]:.*")) {
            adjusted = adjusted.substring(1);
        }
        Path p = Path.of(adjusted);
        if (!p.isAbsolute()) {
            p = Path.of(System.getProperty("user.dir")).resolve(p).normalize();
        }
        File f = p.toFile();
        if (!f.isFile()) {
            throw new FileNotFoundException("Source file not found: " + f.getAbsolutePath());
        }
        return f;
    }
}
