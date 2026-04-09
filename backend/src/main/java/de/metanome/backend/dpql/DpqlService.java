package de.metanome.backend.dpql;

import de.metanome.backend.dpql.result.DiskResultCollector;
import de.metanome.backend.dpql.result.ResultReader;
import de.metanome.backend.dpql.result.CancelAwareResultReceiver;
import de.metanome.backend.engine_loading.EngineJarLoader;
import de.metanome.backend.results_db.FileInput;
import de.metanome.backend.results_db.Engine;
import de.metanome.backend.results_db.DpqlExecution;
import de.metanome.backend.results_db.DpqlNormCell;
import de.metanome.backend.results_db.DpqlNormalizedTable;
import de.metanome.backend.results_db.EntityStorageException;
import de.metanome.backend.results_db.HibernateUtil;
import de.metanome.engine.api.CancellationToken;
import de.metanome.engine.api.EngineException;
import de.metanome.engine.api.EngineExecutionContext;
import de.metanome.engine.api.EngineParameterSpec;
import de.metanome.engine.api.EngineParameterType;
import de.metanome.engine.api.EngineResult;
import de.metanome.engine.api.EngineTable;
import de.metanome.engine.api.ResultKind;
import de.metanome.engine.api.ProfilingQueryEngine;
import de.metanome.engine.api.result_receiver.EngineResultReceiverException;
import de.metanome.engine.api.result_receiver.EngineResultMetadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.core.type.TypeReference;

import org.hibernate.Session;
import org.hibernate.query.NativeQuery;

import java.io.File;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Date;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DpqlService {
    private static final String RESULTS_DIR = "results"; // Relative to backend working dir

    private final EngineJarLoader engineJarLoader = new EngineJarLoader();
    private final ObjectMapper mapper = new ObjectMapper();

    public DpqlService() {
        new File(RESULTS_DIR).mkdirs();
    }

    private static void throwIfCanceled(CancellationToken token) throws EngineException {
        if (Thread.currentThread().isInterrupted()) {
            throw new EngineException("Execution canceled");
        }
        if (token != null) {
            token.throwIfCanceled();
        }
    }

    public String executeToDisk(DpqlQuerryRequest request) throws IOException {
        if (request == null || request.getQuery() == null || request.getQuery().isEmpty()) {
            throw new IllegalArgumentException("DPQL query must not be empty");
        }

        final String executionId = java.util.UUID.randomUUID().toString();

        // Create an empty placeholder file so the UI can start polling results immediately.
        new File(RESULTS_DIR).mkdirs();
        File out = new File(RESULTS_DIR, executionId + ".ndjson");
        if (!out.exists()) {
            // best-effort
            out.createNewFile();
        }

        DpqlRunRegistry.Entry entry = DpqlRunRegistry.register(executionId);
        entry.message = "Queued";

        entry.future = DpqlRunRegistry.executor().submit(() -> {
            entry.status = DpqlRunRegistry.Status.RUNNING;
            entry.startedAt = System.currentTimeMillis();
            entry.message = "Running";
            de.metanome.engine.api.result_receiver.EngineResultReceiver receiver = null;
            try {
                final CancellationToken cancel = entry.cancellationToken;
                throwIfCanceled(cancel);

                EngineExecutionContext ctx = createExecutionContext(request);
                ctx.setCancellationToken(entry.cancellationToken);
                prepareDatasetsFromDb(request.getQuery(), ctx);

                throwIfCanceled(cancel);

                DiskResultCollector disk = new DiskResultCollector(RESULTS_DIR, executionId);
                receiver = new CancelAwareResultReceiver(disk, entry.cancellationToken);

                executeWithSelectedEngine(request, ctx, receiver);

                // If cancel was requested during engine execution, stop immediately.
                throwIfCanceled(cancel);

                if (Boolean.TRUE.equals(request.getNormalizedOnly())) {
                    // OOM-safe: build normalized tables + DPQLNORMCELL by streaming from disk
                    // results
                    // instead of collecting all tables/rows in memory.
                    persistDpqlExecutionFromDisk(executionId, request, cancel);
                } else {
                    // Store metadata for history/prefill; no normalized tables.
                    throwIfCanceled(cancel);
                    persistDpqlExecution(executionId, request, null);
                }

                // Avoid overwriting CANCELED with FINISHED if cancel came in late.
                throwIfCanceled(cancel);

                entry.status = DpqlRunRegistry.Status.FINISHED;
                entry.finishedAt = System.currentTimeMillis();
                entry.message = "Finished";
            } catch (Throwable t) {
                if (entry.status == DpqlRunRegistry.Status.CANCELED) {
                    // keep canceled
                } else {
                    entry.status = DpqlRunRegistry.Status.FAILED;
                    entry.finishedAt = System.currentTimeMillis();
                    entry.error = t.getClass().getName()
                            + (t.getMessage() != null ? (": " + t.getMessage()) : "");
                    entry.message = "Failed";
                }
                try {
                    t.printStackTrace();
                } catch (Exception ignored) {
                }
            }
        });

        return executionId;
    }

    public List<Map<String, Object>> listRuns() {
        @SuppressWarnings("unchecked")
        List<DpqlExecution> items = (List<DpqlExecution>) (List<?>) HibernateUtil
                .executeNamedQuery("get all dpql executions");
        List<Map<String, Object>> out = new ArrayList<>();
        if (items == null) {
            return out;
        }
        for (DpqlExecution e : items) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("executionId", e.getId());
            row.put("createdAt", e.getCreatedAt() != null ? e.getCreatedAt().getTime() : null);
            row.put("query", e.getQuery());
            row.put("engineId", e.getEngineId());
            row.put("engineFileName", e.getEngineFileName());
            row.put("normalizedOnly", e.isNormalizedOnly());
            row.put("hasNormalizedTables",
                    e.getNormalizedTables() != null && !e.getNormalizedTables().isEmpty());
            out.add(row);
        }
        return out;
    }

    /**
     * Deletes a stored DPQL run from the database and best-effort removes associated stored
     * artifacts. Intended for cleanup when results are no longer available.
     */
    public boolean deleteRun(String executionId) {
        if (executionId == null || executionId.trim().isEmpty()) {
            return false;
        }

        final DpqlExecution e;
        try {
            e = (DpqlExecution) HibernateUtil.retrieve(DpqlExecution.class, executionId);
        } catch (EntityStorageException ex) {
            throw new IllegalArgumentException(
                    "Failed to load DPQL run from database: " + ex.getMessage(), ex);
        }
        if (e == null) {
            return false;
        }

        // Best-effort cleanup of normalized cells for this execution.
        Session session = null;
        try {
            session = HibernateUtil.openNewSession();
            session.beginTransaction();
            NativeQuery<?> delete =
                    session.createNativeQuery("DELETE FROM DPQLNORMCELL WHERE EXECUTION_ID = :id");
            delete.setParameter("id", executionId);
            delete.executeUpdate();
            session.getTransaction().commit();
        } catch (Exception ignored) {
            try {
                if (session != null && session.getTransaction() != null) {
                    session.getTransaction().rollback();
                }
            } catch (Exception ignored2) {
                // ignore
            }
        } finally {
            if (session != null) {
                try {
                    session.close();
                } catch (Exception ignored) {
                    // ignore
                }
            }
        }

        try {
            HibernateUtil.delete(e);
        } catch (Exception ex) {
            throw new IllegalArgumentException(
                    "Failed to delete DPQL run from database: " + ex.getMessage(), ex);
        }

        // Best-effort delete disk results.
        try {
            File f = new File(RESULTS_DIR, executionId + ".ndjson");
            if (f.exists()) {
                Files.deleteIfExists(f.toPath());
            }
        } catch (Exception ignored) {
            // ignore
        }

        return true;
    }

    public DpqlExpandResponseDto expand(DpqlExpandRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("Request must not be null");
        }
        if (request.getExecutionId() == null || request.getExecutionId().trim().isEmpty()) {
            throw new IllegalArgumentException("executionId must not be empty");
        }

        String where = extractWhereClause(request.getWhere());
        if (where == null || where.trim().isEmpty()) {
            throw new IllegalArgumentException("where must not be empty");
        }

        List<Atom> atoms = parseAndOnlyAtoms(where);
        if (atoms.isEmpty()) {
            throw new IllegalArgumentException("No supported atoms found in where clause");
        }

        final DpqlExecution exec;
        try {
            exec = (DpqlExecution) HibernateUtil.retrieve(DpqlExecution.class,
                    request.getExecutionId());
        } catch (EntityStorageException ex) {
            throw new IllegalArgumentException(
                    "Failed to load DPQL run from database: " + ex.getMessage(), ex);
        }
        if (exec == null || exec.getNormalizedTables() == null
                || exec.getNormalizedTables().isEmpty()) {
            throw new IllegalArgumentException(
                    "No normalized tables found for executionId=" + request.getExecutionId());
        }

        // If a pattern repeats the same kind (e.g., IND(...) AND IND(...)), we must keep per-atom
        // identifiers to avoid SQL alias collisions and to keep rowIds distinguishable in the
        // response.
        Map<String, Integer> kindCounts = new LinkedHashMap<>();
        for (Atom a : atoms) {
            kindCounts.put(a.kind,
                    (kindCounts.get(a.kind) == null ? 0 : kindCounts.get(a.kind)) + 1);
        }

        String anchorKind = normalizeKind(request.getAnchorKind());
        Integer anchorRowId = request.getAnchorRowId();
        List<Integer> anchorRowIds = request.getAnchorRowIds();

        // Normalize: if anchorRowIds is provided and non-empty, use it; otherwise fall back to
        // single anchorRowId
        List<Integer> effectiveAnchorRowIds = null;
        if (anchorRowIds != null && !anchorRowIds.isEmpty()) {
            effectiveAnchorRowIds = anchorRowIds;
        } else if (anchorRowId != null) {
            effectiveAnchorRowIds = Collections.singletonList(anchorRowId);
        }

        int offset = request.getOffset() == null ? 0 : Math.max(0, request.getOffset());
        int limit =
                request.getLimit() == null ? 100 : Math.max(1, Math.min(1000, request.getLimit()));

        // Determine anchor atom (optional).
        int anchorAtomIndex = -1;
        if (anchorKind != null && effectiveAnchorRowIds != null
                && !effectiveAnchorRowIds.isEmpty()) {
            for (int i = 0; i < atoms.size(); i++) {
                if (anchorKind.equals(atoms.get(i).kind)) {
                    anchorAtomIndex = i;
                    break;
                }
            }
            if (anchorAtomIndex < 0) {
                throw new IllegalArgumentException(
                        "anchorKind does not match any atom kind in where clause");
            }
        }

        LinkedHashMap<String, Integer> varOwner = new LinkedHashMap<>();
        List<String> variables = new ArrayList<>();
        for (int i = 0; i < atoms.size(); i++) {
            for (String v : atoms.get(i).args) {
                if (!varOwner.containsKey(v)) {
                    varOwner.put(v, i);
                    variables.add(v);
                }
            }
        }

        List<Integer> atomTableIds = new ArrayList<>(atoms.size());
        for (Atom a : atoms) {
            Integer tableId = resolveTableIdForAtom(exec.getNormalizedTables(), a);
            if (tableId == null && kindCounts.get(a.kind) != null && kindCounts.get(a.kind) > 1) {
                throw new IllegalArgumentException("Ambiguous atom for kind " + a.kind
                        + " without table ids: " + atomToString(a));
            }
            atomTableIds.add(tableId);
        }

        ParamBuilder pb = new ParamBuilder();

        // Build subqueries.
        List<String> subqueries = new ArrayList<>();
        for (int i = 0; i < atoms.size(); i++) {
            Atom a = atoms.get(i);
            boolean isAnchor = (i == anchorAtomIndex);
            subqueries.add(buildAtomSubquerySql(pb, request.getExecutionId(), a, i,
                    atomTableIds.get(i), isAnchor ? effectiveAnchorRowIds : null));
        }

        // Build main join query.
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT ");

        // 1) RowIds for each atom
        for (int i = 0; i < atoms.size(); i++) {
            if (i > 0)
                sql.append(", ");
            sql.append("t").append(i).append(".ROW_ID AS \"")
                    .append(rowIdAliasForAtom(atoms, kindCounts, i)).append("\"");
        }

        // 2) Variable bindings (one per variable, from its owner table)
        for (String v : variables) {
            Integer owner = varOwner.get(v);
            sql.append(", t").append(owner).append(".\"").append(v).append("\" AS \"").append(v)
                    .append("\"");
        }

        sql.append(" FROM ").append(subqueries.get(0)).append(" t0");

        Set<String> seen = new HashSet<>(atoms.get(0).args);
        for (int i = 1; i < atoms.size(); i++) {
            Atom a = atoms.get(i);
            sql.append(" JOIN ").append(subqueries.get(i)).append(" t").append(i).append(" ON ");
            boolean any = false;
            for (String v : a.args) {
                if (!seen.contains(v)) {
                    continue;
                }
                int owner = varOwner.get(v);
                if (owner == i) {
                    // should not happen for seen variables
                    continue;
                }
                if (any) {
                    sql.append(" AND ");
                }
                sql.append("t").append(i).append(".\"").append(v).append("\" = t").append(owner)
                        .append(".\"").append(v).append("\"");
                any = true;
            }
            if (!any) {
                sql.append("1=1");
            }
            seen.addAll(a.args);
        }

        sql.append(" OFFSET ").append(pb.param(offset)).append(" ROWS FETCH NEXT ")
                .append(pb.param(limit)).append(" ROWS ONLY");

        List<Map<String, Object>> tuples = new ArrayList<>();
        Session session = null;
        try {
            session = HibernateUtil.openNewSession();
            NativeQuery<?> q = session.createNativeQuery(sql.toString());
            pb.bindAll(q);

            @SuppressWarnings("unchecked")
            List<Object[]> rows = (List<Object[]>) q.list();
            for (Object[] row : rows) {
                Map<String, Integer> rowIds = new LinkedHashMap<>();
                Map<String, String> bindings = new LinkedHashMap<>();

                int idx = 0;
                for (int i = 0; i < atoms.size(); i++) {
                    Object v = row[idx++];
                    Integer rowId = v == null ? null : ((Number) v).intValue();
                    rowIds.put(rowIdKeyForAtom(atoms, kindCounts, i), rowId);
                }
                for (String v : variables) {
                    Object vv = row[idx++];
                    bindings.put(v, vv == null ? null : String.valueOf(vv));
                }

                Map<String, Object> tuple = new LinkedHashMap<>();
                tuple.put("rowIds", rowIds);
                tuple.put("bindings", bindings);
                tuples.add(tuple);
            }
        } finally {
            if (session != null) {
                try {
                    session.close();
                } catch (Exception ignored) {
                    // ignore
                }
            }
        }

        DpqlExpandResponseDto out = new DpqlExpandResponseDto();
        out.setVariables(variables);
        out.setTuples(tuples);
        out.setOffset(offset);
        out.setLimit(limit);
        return out;
    }

    private static String rowIdKeyForAtom(List<Atom> atoms, Map<String, Integer> kindCounts,
            int atomIndex) {
        String kind = atoms.get(atomIndex).kind;
        Integer cnt = kindCounts.get(kind);
        if (cnt != null && cnt > 1) {
            return kind + "_" + atomIndex;
        }
        return kind;
    }

    private static String rowIdAliasForAtom(List<Atom> atoms, Map<String, Integer> kindCounts,
            int atomIndex) {
        // Must be unique for Hibernate's auto-discovery of native SQL aliases.
        return rowIdKeyForAtom(atoms, kindCounts, atomIndex) + "_ROW_ID";
    }

    private static String extractWhereClause(String input) {
        if (input == null) {
            return null;
        }
        String s = input.trim();
        String lower = s.toLowerCase();
        int idx = lower.lastIndexOf(" where ");
        if (idx >= 0) {
            return s.substring(idx + " where ".length()).trim();
        }
        return s;
    }

    private static String normalizeKind(String kindOrOp) {
        if (kindOrOp == null) {
            return null;
        }
        String s = kindOrOp.trim();
        if (s.isEmpty()) {
            return null;
        }
        String u = s.toUpperCase();
        if ("FD".equals(u))
            return ResultKind.FD_LIST.name();
        if ("IND".equals(u))
            return ResultKind.IND_LIST.name();
        if ("UCC".equals(u))
            return ResultKind.UCC_LIST.name();
        if (u.endsWith("_LIST"))
            return u;
        return u;
    }

    private static final class Atom {
        final String kind;
        final List<String> args;

        Atom(String kind, List<String> args) {
            this.kind = kind;
            this.args = args;
        }
    }

    private static List<Atom> parseAndOnlyAtoms(String whereClause) {
        String[] parts = whereClause.split("(?i)\\s+AND\\s+");
        List<Atom> out = new ArrayList<>();
        for (String p : parts) {
            if (p == null)
                continue;
            String s = p.trim();
            if (s.isEmpty())
                continue;
            Atom a = parseAtom(s);
            if (a != null) {
                out.add(a);
            }
        }
        return out;
    }

    private static Atom parseAtom(String atom) {
        String s = atom.trim();
        // allow surrounding parentheses
        while (s.startsWith("(") && s.endsWith(")")) {
            s = s.substring(1, s.length() - 1).trim();
        }
        int lp = s.indexOf('(');
        int rp = s.lastIndexOf(')');
        if (lp < 0 || rp < lp) {
            return null;
        }
        String op = s.substring(0, lp).trim();
        String argsPart = s.substring(lp + 1, rp).trim();
        String kind = normalizeKind(op);
        if (kind == null) {
            return null;
        }
        if (!ResultKind.FD_LIST.name().equals(kind) && !ResultKind.IND_LIST.name().equals(kind)
                && !ResultKind.UCC_LIST.name().equals(kind)) {
            return null;
        }
        List<String> args = new ArrayList<>();
        if (!argsPart.isEmpty()) {
            for (String raw : argsPart.split(",")) {
                String v = raw.trim();
                if (v.isEmpty())
                    continue;
                // very small safety: allow only identifiers (variables map to COL_NAME)
                if (!v.matches("[A-Za-z_][A-Za-z0-9_]*")) {
                    throw new IllegalArgumentException("Unsupported variable name: " + v);
                }
                args.add(v);
            }
        }
        if (args.isEmpty()) {
            throw new IllegalArgumentException("Atom has no arguments: " + atom);
        }
        return new Atom(kind, args);
    }

    private Integer resolveTableIdForAtom(List<DpqlNormalizedTable> tables, Atom atom) {
        if (tables == null || tables.isEmpty() || atom == null) {
            return null;
        }
        List<DpqlNormalizedTable> sameKind = new ArrayList<>();
        boolean anySourceId = false;
        for (DpqlNormalizedTable t : tables) {
            if (t == null || t.getKind() == null) {
                continue;
            }
            if (atom.kind.equals(t.getKind())) {
                sameKind.add(t);
                if (t.getSourceTableId() != null) {
                    anySourceId = true;
                }
            }
        }
        if (sameKind.isEmpty()) {
            return null;
        }

        if (!anySourceId) {
            if (sameKind.size() == 1) {
                return null;
            }
            throw new IllegalArgumentException("Multiple normalized tables for kind " + atom.kind
                    + " but no table ids are available");
        }

        for (DpqlNormalizedTable t : sameKind) {
            if (t.getSourceTableId() == null) {
                continue;
            }
            List<String> cols = parseColumnsJson(t.getColumnsJson());
            if (cols.equals(atom.args)) {
                return t.getSourceTableId();
            }
        }
        if (sameKind.size() == 1 && sameKind.get(0).getSourceTableId() != null) {
            return sameKind.get(0).getSourceTableId();
        }
        throw new IllegalArgumentException(
                "No normalized table matches atom " + atomToString(atom));
    }

    private List<String> parseColumnsJson(String columnsJson) {
        if (columnsJson == null || columnsJson.trim().isEmpty()) {
            return List.of();
        }
        try {
            @SuppressWarnings("unchecked")
            List<String> parsed = mapper.readValue(columnsJson, List.class);
            return parsed == null ? List.of() : parsed;
        } catch (Exception ignored) {
            return List.of();
        }
    }

    private static String atomToString(Atom atom) {
        if (atom == null) {
            return "";
        }
        return atom.kind + "(" + String.join(",", atom.args) + ")";
    }

    private static String buildAtomSubquerySql(ParamBuilder pb, String executionId, Atom atom,
            int atomIndex, Integer tableId, List<Integer> anchorRowIds) {
        String base = "c" + atomIndex + "_0";

        StringBuilder sb = new StringBuilder();
        sb.append("(SELECT ");
        sb.append(base).append(".ROW_ID AS ROW_ID");

        // Select values for each argument.
        for (int j = 0; j < atom.args.size(); j++) {
            String alias = "c" + atomIndex + "_" + j;
            String v = atom.args.get(j);
            sb.append(", ").append(alias).append(".VALUE AS \"").append(v).append("\"");
        }

        sb.append(" FROM DPQLNORMCELL ").append(base);

        for (int j = 1; j < atom.args.size(); j++) {
            String alias = "c" + atomIndex + "_" + j;
            String colName = atom.args.get(j);
            sb.append(" JOIN DPQLNORMCELL ").append(alias);
            sb.append(" ON ");
            sb.append(alias).append(".EXECUTION_ID = ").append(base).append(".EXECUTION_ID");
            if (tableId != null) {
                sb.append(" AND ").append(alias).append(".TABLE_ID = ").append(base)
                        .append(".TABLE_ID");
            } else {
                sb.append(" AND ").append(alias).append(".KIND = ").append(base).append(".KIND");
            }
            sb.append(" AND ").append(alias).append(".ROW_ID = ").append(base).append(".ROW_ID");
            sb.append(" AND ").append(alias).append(".COL_NAME = ").append(pb.param(colName));
        }

        sb.append(" WHERE ");
        sb.append(base).append(".EXECUTION_ID = ").append(pb.param(executionId));
        if (tableId != null) {
            sb.append(" AND ").append(base).append(".TABLE_ID = ").append(pb.param(tableId));
        } else {
            sb.append(" AND ").append(base).append(".KIND = ").append(pb.param(atom.kind));
        }
        sb.append(" AND ").append(base).append(".COL_NAME = ").append(pb.param(atom.args.get(0)));
        if (anchorRowIds != null && !anchorRowIds.isEmpty()) {
            if (anchorRowIds.size() == 1) {
                sb.append(" AND ").append(base).append(".ROW_ID = ")
                        .append(pb.param(anchorRowIds.get(0)));
            } else {
                sb.append(" AND ").append(base).append(".ROW_ID IN (")
                        .append(pb.paramList(anchorRowIds)).append(")");
            }
        }

        sb.append(")");
        return sb.toString();
    }

    private static final class ParamBuilder {
        private final LinkedHashMap<String, Object> params = new LinkedHashMap<>();
        private int i = 0;

        String param(Object value) {
            String name = "p" + (i++);
            params.put(name, value);
            return ":" + name;
        }

        /** Returns comma-separated named params for IN clause, e.g. ":p5, :p6, :p7" */
        String paramList(List<?> values) {
            if (values == null || values.isEmpty()) {
                throw new IllegalArgumentException("paramList requires non-empty list");
            }
            StringBuilder sb = new StringBuilder();
            for (int j = 0; j < values.size(); j++) {
                if (j > 0)
                    sb.append(", ");
                String name = "p" + (i++);
                params.put(name, values.get(j));
                sb.append(":").append(name);
            }
            return sb.toString();
        }

        void bindAll(NativeQuery<?> q) {
            for (Map.Entry<String, Object> e : params.entrySet()) {
                q.setParameter(e.getKey(), e.getValue());
            }
        }
    }

    public Map<String, Object> getRun(String executionId) {
        if (executionId == null || executionId.trim().isEmpty()) {
            return null;
        }
        final DpqlExecution e;
        try {
            e = (DpqlExecution) HibernateUtil.retrieve(DpqlExecution.class, executionId);
        } catch (EntityStorageException ex) {
            throw new IllegalArgumentException(
                    "Failed to load DPQL run from database: " + ex.getMessage(), ex);
        }
        if (e == null) {
            return null;
        }

        List<Map<String, Object>> tables = new ArrayList<>();
        Session session = null;
        try {
            session = HibernateUtil.openNewSession();
            if (e.getNormalizedTables() != null) {
                for (DpqlNormalizedTable t : e.getNormalizedTables()) {
                    tables.add(normalizedTableMeta(session, executionId, t));
                }
            }
        } finally {
            if (session != null) {
                try {
                    session.close();
                } catch (Exception ignored) {
                    // ignore
                }
            }
        }

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("executionId", e.getId());
        out.put("createdAt", e.getCreatedAt() != null ? e.getCreatedAt().getTime() : null);
        out.put("query", e.getQuery());
        out.put("engineId", e.getEngineId());
        out.put("engineFileName", e.getEngineFileName());
        out.put("normalizedOnly", e.isNormalizedOnly());
        out.put("normalizedTables", tables);
        return out;
    }

    private Map<String, Object> normalizedTableMeta(Session session, String executionId,
            DpqlNormalizedTable t) {
        String kind = t == null ? null : t.getKind();
        String name = t == null ? null : t.getName();
        long tableId = t == null ? 0L : t.getId();
        Integer sourceTableId = t == null ? null : t.getSourceTableId();

        List<String> cols = List.of();
        try {
            @SuppressWarnings("unchecked")
            List<String> parsed = t != null && t.getColumnsJson() != null
                    ? mapper.readValue(t.getColumnsJson(), List.class)
                    : null;
            cols = parsed == null ? List.of() : parsed;
        } catch (Exception ignored) {
            cols = List.of();
        }

        int rowCount = getRowCountBestEffort(session, executionId, kind, sourceTableId,
                t != null ? t.getRowsJson() : null);

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("tableId", tableId);
        out.put("kind", kind);
        out.put("name", name);
        out.put("columns", cols);
        out.put("rowCount", rowCount);
        return out;
    }

    private int getRowCountBestEffort(Session session, String executionId, String kind,
            Integer tableId, String rowsJson) {
        Integer fromCells = getRowCountFromCells(session, executionId, kind, tableId);
        if (fromCells != null) {
            return fromCells;
        }
        // Fallback: decode JSON only if it is small enough to not risk OOM.
        if (rowsJson != null && rowsJson.length() <= 500_000) {
            try {
                @SuppressWarnings("unchecked")
                List<List<String>> rows = mapper.readValue(rowsJson, List.class);
                return rows == null ? 0 : rows.size();
            } catch (Exception ignored) {
                // ignore
            }
        }
        return 0;
    }

    private Integer getRowCountFromCells(Session session, String executionId, String kind,
            Integer tableId) {
        if (session == null || executionId == null || executionId.trim().isEmpty()) {
            return null;
        }
        if (tableId == null && (kind == null || kind.trim().isEmpty())) {
            return null;
        }
        try {
            StringBuilder sql = new StringBuilder();
            sql.append("SELECT MAX(ROW_ID) FROM DPQLNORMCELL WHERE EXECUTION_ID = :id");
            if (tableId != null) {
                sql.append(" AND TABLE_ID = :tableId");
            } else {
                sql.append(" AND KIND = :kind");
            }
            NativeQuery<?> q = session.createNativeQuery(sql.toString());
            q.setParameter("id", executionId);
            if (tableId != null) {
                q.setParameter("tableId", tableId);
            } else {
                q.setParameter("kind", kind);
            }
            Object v = q.uniqueResult();
            if (v == null) {
                return 0;
            }
            int max = ((Number) v).intValue();
            return Math.max(0, max + 1);
        } catch (Exception ignored) {
            return null;
        }
    }

    public DpqlNormalizedTablePageResponseDto getNormalizedTablePage(String executionId,
            long tableId, int offset, int limit, String search) {
        if (executionId == null || executionId.trim().isEmpty()) {
            throw new IllegalArgumentException("executionId must not be empty");
        }
        if (tableId <= 0) {
            throw new IllegalArgumentException("tableId must be positive");
        }

        int safeOffset = Math.max(0, offset);
        int safeLimit = Math.max(1, Math.min(1000, limit));
        String q = (search == null || search.trim().isEmpty()) ? null : search.trim();

        final DpqlExecution e;
        try {
            e = (DpqlExecution) HibernateUtil.retrieve(DpqlExecution.class, executionId);
        } catch (EntityStorageException ex) {
            throw new IllegalArgumentException(
                    "Failed to load DPQL run from database: " + ex.getMessage(), ex);
        }
        if (e == null) {
            return null;
        }

        DpqlNormalizedTable table = null;
        if (e.getNormalizedTables() != null) {
            for (DpqlNormalizedTable t : e.getNormalizedTables()) {
                if (t != null && t.getId() == tableId) {
                    table = t;
                    break;
                }
            }
        }
        if (table == null) {
            return null;
        }

        List<String> columns = List.of();
        try {
            @SuppressWarnings("unchecked")
            List<String> parsed = table.getColumnsJson() == null ? null
                    : mapper.readValue(table.getColumnsJson(), List.class);
            columns = parsed == null ? List.of() : parsed;
        } catch (Exception ignored) {
            columns = List.of();
        }

        DpqlNormalizedTablePageResponseDto out = new DpqlNormalizedTablePageResponseDto();
        out.setExecutionId(executionId);
        out.setTableId(tableId);
        out.setKind(table.getKind());
        out.setName(table.getName());
        out.setColumns(columns);
        out.setOffset(safeOffset);
        out.setLimit(safeLimit);
        out.setSearch(q);

        // Prefer reading from DPQLNORMCELL (fast + scalable).
        Session session = null;
        try {
            session = HibernateUtil.openNewSession();

            Integer totalRows = null;
            List<Integer> rowIds = fetchRowIdsPage(session, executionId, table.getKind(),
                    table.getSourceTableId(), q, safeOffset, safeLimit);
            if (rowIds != null) {
                totalRows = countDistinctRowIds(session, executionId, table.getKind(),
                        table.getSourceTableId(), q);
                out.setTotalRows(totalRows == null ? 0 : totalRows);
                out.setRowIds(rowIds);
                out.setRows(fetchRowsForRowIds(session, executionId, table.getKind(),
                        table.getSourceTableId(), rowIds, columns));
                return out;
            }
        } finally {
            if (session != null) {
                try {
                    session.close();
                } catch (Exception ignored) {
                    // ignore
                }
            }
        }

        // Fallback: decode stored rows JSON (only if small enough).
        String rowsJson = table.getRowsJson();
        if (rowsJson == null) {
            out.setTotalRows(0);
            out.setRowIds(List.of());
            out.setRows(List.of());
            return out;
        }
        if (rowsJson.length() > 1_000_000) {
            throw new IllegalArgumentException(
                    "Stored normalized table is too large to page from JSON; DPQLNORMCELL is required");
        }

        List<List<String>> allRows;
        try {
            @SuppressWarnings("unchecked")
            List<List<String>> parsed = mapper.readValue(rowsJson, List.class);
            allRows = parsed == null ? List.of() : parsed;
        } catch (Exception ex) {
            throw new IllegalArgumentException("Failed to decode stored table rows JSON");
        }

        List<Integer> ids = new ArrayList<>();
        List<List<String>> filtered = new ArrayList<>();
        for (int i = 0; i < allRows.size(); i++) {
            List<String> r = allRows.get(i);
            if (matchesSearch(r, q)) {
                ids.add(i);
                filtered.add(r);
            }
        }
        out.setTotalRows(filtered.size());

        int from = Math.min(safeOffset, filtered.size());
        int to = Math.min(from + safeLimit, filtered.size());
        out.setRowIds(ids.subList(from, to));
        out.setRows(filtered.subList(from, to));
        return out;
    }

    private static boolean matchesSearch(List<String> row, String q) {
        if (q == null || q.isEmpty()) {
            return true;
        }
        if (row == null) {
            return false;
        }
        String needle = q.toLowerCase();
        for (String cell : row) {
            if (cell != null && cell.toLowerCase().contains(needle)) {
                return true;
            }
        }
        return false;
    }

    private List<Integer> fetchRowIdsPage(Session session, String executionId, String kind,
            Integer tableId, String search, int offset, int limit) {
        if (session == null) {
            return null;
        }
        if (tableId == null && (kind == null || kind.trim().isEmpty())) {
            return null;
        }
        try {
            StringBuilder sql = new StringBuilder();
            sql.append("SELECT DISTINCT ROW_ID FROM DPQLNORMCELL WHERE EXECUTION_ID = :id");
            if (tableId != null) {
                sql.append(" AND TABLE_ID = :tableId");
            } else {
                sql.append(" AND KIND = :kind");
            }
            if (search != null && !search.isEmpty()) {
                sql.append(" AND LOWER(VALUE) LIKE :q");
            }
            sql.append(" ORDER BY ROW_ID OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY");
            NativeQuery<?> q = session.createNativeQuery(sql.toString());
            q.setParameter("id", executionId);
            if (tableId != null) {
                q.setParameter("tableId", tableId);
            } else {
                q.setParameter("kind", kind);
            }
            if (search != null && !search.isEmpty()) {
                q.setParameter("q", "%" + search.toLowerCase() + "%");
            }
            q.setParameter("offset", offset);
            q.setParameter("limit", limit);

            @SuppressWarnings("unchecked")
            List<Object> rows = (List<Object>) q.list();
            List<Integer> out = new ArrayList<>(rows == null ? 0 : rows.size());
            if (rows != null) {
                for (Object v : rows) {
                    if (v == null)
                        continue;
                    out.add(((Number) v).intValue());
                }
            }
            return out;
        } catch (Exception ex) {
            // If the shadow table doesn't exist or SQL dialect can't handle OFFSET/FETCH, treat as
            // missing.
            return null;
        }
    }

    private Integer countDistinctRowIds(Session session, String executionId, String kind,
            Integer tableId, String search) {
        if (session == null) {
            return null;
        }
        if (tableId == null && (kind == null || kind.trim().isEmpty())) {
            return null;
        }
        try {
            StringBuilder sql = new StringBuilder();
            sql.append("SELECT COUNT(DISTINCT ROW_ID) FROM DPQLNORMCELL WHERE EXECUTION_ID = :id");
            if (tableId != null) {
                sql.append(" AND TABLE_ID = :tableId");
            } else {
                sql.append(" AND KIND = :kind");
            }
            if (search != null && !search.isEmpty()) {
                sql.append(" AND LOWER(VALUE) LIKE :q");
            }
            NativeQuery<?> q = session.createNativeQuery(sql.toString());
            q.setParameter("id", executionId);
            if (tableId != null) {
                q.setParameter("tableId", tableId);
            } else {
                q.setParameter("kind", kind);
            }
            if (search != null && !search.isEmpty()) {
                q.setParameter("q", "%" + search.toLowerCase() + "%");
            }
            Object v = q.uniqueResult();
            return v == null ? 0 : ((Number) v).intValue();
        } catch (Exception ex) {
            return null;
        }
    }

    private List<List<String>> fetchRowsForRowIds(Session session, String executionId, String kind,
            Integer tableId, List<Integer> rowIds, List<String> columns) {
        if (rowIds == null || rowIds.isEmpty()) {
            return List.of();
        }
        List<String> cols = (columns == null) ? List.of() : columns;

        StringBuilder sql = new StringBuilder();
        sql.append("SELECT ROW_ID, COL_NAME, VALUE FROM DPQLNORMCELL WHERE EXECUTION_ID = :id");
        if (tableId != null) {
            sql.append(" AND TABLE_ID = :tableId");
        } else {
            sql.append(" AND KIND = :kind");
        }
        sql.append(" AND ROW_ID IN (:rowIds)");
        if (!cols.isEmpty()) {
            sql.append(" AND COL_NAME IN (:cols)");
        }
        sql.append(" ORDER BY ROW_ID");

        NativeQuery<?> q = session.createNativeQuery(sql.toString());
        q.setParameter("id", executionId);
        if (tableId != null) {
            q.setParameter("tableId", tableId);
        } else {
            q.setParameter("kind", kind);
        }
        q.setParameterList("rowIds", rowIds);
        if (!cols.isEmpty()) {
            q.setParameterList("cols", cols);
        }

        @SuppressWarnings("unchecked")
        List<Object[]> items = (List<Object[]>) q.list();

        Map<Integer, Map<String, String>> byRow = new LinkedHashMap<>();
        for (Integer rid : rowIds) {
            byRow.put(rid, new LinkedHashMap<>());
        }
        if (items != null) {
            for (Object[] r : items) {
                if (r == null || r.length < 3)
                    continue;
                Integer rid = r[0] == null ? null : ((Number) r[0]).intValue();
                String col = r[1] == null ? null : String.valueOf(r[1]);
                String val = r[2] == null ? null : String.valueOf(r[2]);
                if (rid == null || col == null)
                    continue;
                Map<String, String> m = byRow.get(rid);
                if (m != null) {
                    m.put(col, val);
                }
            }
        }

        List<List<String>> rows = new ArrayList<>(rowIds.size());
        for (Integer rid : rowIds) {
            Map<String, String> m = byRow.get(rid);
            if (cols.isEmpty()) {
                rows.add(Collections.emptyList());
                continue;
            }
            List<String> row = new ArrayList<>(cols.size());
            for (String c : cols) {
                row.add(m == null ? null : m.get(c));
            }
            rows.add(row);
        }
        return rows;
    }

    public Map<String, Object> getResultOverview(String executionId) throws IOException {
        ResultReader reader = new ResultReader(RESULTS_DIR, executionId);
        if (!reader.exists()) {
            return null;
        }
        return reader.readOverview();
    }

    public DpqlRunStatusDto getRunStatus(String executionId) {
        if (executionId == null || executionId.trim().isEmpty()) {
            return null;
        }
        DpqlRunRegistry.Entry e = DpqlRunRegistry.get(executionId);
        if (e != null) {
            return e.toDto();
        }
        try {
            Object existing = HibernateUtil
                    .retrieve(de.metanome.backend.results_db.DpqlExecution.class, executionId);
            if (existing != null) {
                DpqlRunStatusDto dto = new DpqlRunStatusDto();
                dto.setExecutionId(executionId);
                dto.setStatus(DpqlRunRegistry.Status.FINISHED.name());
                dto.setMessage("Finished");
                return dto;
            }
        } catch (Exception ignored) {
            // ignore
        }
        return null;
    }

    public boolean cancelRun(String executionId) {
        return DpqlRunRegistry.cancel(executionId);
    }

    public Map<String, Object> getTablePage(String executionId, int tableId, int offset, int limit,
            String search) throws IOException {
        ResultReader reader = new ResultReader(RESULTS_DIR, executionId);
        if (!reader.exists()) {
            return null;
        }
        return reader.readTablePage(tableId, offset, limit, search);
    }

    private void persistDpqlExecutionFromDisk(String executionId, DpqlQuerryRequest request,
            CancellationToken cancel) throws IOException, EngineException {
        if (executionId == null || executionId.trim().isEmpty() || request == null) {
            return;
        }

        File in = new File(RESULTS_DIR, executionId + ".ndjson");
        if (!in.exists() || !in.isFile()) {
            throw new IllegalArgumentException(
                    "DPQL result file not found for executionId=" + executionId);
        }

        // Replace any existing record for the same executionId (best-effort)
        try {
            DpqlExecution existing =
                    (DpqlExecution) HibernateUtil.retrieve(DpqlExecution.class, executionId);
            if (existing != null) {
                HibernateUtil.delete(existing);
            }
        } catch (Exception ignored) {
            // best-effort
        }

        // Pass 1: collect normalized table headers (FD/IND/UCC) without loading rows.
        LinkedHashMap<Integer, TableHeader> headersByTableId = new LinkedHashMap<>();
        try (BufferedReader br = new BufferedReader(new FileReader(in))) {
            String line;
            int lines = 0;
            while ((line = br.readLine()) != null) {
                if ((++lines % 500) == 0) {
                    throwIfCanceled(cancel);
                }
                if (line.isEmpty())
                    continue;
                JsonNode node = mapper.readTree(line);
                JsonNode typeNode = node.get("type");
                if (typeNode == null)
                    continue;
                String type = typeNode.asText();
                if (!"table-start".equals(type))
                    continue;

                int tableId = node.hasNonNull("tableId") ? node.get("tableId").asInt() : -1;
                if (tableId <= 0)
                    continue;
                String kind = node.hasNonNull("kind") ? node.get("kind").asText() : null;
                if (kind == null)
                    continue;
                if (!ResultKind.FD_LIST.name().equals(kind)
                        && !ResultKind.IND_LIST.name().equals(kind)
                        && !ResultKind.UCC_LIST.name().equals(kind)) {
                    continue;
                }

                if (headersByTableId.containsKey(tableId)) {
                    continue;
                }

                String name = node.hasNonNull("name") ? node.get("name").asText() : null;
                List<String> cols;
                try {
                    cols = node.has("columns")
                            ? mapper.convertValue(node.get("columns"),
                                    new TypeReference<List<String>>() {})
                            : List.of();
                } catch (Exception ex) {
                    cols = List.of();
                }

                headersByTableId.put(tableId,
                        new TableHeader(tableId, kind, name, cols == null ? List.of() : cols));
            }
        }

        DpqlExecution e = new DpqlExecution(executionId);
        e.setCreatedAt(new Date());
        e.setQuery(request.getQuery());
        e.setEngineId(request.getEngineId());
        e.setEngineFileName(request.getEngineFileName());
        e.setNormalizedOnly(true);

        for (TableHeader h : headersByTableId.values()) {
            DpqlNormalizedTable nt = new DpqlNormalizedTable();
            nt.setKind(h.kind);
            nt.setName(h.name);
            nt.setSourceTableId(h.tableId);
            try {
                nt.setColumnsJson(
                        mapper.writeValueAsString(h.columns == null ? List.of() : h.columns));
            } catch (Exception ex) {
                nt.setColumnsJson("[]");
            }
            // Avoid storing full rows JSON to prevent duplication and OOM; paging uses
            // DPQLNORMCELL.
            nt.setRowsJson(null);
            e.addNormalizedTable(nt);
        }

        try {
            throwIfCanceled(cancel);
            HibernateUtil.store(e);
        } catch (EntityStorageException ex) {
            throw new IllegalArgumentException(
                    "Failed to store DPQL run in database: " + ex.getMessage(), ex);
        }

        // Pass 2: stream rows into DPQLNORMCELL in batches.
        persistDpqlNormCellsFromDisk(executionId, in, headersByTableId, cancel);
    }

    private void persistDpqlNormCellsFromDisk(String executionId, File in,
            Map<Integer, TableHeader> headersByTableId, CancellationToken cancel)
            throws IOException, EngineException {
        if (headersByTableId == null || headersByTableId.isEmpty()) {
            return;
        }

        Session session = null;
        try {
            session = HibernateUtil.openNewSession();
            session.beginTransaction();

            NativeQuery<?> delete =
                    session.createNativeQuery("DELETE FROM DPQLNORMCELL WHERE EXECUTION_ID = :id");
            delete.setParameter("id", executionId);
            delete.executeUpdate();

            Integer currentTableId = null;
            String currentKind = null;
            List<String> currentCols = null;
            int currentRowId = 0;
            int batch = 0;
            int lines = 0;

            try (BufferedReader br = new BufferedReader(new FileReader(in))) {
                String line;
                while ((line = br.readLine()) != null) {
                    if ((++lines % 250) == 0) {
                        throwIfCanceled(cancel);
                    }
                    if (line.isEmpty())
                        continue;
                    JsonNode node = mapper.readTree(line);
                    JsonNode typeNode = node.get("type");
                    if (typeNode == null)
                        continue;
                    String type = typeNode.asText();

                    if ("table-start".equals(type)) {
                        int tableId = node.hasNonNull("tableId") ? node.get("tableId").asInt() : -1;
                        TableHeader h = tableId > 0 ? headersByTableId.get(tableId) : null;
                        if (h != null) {
                            currentTableId = h.tableId;
                            currentKind = h.kind;
                            currentCols = h.columns;
                            currentRowId = 0;
                        } else {
                            currentTableId = null;
                            currentKind = null;
                            currentCols = null;
                        }
                        continue;
                    }

                    if ("table-end".equals(type)) {
                        currentTableId = null;
                        currentKind = null;
                        currentCols = null;
                        continue;
                    }

                    if (!"row".equals(type)) {
                        continue;
                    }

                    if (currentTableId == null || currentKind == null || currentCols == null
                            || currentCols.isEmpty()) {
                        // Without columns, DPQLNORMCELL isn't useful; skip.
                        continue;
                    }

                    List<String> row;
                    try {
                        row = mapper.convertValue(node.get("data"),
                                new TypeReference<List<String>>() {});
                    } catch (Exception ex) {
                        row = null;
                    }
                    int max = Math.min(currentCols.size(), row == null ? 0 : row.size());
                    for (int c = 0; c < max; c++) {
                        if ((batch % 500) == 0) {
                            throwIfCanceled(cancel);
                        }
                        String colName = currentCols.get(c);
                        if (colName == null || colName.trim().isEmpty()) {
                            continue;
                        }
                        String val = row.get(c);
                        session.save(new DpqlNormCell(executionId, currentTableId, currentKind,
                                currentRowId, colName.trim(), val));
                        batch++;
                        if (batch % 500 == 0) {
                            session.flush();
                            session.clear();
                        }
                    }
                    currentRowId++;
                }
            }

            session.getTransaction().commit();
        } catch (Exception ex) {
            if (session != null) {
                try {
                    if (session.getTransaction() != null && session.getTransaction().isActive()) {
                        session.getTransaction().rollback();
                    }
                } catch (Exception ignoredRollback) {
                    // ignore
                }
            }
            // Best-effort: normalized paging depends on DPQLNORMCELL, so surface error.
            throw new IllegalArgumentException(
                    "Failed to persist DPQL normalized cells: " + ex.getMessage(), ex);
        } finally {
            if (session != null) {
                try {
                    session.close();
                } catch (Exception ignored) {
                    // ignore
                }
            }
        }
    }

    private static final class TableHeader {
        final int tableId;
        final String kind;
        final String name;
        final List<String> columns;

        TableHeader(int tableId, String kind, String name, List<String> columns) {
            this.tableId = tableId;
            this.kind = kind;
            this.name = name;
            this.columns = columns;
        }
    }

    private void persistDpqlExecution(String executionId, DpqlQuerryRequest request,
            EngineResult maybeNormalizedResult) {
        if (executionId == null || executionId.trim().isEmpty() || request == null) {
            return;
        }

        try {
            // Replace any existing record for the same executionId (best-effort)
            DpqlExecution existing =
                    (DpqlExecution) HibernateUtil.retrieve(DpqlExecution.class, executionId);
            if (existing != null) {
                HibernateUtil.delete(existing);
            }
        } catch (Exception ignored) {
            // best-effort
        }

        DpqlExecution e = new DpqlExecution(executionId);
        e.setCreatedAt(new Date());
        e.setQuery(request.getQuery());
        e.setEngineId(request.getEngineId());
        e.setEngineFileName(request.getEngineFileName());
        e.setNormalizedOnly(Boolean.TRUE.equals(request.getNormalizedOnly()));

        List<DpqlNormCell> normCells = new ArrayList<>();

        if (Boolean.TRUE.equals(request.getNormalizedOnly()) && maybeNormalizedResult != null) {
            List<EngineTable> tables = maybeNormalizedResult.getTables();
            if (tables != null) {
                int tableIndex = 0;
                for (EngineTable t : tables) {
                    if (t == null || t.getKind() == null) {
                        continue;
                    }
                    ResultKind k = t.getKind();
                    if (k != ResultKind.FD_LIST && k != ResultKind.IND_LIST
                            && k != ResultKind.UCC_LIST) {
                        continue;
                    }
                    DpqlNormalizedTable nt = new DpqlNormalizedTable();
                    nt.setKind(k.name());
                    nt.setName(t.getName());
                    nt.setSourceTableId(++tableIndex);

                    // Build relational shadow cells from the in-memory table to avoid JSON parsing.
                    List<String> cols = t.getColumns() == null ? List.of() : t.getColumns();
                    List<List<String>> rows = t.getRows() == null ? List.of() : t.getRows();
                    for (int r = 0; r < rows.size(); r++) {
                        List<String> row = rows.get(r);
                        if (row == null) {
                            continue;
                        }
                        int max = Math.min(cols.size(), row.size());
                        for (int c = 0; c < max; c++) {
                            String colName = cols.get(c);
                            if (colName == null || colName.trim().isEmpty()) {
                                continue;
                            }
                            normCells.add(new DpqlNormCell(executionId, nt.getSourceTableId(),
                                    k.name(), r, colName.trim(), row.get(c)));
                        }
                    }

                    try {
                        nt.setColumnsJson(mapper.writeValueAsString(
                                t.getColumns() == null ? List.of() : t.getColumns()));
                        nt.setRowsJson(mapper
                                .writeValueAsString(t.getRows() == null ? List.of() : t.getRows()));
                    } catch (Exception ex) {
                        nt.setColumnsJson("[]");
                        nt.setRowsJson("[]");
                    }
                    e.addNormalizedTable(nt);
                }
            }
        }

        try {
            HibernateUtil.store(e);

            // Persist shadow cells best-effort (used for fast SQL join expansion).
            if (!normCells.isEmpty()) {
                persistDpqlNormCells(executionId, normCells);
            }
        } catch (EntityStorageException ex) {
            throw new IllegalArgumentException(
                    "Failed to store DPQL run in database: " + ex.getMessage(), ex);
        }
    }

    private void persistDpqlNormCells(String executionId, List<DpqlNormCell> cells) {
        Session session = null;
        try {
            session = HibernateUtil.openNewSession();
            session.beginTransaction();

            NativeQuery<?> delete =
                    session.createNativeQuery("DELETE FROM DPQLNORMCELL WHERE EXECUTION_ID = :id");
            delete.setParameter("id", executionId);
            delete.executeUpdate();

            int batch = 0;
            for (DpqlNormCell cell : cells) {
                session.save(cell);
                batch++;
                if (batch % 500 == 0) {
                    session.flush();
                    session.clear();
                }
            }

            session.getTransaction().commit();
        } catch (Exception ignored) {
            if (session != null) {
                try {
                    if (session.getTransaction() != null && session.getTransaction().isActive()) {
                        session.getTransaction().rollback();
                    }
                } catch (Exception ignoredRollback) {
                    // ignore
                }
            }
        } finally {
            if (session != null) {
                try {
                    session.close();
                } catch (Exception ignoredClose) {
                    // ignore
                }
            }
        }
    }

    private void prepareDatasetsFromDb(String query, EngineExecutionContext ctx) {
        if (ctx == null) {
            return;
        }

        String basePath = ctx.getBasePath();
        if (basePath == null || basePath.trim().isEmpty()) {
            basePath = System.getProperty("user.dir");
        }

        String dataset = ctx.getDataset();
        if (dataset == null || dataset.trim().isEmpty()) {
            dataset = "TPCHNEW";
        }

        Path datasetDir = Path.of(basePath).resolve("io").resolve("data").resolve(dataset);

        try {
            @SuppressWarnings("unchecked")
            List<FileInput> allInputs =
                    (List<FileInput>) (List<?>) HibernateUtil.queryCriteria(FileInput.class);
            DpqlDatasetPreparer.RequiredFiles required =
                    DpqlDatasetPreparer.parseRequiredFilesFromQuery(query);
            // If there are no CC(...) scopes, do nothing.
            if (!required.all && (required.names == null || required.names.isEmpty())) {
                return;
            }
            DpqlDatasetPreparer.copyRequiredFiles(allInputs, required, datasetDir);
        } catch (IllegalArgumentException e) {
            // Make missing inputs visible to the client as 400 via existing
            // IllegalArgumentException mapping.
            throw e;
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Failed to prepare input dataset files: " + e.getMessage(), e);
        }
    }

    private void executeWithSelectedEngine(DpqlQuerryRequest request, EngineExecutionContext ctx,
            de.metanome.engine.api.result_receiver.EngineResultReceiver receiver)
            throws EngineException, EngineResultReceiverException {
        // 1) If request references a registered engine: load from its JAR
        if (request.getEngineId() != null) {
            try {
                System.out.println("[DPQL] Using registered engineId=" + request.getEngineId());
                Engine engineEntity =
                        (Engine) HibernateUtil.retrieve(Engine.class, request.getEngineId());
                if (engineEntity == null || engineEntity.getFileName() == null) {
                    throw new IllegalArgumentException(
                            "Engine id not found: " + request.getEngineId());
                }
                System.out.println("[DPQL] Resolved engineId=" + request.getEngineId() + " to jar='"
                        + engineEntity.getFileName() + "'");
                try (EngineJarLoader.LoadedEngine loaded =
                        engineJarLoader.loadEngine(engineEntity.getFileName())) {
                    System.out.println("[DPQL] Loaded engine implementation: "
                            + (loaded.getEngine() != null ? loaded.getEngine().getClass().getName()
                                    : "<null>")
                            + " name='"
                            + (loaded.getEngine() != null ? loaded.getEngine().getName() : "")
                            + "'");
                    validateRequiredEngineParameters(request, loaded.getEngine());
                    executeEngineStreaming(loaded.getEngine(), request.getQuery(), ctx, receiver);
                }
                return;
            } catch (IllegalArgumentException e) {
                throw e;
            } catch (EngineException | EngineResultReceiverException e) {
                throw e;
            } catch (Exception e) {
                throw new IllegalArgumentException(
                        "Could not load engine id " + request.getEngineId() + ": " + e.getMessage(),
                        e);
            }
        }

        // 2) If request references an engine JAR directly: load it
        if (request.getEngineFileName() != null && !request.getEngineFileName().trim().isEmpty()) {
            try (EngineJarLoader.LoadedEngine loaded =
                    engineJarLoader.loadEngine(request.getEngineFileName().trim())) {
                System.out.println("[DPQL] Using engineFileName='"
                        + request.getEngineFileName().trim() + "' -> "
                        + (loaded.getEngine() != null ? loaded.getEngine().getClass().getName()
                                : "<null>")
                        + " name='"
                        + (loaded.getEngine() != null ? loaded.getEngine().getName() : "") + "'");
                validateRequiredEngineParameters(request, loaded.getEngine());
                executeEngineStreaming(loaded.getEngine(), request.getQuery(), ctx, receiver);
            } catch (IOException e) {
                throw new IllegalArgumentException("Could not load engine jar '"
                        + request.getEngineFileName() + "': " + e.getMessage(), e);
            }
            return;
        }

        // 3) No default engine: enforce engine selection (UI no longer offers a default)
        throw new IllegalArgumentException("Engine must be selected (engineId or engineFileName)");
    }

    private static void executeEngineStreaming(ProfilingQueryEngine engine, String query,
            EngineExecutionContext ctx,
            de.metanome.engine.api.result_receiver.EngineResultReceiver receiver)
            throws EngineException, EngineResultReceiverException {
        if (engine == null) {
            throw new IllegalArgumentException("Engine is null");
        }
        if (receiver == null) {
            throw new IllegalArgumentException("EngineResultReceiver is null");
        }

        EngineResultMetadata md = EngineResultMetadata.from(engine.getName(), query, ctx);
        receiver.start(md);
        boolean finished = false;
        try {
            engine.execute(query, ctx, receiver);
            finished = true;
        } finally {
            // Always try to finish to release resources (e.g. file handles).
            try {
                receiver.finish();
            } catch (Exception ignored) {
                // ignore
            }
            if (!finished) {
                // If needed later: add receiver.abort() to distinguish failures.
            }
        }
    }

    private static void validateRequiredEngineParameters(DpqlQuerryRequest request,
            ProfilingQueryEngine engine) {
        if (request == null || engine == null) {
            return;
        }

        ArrayList<EngineParameterSpec> specs;
        try {
            specs = engine.getParameterSpecifications();
        } catch (Exception e) {
            // If an engine can't provide specs, do not block execution.
            return;
        }

        if (specs == null || specs.isEmpty()) {
            return;
        }

        List<String> missing = new ArrayList<>();
        for (EngineParameterSpec spec : specs) {
            if (spec == null || !spec.isRequired()) {
                continue;
            }

            String key = spec.getKey();
            if (key == null || key.trim().isEmpty()) {
                continue;
            }

            String value = getEffectiveRequestParamValue(request, key, spec);
            if (value == null) {
                missing.add(spec.getLabel() != null ? spec.getLabel() : key);
                continue;
            }

            if (spec.getType() == EngineParameterType.BOOLEAN) {
                // For booleans: presence is the key; "false" is valid.
                String v = value.trim().toLowerCase();
                if (!"true".equals(v) && !"false".equals(v)) {
                    missing.add(spec.getLabel() != null ? spec.getLabel() : key);
                }
                continue;
            }

            if (value.trim().isEmpty()) {
                missing.add(spec.getLabel() != null ? spec.getLabel() : key);
            }
        }

        if (!missing.isEmpty()) {
            throw new IllegalArgumentException(
                    "Missing required parameters: " + String.join(", ", missing));
        }
    }

    private static String getEffectiveRequestParamValue(DpqlQuerryRequest request, String key,
            EngineParameterSpec spec) {
        // "Everything is engine-specific": first look into engineParameters, then fall back to
        // legacy top-level fields.
        Map<String, String> extras = request.getEngineParameters();
        String raw = extras == null ? null : extras.get(key);
        if (raw != null && !raw.trim().isEmpty()) {
            return raw;
        }

        // Legacy/compat: accept the old top-level request fields if present.
        String legacy = null;
        if ("dataset".equals(key))
            legacy = request.getDataset();
        else if ("basePath".equals(key))
            legacy = request.getBasePath();
        else if ("separator".equals(key))
            legacy = request.getSeparator();
        else if ("quoteChar".equals(key))
            legacy = request.getQuoteChar();
        else if ("cached".equals(key))
            legacy = request.getCached() == null ? null : request.getCached().toString();

        if (legacy != null && !legacy.trim().isEmpty()) {
            return legacy;
        }

        // Only then consider engine-declared default.
        String def = spec == null ? null : spec.getDefaultValue();
        return (def == null || def.trim().isEmpty()) ? null : def;
    }

    private EngineExecutionContext createExecutionContext(DpqlQuerryRequest request) {
        EngineExecutionContext ctx = new EngineExecutionContext();

        // Cross-engine options
        ctx.setNormalizedOnly(request != null && Boolean.TRUE.equals(request.getNormalizedOnly()));

        Map<String, String> extras = request.getEngineParameters();
        // Fill standard context fields from engineParameters first (new behavior), then fall back
        // to legacy top-level fields.
        String dataset = (extras == null) ? null : extras.get("dataset");
        if (dataset == null || dataset.trim().isEmpty())
            dataset = request.getDataset();
        ctx.setDataset(withDefault(dataset, "TPCHNEW"));

        String basePath = (extras == null) ? null : extras.get("basePath");
        if (basePath == null || basePath.trim().isEmpty())
            basePath = request.getBasePath();
        ctx.setBasePath(withDefault(basePath, System.getProperty("user.dir")));

        String separator = (extras == null) ? null : extras.get("separator");
        if (separator == null || separator.trim().isEmpty())
            separator = request.getSeparator();
        ctx.setSeparator(withDefault(separator, ","));

        String quoteChar = (extras == null) ? null : extras.get("quoteChar");
        if (quoteChar == null || quoteChar.trim().isEmpty())
            quoteChar = request.getQuoteChar();
        ctx.setQuoteChar(withDefault(quoteChar, "\""));

        String cached = (extras == null) ? null : extras.get("cached");
        if (cached == null || cached.trim().isEmpty()) {
            ctx.setCached(Boolean.TRUE.equals(request.getCached()));
        } else {
            ctx.setCached("true".equalsIgnoreCase(cached.trim()));
        }

        ctx.setEngineParameters(extras);
        return ctx;
    }

    private static String withDefault(String candidate, String fallback) {
        return (candidate == null || candidate.isEmpty()) ? fallback : candidate;
    }
}
