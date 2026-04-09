package de.metanome.engines.testing.example_normalized_engine;

import de.metanome.engine.api.EngineException;
import de.metanome.engine.api.EngineExecutionContext;
import de.metanome.engine.api.EngineParameterSpec;
import de.metanome.engine.api.EngineParameterType;
import de.metanome.engine.api.EngineTable;
import de.metanome.engine.api.ProfilingQueryEngine;
import de.metanome.engine.api.ResultKind;
import de.metanome.engine.api.result_receiver.EngineResultReceiver;
import de.metanome.engine.api.result_receiver.EngineResultReceiverException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.LongFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Dummy engine used for testing large result sets and the normalized results pipeline.
 */
public class ExampleNormalizedQueryEngine implements ProfilingQueryEngine {

    private static final long DEFAULT_ROW_COUNT = 10_000_000L;
    private static final int BATCH_SIZE = 10_000;

    private static final int DEFAULT_JOIN_KEYS = 10;

    private static final String PARAM_BATCH_DELAY_MS = "batchDelayMs";
    private static final String PARAM_ROW_COUNT = "rowCount";
    private static final String PARAM_STRESS_ROWS = "stressRows";

    @Override
    public void execute(String query,
                        EngineExecutionContext context,
                        EngineResultReceiver receiver) throws EngineException, EngineResultReceiverException {
        System.out.println("[ExampleNormalizedQueryEngine] execute() called");
        System.out.println("[ExampleNormalizedQueryEngine] query=" + query);

        final int batchDelayMs = parseNonNegativeInt(context != null ? context.getEngineParameter(PARAM_BATCH_DELAY_MS) : null, 0);
        final long rowCount = resolveRequestedRowCount(context);

        QuerySpec spec = parseQuery(query);
        GenerationPlan plan = buildPlan(spec, rowCount);

        boolean normalizedOnly = context != null && context.isNormalizedOnly();
        if (normalizedOnly) {
            streamNormalized(spec, plan, context, receiver, batchDelayMs);
        } else {
            streamDenormalized(spec, plan, context, receiver, batchDelayMs);
        }
    }

    @Override
    public ArrayList<EngineParameterSpec> getParameterSpecifications() {
        ArrayList<EngineParameterSpec> out = new ArrayList<>();

        EngineParameterSpec delay = new EngineParameterSpec(PARAM_BATCH_DELAY_MS, "Batch delay (ms)", EngineParameterType.STRING);
        delay.setDefaultValue("0");
        delay.setRequired(false);
        delay.setPlaceholder("0");
        delay.setHelpText("Optional. Adds a sleep after each emitted batch to make cancellation/stop observable in the UI.");
        out.add(delay);

        EngineParameterSpec rowCount = new EngineParameterSpec(PARAM_ROW_COUNT, "Rows to produce", EngineParameterType.STRING);
        rowCount.setDefaultValue(Long.toString(DEFAULT_ROW_COUNT));
        rowCount.setRequired(false);
        rowCount.setPlaceholder(Long.toString(DEFAULT_ROW_COUNT));
        rowCount.setHelpText("Optional. Total number of synthetic rows to emit per non-UCC result table.");
        out.add(rowCount);

        return out;
    }

    @Override
    public String getName() {
        return "Dummy Normalized Engine (DPQL query-driven synthetic)";
    }

    private static void checkCanceled(EngineExecutionContext context) throws EngineException {
        if (Thread.currentThread().isInterrupted()) {
            throw new EngineException("Execution canceled");
        }
        if (context != null) {
            context.throwIfCanceled();
        }
    }

    private static void sleepAfterBatchIfConfigured(EngineExecutionContext context, int batchDelayMs) throws EngineException {
        if (batchDelayMs <= 0) {
            return;
        }
        checkCanceled(context);
        try {
            Thread.sleep(batchDelayMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new EngineException("Execution canceled");
        }
        checkCanceled(context);
    }

    private static int parseNonNegativeInt(String raw, int defaultValue) {
        if (raw == null) {
            return defaultValue;
        }
        String s = raw.trim();
        if (s.isEmpty()) {
            return defaultValue;
        }
        try {
            int v = Integer.parseInt(s);
            return Math.max(0, v);
        } catch (NumberFormatException ignored) {
            return defaultValue;
        }
    }

    private static long parseNonNegativeLong(String raw, long defaultValue) {
        if (raw == null) {
            return defaultValue;
        }
        String s = raw.trim();
        if (s.isEmpty()) {
            return defaultValue;
        }
        try {
            long v = Long.parseLong(s);
            return Math.max(0L, v);
        } catch (NumberFormatException ignored) {
            return defaultValue;
        }
    }

    private static long resolveRequestedRowCount(EngineExecutionContext context) {
        if (context == null) {
            return DEFAULT_ROW_COUNT;
        }

        String requested = context.getEngineParameter(PARAM_ROW_COUNT);
        if (requested != null && !requested.trim().isEmpty()) {
            return parseNonNegativeLong(requested, DEFAULT_ROW_COUNT);
        }

        return parseNonNegativeLong(context.getEngineParameter(PARAM_STRESS_ROWS), DEFAULT_ROW_COUNT);
    }

    private static void streamDenormalized(QuerySpec spec,
                                           GenerationPlan plan,
                                           EngineExecutionContext context,
                                           EngineResultReceiver receiver,
                                           int batchDelayMs) throws EngineException, EngineResultReceiverException {
        List<String> columns = !spec.selectVars.isEmpty() ? spec.selectVars : plan.allVariables;
        EngineTable t = new EngineTable();
        t.setKind(ResultKind.TABLE);
        t.setName("Answer relation");
        t.setColumns(columns);
        receiver.startTable(t);

        streamTableRows(context, receiver, batchDelayMs, t, plan.totalRows, i -> plan.rowFor(columns, i), "ANSWER_RELATION");
    }

    private static void streamNormalized(QuerySpec spec,
                                         GenerationPlan plan,
                                         EngineExecutionContext context,
                                         EngineResultReceiver receiver,
                                         int batchDelayMs) throws EngineException, EngineResultReceiverException {
        List<TableSpec> tables = buildNormalizedTables(spec);
        for (TableSpec tableSpec : tables) {
            EngineTable t = new EngineTable();
            t.setKind(tableSpec.kind);
            t.setName(tableSpec.name);
            t.setColumns(tableSpec.columns);
            receiver.startTable(t);

            if (tableSpec.rowMode == TableRowMode.UCC_UNIQUE) {
                int count = plan.getUccUniqueCount(tableSpec.columns.get(0));
                streamTableRows(context, receiver, batchDelayMs, t, count, i ->
                        Collections.singletonList(plan.valueFor(tableSpec.columns.get(0), i)), tableSpec.predicateLabel);
            } else {
                streamTableRows(context, receiver, batchDelayMs, t, plan.totalRows, i -> plan.rowFor(tableSpec.columns, i),
                        tableSpec.predicateLabel);
            }
        }
    }

    private static void streamTableRows(EngineExecutionContext context,
                                        EngineResultReceiver receiver,
                                        int batchDelayMs,
                                        EngineTable t,
                                        long totalRows,
                                        RowSupplier supplier,
                                        String predicateLabel) throws EngineException, EngineResultReceiverException {
        List<List<String>> batch = new ArrayList<>(Math.min(BATCH_SIZE, (int) Math.min(Integer.MAX_VALUE, totalRows)));
        long emitted = 0L;
        for (long i = 0; i < totalRows; i++) {
            if ((i % BATCH_SIZE) == 0) {
                checkCanceled(context);
            }
            List<String> row = supplier.rowFor(i);
            if (row != null) {
                batch.add(row);
                emitted++;
            }
            if (batch.size() >= BATCH_SIZE) {
                checkCanceled(context);
                receiver.receiveRows(batch);
                batch.clear();
                sleepAfterBatchIfConfigured(context, batchDelayMs);
            }
        }
        if (!batch.isEmpty()) {
            checkCanceled(context);
            receiver.receiveRows(batch);
            sleepAfterBatchIfConfigured(context, batchDelayMs);
        }
        receiver.endTable();

        System.out.println("[ExampleNormalizedQueryEngine] emitted table name=" + t.getName()
                + " kind=" + t.getKind()
                + " rows=" + emitted
                + " batchSize=" + BATCH_SIZE
                + " predicate=" + predicateLabel);
    }

    private static QuerySpec parseQuery(String query) {
        if (query == null) {
            return QuerySpec.defaultSpec();
        }
        String trimmed = query.trim();
        if (trimmed.isEmpty()) {
            return QuerySpec.defaultSpec();
        }

        List<String> selectVars = parseSelectVariables(trimmed);
        List<Predicate> predicates = parseWherePredicates(trimmed);

        if (selectVars.isEmpty() && !predicates.isEmpty()) {
            LinkedHashSet<String> vars = new LinkedHashSet<>();
            for (Predicate p : predicates) {
                vars.addAll(p.variables);
            }
            selectVars = new ArrayList<>(vars);
        }
        return new QuerySpec(selectVars, predicates);
    }

    private static List<String> parseSelectVariables(String query) {
        Pattern selectPattern = Pattern.compile("(?i)\\bselect\\b(.*?)(\\bwhere\\b|\\bfrom\\b|$)");
        Matcher m = selectPattern.matcher(query);
        if (!m.find()) {
            return Collections.emptyList();
        }
        String body = m.group(1);
        if (body == null) {
            return Collections.emptyList();
        }
        return parseVariableList(body);
    }

    private static List<Predicate> parseWherePredicates(String query) {
        Pattern wherePattern = Pattern.compile("(?i)\\bwhere\\b(.*)$");
        Matcher m = wherePattern.matcher(query);
        if (!m.find()) {
            return Collections.emptyList();
        }
        String body = m.group(1);
        if (body == null || body.trim().isEmpty()) {
            return Collections.emptyList();
        }
        String[] atoms = body.split("(?i)\\band\\b");
        List<Predicate> out = new ArrayList<>();
        Pattern atomPattern = Pattern.compile("([A-Za-z_][A-Za-z0-9_]*)\\s*\\(([^)]*)\\)");
        for (String atom : atoms) {
            Matcher am = atomPattern.matcher(atom.trim());
            if (!am.find()) {
                continue;
            }
            String rawKind = am.group(1);
            String rawVars = am.group(2);
            if (rawKind == null || rawVars == null) {
                continue;
            }
            String kind = rawKind.trim().toUpperCase(Locale.ROOT);
            List<String> vars = parseVariableList(rawVars);
            if (!vars.isEmpty()) {
                out.add(new Predicate(kind, vars));
            }
        }
        return out;
    }

    private static List<String> parseVariableList(String raw) {
        if (raw == null) {
            return Collections.emptyList();
        }
        String[] parts = raw.split(",");
        List<String> out = new ArrayList<>();
        for (String part : parts) {
            String v = part.trim();
            if (!v.isEmpty()) {
                out.add(v);
            }
        }
        return out;
    }

    private static List<TableSpec> buildNormalizedTables(QuerySpec spec) {
        List<TableSpec> out = new ArrayList<>();
        Set<String> varsInBinary = new HashSet<>();
        Set<String> unaryAlready = new HashSet<>();

        for (Predicate p : spec.predicates) {
            if (p.variables.size() == 2) {
                String left = p.variables.get(0);
                String right = p.variables.get(1);
                if (spec.selectVars.contains(left) && spec.selectVars.contains(right)) {
                    varsInBinary.add(left);
                    varsInBinary.add(right);
                    out.add(new TableSpec(kindForPredicate(p.kind),
                            p.kind + "(" + left + "," + right + ")",
                            List.of(left, right),
                            TableRowMode.ASSIGNMENT_PROJECTION,
                            p.kind + "(" + left + "," + right + ")"));
                }
            } else if (p.variables.size() == 1) {
                String var = p.variables.get(0);
                if (spec.selectVars.contains(var)) {
                    unaryAlready.add(var);
                    out.add(new TableSpec(kindForPredicate(p.kind),
                            p.kind + "(" + var + ")",
                            Collections.singletonList(var),
                            "UCC".equalsIgnoreCase(p.kind) ? TableRowMode.UCC_UNIQUE : TableRowMode.ASSIGNMENT_PROJECTION,
                            p.kind + "(" + var + ")"));
                }
            }
        }

        for (String var : spec.selectVars) {
            if (!varsInBinary.contains(var) && !unaryAlready.contains(var)) {
                out.add(new TableSpec(ResultKind.TABLE,
                        "VAR(" + var + ")",
                        Collections.singletonList(var),
                        TableRowMode.ASSIGNMENT_PROJECTION,
                        "PROJECTION(" + var + ")"));
            }
        }

        return out;
    }

    private static ResultKind kindForPredicate(String kind) {
        if ("FD".equalsIgnoreCase(kind)) {
            return ResultKind.FD_LIST;
        }
        if ("IND".equalsIgnoreCase(kind)) {
            return ResultKind.IND_LIST;
        }
        if ("UCC".equalsIgnoreCase(kind)) {
            return ResultKind.UCC_LIST;
        }
        return ResultKind.TABLE;
    }

    private static GenerationPlan buildPlan(QuerySpec spec, long stressRows) {
        List<Predicate> predicates = spec.predicates;

        if (predicates.size() == 2) {
            Predicate p0 = predicates.get(0);
            Predicate p1 = predicates.get(1);
            if (isBinaryKind(p0, "FD") && isBinaryKind(p1, "FD")) {
                String shared = findSharedVar(p0, p1);
                if (shared != null) {
                    return buildChainPlan(p0, p1, shared, stressRows, DEFAULT_JOIN_KEYS);
                }
            }
            if (isBinaryKind(p0, "IND") && isUnaryKind(p1, "UCC")) {
                String shared = findSharedVar(p0, p1);
                if (shared != null) {
                    return buildIndUccPlan(p0, shared, stressRows);
                }
            }
            if (isBinaryKind(p1, "IND") && isUnaryKind(p0, "UCC")) {
                String shared = findSharedVar(p1, p0);
                if (shared != null) {
                    return buildIndUccPlan(p1, shared, stressRows);
                }
            }
            if (isBinaryKind(p0, "IND") && isBinaryKind(p1, "IND")) {
                String shared = findSharedVar(p0, p1);
                if (shared != null) {
                    return buildChainPlan(p0, p1, shared, stressRows, DEFAULT_JOIN_KEYS);
                }
            }
        }

        return buildGenericPlan(spec, stressRows);
    }

    private static GenerationPlan buildChainPlan(Predicate first,
                                                 Predicate second,
                                                 String joinVar,
                                                 long stressRows,
                                                 int joinKeys) {
        String xVar = otherVar(first, joinVar);
        String zVar = otherVar(second, joinVar);
        if (xVar == null || zVar == null) {
            return buildGenericPlan(new QuerySpec(Collections.emptyList(), Arrays.asList(first, second)), stressRows);
        }

        Map<String, LongFunction<String>> valueByVar = new HashMap<>();
        valueByVar.put(joinVar, i -> joinVar + (i % joinKeys));
        valueByVar.put(xVar, i -> xVar + i);
        valueByVar.put(zVar, i -> zVar + i);

        List<String> allVars = new ArrayList<>(new LinkedHashSet<>(Arrays.asList(xVar, joinVar, zVar)));
        return new GenerationPlan(stressRows, allVars, valueByVar, Collections.emptyMap());
    }

    private static GenerationPlan buildIndUccPlan(Predicate ind,
                                                  String uccVar,
                                                  long stressRows) {
        String xVar = ind.variables.get(0);
        String yVar = ind.variables.get(1);
        if (!Objects.equals(yVar, uccVar)) {
            return buildGenericPlan(new QuerySpec(Collections.emptyList(), List.of(ind)), stressRows);
        }
        int uniqueCount = Math.max(1, toRowCountInt(stressRows));
        Map<String, LongFunction<String>> valueByVar = new HashMap<>();
        valueByVar.put(yVar, i -> yVar + (i % uniqueCount));
        valueByVar.put(xVar, i -> xVar + i);
        Map<String, Integer> uccCounts = new HashMap<>();
        uccCounts.put(yVar, toRowCountInt(stressRows));

        List<String> allVars = new ArrayList<>(new LinkedHashSet<>(Arrays.asList(xVar, yVar)));
        return new GenerationPlan(stressRows, allVars, valueByVar, uccCounts);
    }

    private static GenerationPlan buildGenericPlan(QuerySpec spec, long stressRows) {
        Map<String, LongFunction<String>> valueByVar = new HashMap<>();
        Map<String, Integer> uccCounts = new HashMap<>();
        Set<String> variables = new LinkedHashSet<>();
        variables.addAll(spec.selectVars);
        for (Predicate p : spec.predicates) {
            variables.addAll(p.variables);
            if (p.variables.size() == 1 && "UCC".equalsIgnoreCase(p.kind)) {
                uccCounts.put(p.variables.get(0), toRowCountInt(stressRows));
            }
        }

        for (String var : variables) {
            valueByVar.put(var, i -> var + "_" + (i % DEFAULT_JOIN_KEYS) + "_" + i);
        }

        return new GenerationPlan(stressRows, new ArrayList<>(variables), valueByVar, uccCounts);
    }

    private static int toRowCountInt(long rowCount) {
        return (int) Math.min(Integer.MAX_VALUE, Math.max(0L, rowCount));
    }

    private static boolean isBinaryKind(Predicate predicate, String kind) {
        return predicate != null
                && predicate.variables.size() == 2
                && kind.equalsIgnoreCase(predicate.kind);
    }

    private static boolean isUnaryKind(Predicate predicate, String kind) {
        return predicate != null
                && predicate.variables.size() == 1
                && kind.equalsIgnoreCase(predicate.kind);
    }

    private static String findSharedVar(Predicate left, Predicate right) {
        if (left == null || right == null) {
            return null;
        }
        for (String v : left.variables) {
            if (right.variables.contains(v)) {
                return v;
            }
        }
        return null;
    }

    private static String otherVar(Predicate predicate, String shared) {
        if (predicate == null || shared == null) {
            return null;
        }
        for (String v : predicate.variables) {
            if (!shared.equals(v)) {
                return v;
            }
        }
        return null;
    }

    private interface RowSupplier {
        List<String> rowFor(long rowIndex);
    }

    private enum TableRowMode {
        ASSIGNMENT_PROJECTION,
        UCC_UNIQUE
    }

    private static final class TableSpec {
        private final ResultKind kind;
        private final String name;
        private final List<String> columns;
        private final TableRowMode rowMode;
        private final String predicateLabel;

        private TableSpec(ResultKind kind, String name, List<String> columns, TableRowMode rowMode, String predicateLabel) {
            this.kind = kind;
            this.name = name;
            this.columns = columns;
            this.rowMode = rowMode;
            this.predicateLabel = predicateLabel;
        }
    }

    private static final class Predicate {
        private final String kind;
        private final List<String> variables;

        private Predicate(String kind, List<String> variables) {
            this.kind = kind;
            this.variables = variables;
        }
    }

    private static final class QuerySpec {
        private final List<String> selectVars;
        private final List<Predicate> predicates;

        private QuerySpec(List<String> selectVars, List<Predicate> predicates) {
            this.selectVars = selectVars;
            this.predicates = predicates;
        }

        private static QuerySpec defaultSpec() {
            return new QuerySpec(List.of("X", "Y"), Collections.emptyList());
        }
    }

    private static final class GenerationPlan {
        private final long totalRows;
        private final List<String> allVariables;
        private final Map<String, LongFunction<String>> valueByVar;
        private final Map<String, Integer> uccUniqueCounts;

        private GenerationPlan(long totalRows,
                               List<String> allVariables,
                               Map<String, LongFunction<String>> valueByVar,
                               Map<String, Integer> uccUniqueCounts) {
            this.totalRows = totalRows;
            this.allVariables = allVariables;
            this.valueByVar = valueByVar;
            this.uccUniqueCounts = uccUniqueCounts;
        }

        private String valueFor(String var, long rowIndex) {
            LongFunction<String> fn = valueByVar.get(var);
            if (fn == null) {
                return var + "_" + rowIndex;
            }
            return fn.apply(rowIndex);
        }

        private List<String> rowFor(List<String> columns, long rowIndex) {
            List<String> row = new ArrayList<>(columns.size());
            for (String col : columns) {
                row.add(valueFor(col, rowIndex));
            }
            return row;
        }

        private int getUccUniqueCount(String var) {
            Integer count = uccUniqueCounts.get(var);
            if (count != null) {
                return count;
            }
            return toRowCountInt(totalRows);
        }
    }

}
