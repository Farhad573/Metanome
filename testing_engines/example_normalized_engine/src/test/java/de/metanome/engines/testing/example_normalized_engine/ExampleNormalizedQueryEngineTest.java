package de.metanome.engines.testing.example_normalized_engine;

import de.metanome.engine.api.EngineExecutionContext;
import de.metanome.engine.api.EngineTable;
import de.metanome.engine.api.ResultKind;
import de.metanome.engine.api.result_receiver.EngineResultMetadata;
import de.metanome.engine.api.result_receiver.EngineResultReceiver;
import de.metanome.engine.api.result_receiver.EngineResultReceiverException;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.not;

public class ExampleNormalizedQueryEngineTest {

    @Test
    public void testNormalizedEmitsOnlyRequiredTables() throws Exception {
        ExampleNormalizedQueryEngine engine = new ExampleNormalizedQueryEngine();
        CollectingReceiver receiver = new CollectingReceiver();
        EngineExecutionContext ctx = contextWithRowCount(25);
        ctx.setNormalizedOnly(true);

        engine.execute("SELECT X, Y WHERE FD(X,Y)", ctx, receiver);

        assertThat(receiver.tables, hasSize(1));
        TableData table = receiver.tables.get(0);
        assertThat(table.table.getKind(), is(ResultKind.FD_LIST));
        assertThat(table.table.getName(), is("FD(X,Y)"));
        assertThat(table.table.getColumns(), is(List.of("X", "Y")));
    }

    @Test
    public void testIndUccPairsAreJoinable() throws Exception {
        ExampleNormalizedQueryEngine engine = new ExampleNormalizedQueryEngine();
        CollectingReceiver receiver = new CollectingReceiver();
        EngineExecutionContext ctx = contextWithRowCount(250);
        ctx.setNormalizedOnly(true);

        engine.execute("SELECT X, Y WHERE IND(X,Y) AND UCC(Y)", ctx, receiver);

        TableData ind = receiver.byName("IND(X,Y)");
        TableData ucc = receiver.byName("UCC(Y)");

        assertThat(ind, not(equalTo(null)));
        assertThat(ucc, not(equalTo(null)));
        assertThat(ind.rows, hasSize(250));
        assertThat(ucc.rows, hasSize(250));

        Set<String> uccValues = new HashSet<>();
        for (List<String> row : ucc.rows) {
            uccValues.add(row.get(0));
        }

        for (List<String> row : ind.rows) {
            assertThat(uccValues.contains(row.get(1)), is(true));
        }
    }

    @Test
    public void testFdChainSharesJoinY() throws Exception {
        ExampleNormalizedQueryEngine engine = new ExampleNormalizedQueryEngine();
        CollectingReceiver receiver = new CollectingReceiver();
        EngineExecutionContext ctx = contextWithRowCount(100);
        ctx.setNormalizedOnly(true);

        engine.execute("SELECT X, Y, Z WHERE FD(X,Y) AND FD(Y,Z)", ctx, receiver);

        TableData fdXY = receiver.byName("FD(X,Y)");
        TableData fdYZ = receiver.byName("FD(Y,Z)");

        assertThat(fdXY, not(equalTo(null)));
        assertThat(fdYZ, not(equalTo(null)));

        Set<String> yFromXY = new HashSet<>();
        for (List<String> row : fdXY.rows) {
            yFromXY.add(row.get(1));
        }
        Set<String> yFromYZ = new HashSet<>();
        for (List<String> row : fdYZ.rows) {
            yFromYZ.add(row.get(0));
        }

        Set<String> intersection = new HashSet<>(yFromXY);
        intersection.retainAll(yFromYZ);
        assertThat(intersection.isEmpty(), is(false));
        assertThat(yFromXY, is(yFromYZ));
    }

    @Test
    public void testEngineExposesRowCountParameter() {
        ExampleNormalizedQueryEngine engine = new ExampleNormalizedQueryEngine();

        assertThat(engine.getParameterSpecifications(), hasSize(2));
        assertThat(engine.getParameterSpecifications().get(1).getKey(), is("rowCount"));
        assertThat(engine.getParameterSpecifications().get(1).getLabel(), is("Rows to produce"));
        assertThat(engine.getParameterSpecifications().get(1).getDefaultValue(), notNullValue());
    }

    @Test
    public void testLegacyStressRowsParameterStillWorks() throws Exception {
        ExampleNormalizedQueryEngine engine = new ExampleNormalizedQueryEngine();
        CollectingReceiver receiver = new CollectingReceiver();

        engine.execute("SELECT X, Y WHERE FD(X,Y)", contextWithLegacyStressRows(7), receiver);

        assertThat(receiver.tables, hasSize(1));
        assertThat(receiver.tables.get(0).rows, hasSize(7));
    }

    private static EngineExecutionContext contextWithRowCount(int rowCount) {
        EngineExecutionContext ctx = new EngineExecutionContext();
        Map<String, String> params = new HashMap<>();
        params.put("rowCount", Integer.toString(rowCount));
        ctx.setEngineParameters(params);
        return ctx;
    }

    private static EngineExecutionContext contextWithLegacyStressRows(int stressRows) {
        EngineExecutionContext ctx = new EngineExecutionContext();
        Map<String, String> params = new HashMap<>();
        params.put("stressRows", Integer.toString(stressRows));
        ctx.setEngineParameters(params);
        return ctx;
    }

    private static final class TableData {
        private final EngineTable table;
        private final List<List<String>> rows = new ArrayList<>();

        private TableData(EngineTable table) {
            this.table = table;
        }
    }

    private static final class CollectingReceiver implements EngineResultReceiver {
        private final List<TableData> tables = new ArrayList<>();
        private TableData current;

        @Override
        public void start(EngineResultMetadata executionMetadata) {
        }

        @Override
        public void startTable(EngineTable table) {
            current = new TableData(table);
            tables.add(current);
        }

        @Override
        public void receiveRow(List<String> row) throws EngineResultReceiverException {
            if (current != null) {
                current.rows.add(row);
            }
        }

        @Override
        public void endTable() {
            current = null;
        }

        @Override
        public void finish() {
        }

        private TableData byName(String name) {
            for (TableData t : tables) {
                if (t.table.getName().equals(name)) {
                    return t;
                }
            }
            return null;
        }
    }
}
