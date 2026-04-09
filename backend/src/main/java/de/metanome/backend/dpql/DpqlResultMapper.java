package de.metanome.backend.dpql;

import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.engine.api.EngineResult;
import de.metanome.engine.api.EngineTable;

import java.util.*;
import java.util.stream.Collectors;

public class DpqlResultMapper {
    private DpqlResultMapper() {
    }
    public static DpqlResultDto fromEngineTable(EngineTable table,
                                                Map<String, String> globalMetadata) {
        DpqlResultDto dto = new DpqlResultDto();
        if (table != null) {
            dto.setKind(table.getKind() != null ? table.getKind().name() : null);
            dto.setName(table.getName());
            dto.setColumns(table.getColumns());
            dto.setRows(table.getRows());
        } else {
            dto.setColumns(new ArrayList<>());
            dto.setRows(new ArrayList<>());
        }

        dto.setMetadata(globalMetadata); // or merge global + table metadata later
        return dto;
    }


}
