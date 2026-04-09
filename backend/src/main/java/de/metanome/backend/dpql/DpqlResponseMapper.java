package de.metanome.backend.dpql;

import de.metanome.engine.api.EngineResult;
import de.metanome.engine.api.EngineTable;

import java.util.ArrayList;
import java.util.List;

public final class DpqlResponseMapper {

    public static DpqlQueryResponseDto fromEngineResult(EngineResult engineResult) {
        DpqlQueryResponseDto dto = new DpqlQueryResponseDto();

        List<DpqlTableDto> tableDtos = new ArrayList<>();
        if (engineResult.getTables() != null) {
            for (EngineTable t : engineResult.getTables()) {
                DpqlTableDto td = new DpqlTableDto();
                td.setKind(t.getKind() != null ? t.getKind().name() : null);
                td.setName(t.getName());
                td.setColumns(t.getColumns());
                td.setRows(t.getRows());
                td.setMetadata(t.getMetadata());
                tableDtos.add(td);
            }
        }

        dto.setTables(tableDtos);
        dto.setMetadata(engineResult.getMetadata());
        return dto;
    }
}

