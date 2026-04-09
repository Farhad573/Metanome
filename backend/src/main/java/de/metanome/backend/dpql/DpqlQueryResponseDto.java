package de.metanome.backend.dpql;

import java.util.List;
import java.util.Map;

public class DpqlQueryResponseDto {
    private List<DpqlTableDto> tables;
    private Map<String, String> metadata;

    public List<DpqlTableDto> getTables() {
        return tables;
    }

    public void setTables(List<DpqlTableDto> tables) {
        this.tables = tables;
    }

    public Map<String, String> getMetadata() {
        return metadata;
    }

    public void setMetadata(Map<String, String> metadata) {
        this.metadata = metadata;
    }
}
