package de.metanome.backend.dpql;

import java.util.Map;

public class DpqlQuerryRequest {
    private String query;
    // Optional engine selection (registered engine id or direct jar file name)
    private Long engineId;
    private String engineFileName;

    /**
     * Cross-engine option: if true, engines should emit only normalized FD/IND/UCC tables
     * (instead of any engine-specific/OTHER outputs).
     */
    private Boolean normalizedOnly;

    private String dataset;
    private String basePath;
    private String separator;
    private String quoteChar;
    private Boolean cached;

    // Engine-specific optional parameters (transported as strings)
    private Map<String, String> engineParameters;

    public DpqlQuerryRequest() {
    }

    public DpqlQuerryRequest(String query) {
        this.query = query;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public Long getEngineId() {
        return engineId;
    }

    public void setEngineId(Long engineId) {
        this.engineId = engineId;
    }

    public String getEngineFileName() {
        return engineFileName;
    }

    public void setEngineFileName(String engineFileName) {
        this.engineFileName = engineFileName;
    }

    public Boolean getNormalizedOnly() {
        return normalizedOnly;
    }

    public void setNormalizedOnly(Boolean normalizedOnly) {
        this.normalizedOnly = normalizedOnly;
    }

    public String getDataset() {
        return dataset;
    }

    public void setDataset(String dataset) {
        this.dataset = dataset;
    }

    public String getBasePath() {
        return basePath;
    }

    public void setBasePath(String basePath) {
        this.basePath = basePath;
    }

    public String getSeparator() {
        return separator;
    }

    public void setSeparator(String separator) {
        this.separator = separator;
    }

    public String getQuoteChar() {
        return quoteChar;
    }

    public void setQuoteChar(String quoteChar) {
        this.quoteChar = quoteChar;
    }

    public Boolean getCached() {
        return cached;
    }

    public void setCached(Boolean cached) {
        this.cached = cached;
    }

    public Map<String, String> getEngineParameters() {
        return engineParameters;
    }

    public void setEngineParameters(Map<String, String> engineParameters) {
        this.engineParameters = engineParameters;
    }

}
