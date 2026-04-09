package de.metanome.engine.api;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
/**
 *it describes exactly what the object represents:
 * All information the engine needs in order to execute a query in a specific runtime context.
 * It groups the parameters needed for one execution
 * When you run a DPQL query, the engine needs this information.
 */
public final class EngineExecutionContext {

    private String dataset;
    private String basePath;
    private String separator;
    private String quoteChar;

    /**
     * Cross-engine option: if true, engines should emit only normalized FD/IND/UCC tables.
     * Default: false.
     */
    private boolean normalizedOnly;

    /**
     * Optional, engine-specific parameters.
     *
     * Values are transported as strings; engines can parse as needed.
     */
    private Map<String, String> engineParameters;

    /**
     * Cooperative cancellation signal.
     *
     * The backend may provide a token for async runs; engines should check it periodically.
     * Default: never canceled.
     */
    private CancellationToken cancellationToken;

    public boolean isCached() {
        return isCached;
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

    public void setCached(boolean cached) {
        isCached = cached;
    }

    private boolean isCached;

    public boolean isNormalizedOnly() {
        return normalizedOnly;
    }

    public void setNormalizedOnly(boolean normalizedOnly) {
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

    public Map<String, String> getEngineParameters() {
        if (engineParameters == null) {
            return Collections.emptyMap();
        }
        return engineParameters;
    }

    public void setEngineParameters(Map<String, String> engineParameters) {
        if (engineParameters == null) {
            this.engineParameters = null;
            return;
        }
        this.engineParameters = new HashMap<>(engineParameters);
    }

    public String getEngineParameter(String key) {
        if (key == null) {
            return null;
        }
        return getEngineParameters().get(key);
    }

    public CancellationToken getCancellationToken() {
        return cancellationToken;
    }

    public void setCancellationToken(CancellationToken cancellationToken) {
        this.cancellationToken = cancellationToken;
    }

    public boolean isCanceled() {
        return cancellationToken != null && cancellationToken.isCanceled();
    }

    public void throwIfCanceled() throws EngineException {
        if (cancellationToken != null) {
            cancellationToken.throwIfCanceled();
        }
    }
}
