package de.metanome.engine.api.result_receiver;

import de.metanome.engine.api.EngineExecutionContext;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Generic metadata descriptor that engines can populate to describe their run.
 */
public final class EngineResultMetadata {

    private final Map<String, String> values = new HashMap<>();

    public EngineResultMetadata add(String key, String value) {
        if (key != null && value != null) {
            values.put(key, value);
        }
        return this;
    }

    public EngineResultMetadata addAll(Map<String, String> other) {
        if (other != null) {
            other.forEach(this::add);
        }
        return this;
    }

    public Map<String, String> asMap() {
        return Collections.unmodifiableMap(values);
    }

    public String get(String key) {
        return values.get(key);
    }

    public static EngineResultMetadata from(String engineName,
                                            String query,
                                            EngineExecutionContext context) {
        EngineResultMetadata metadata = new EngineResultMetadata();
        metadata.add("engineName", engineName)
                .add("query", query);

        if (context != null) {
            metadata.add("dataset", context.getDataset())
                    .add("basePath", context.getBasePath())
                    .add("separator", context.getSeparator())
                    .add("quoteChar", context.getQuoteChar())
                    .add("cached", String.valueOf(context.isCached()));
        }
        return metadata;
    }
}
