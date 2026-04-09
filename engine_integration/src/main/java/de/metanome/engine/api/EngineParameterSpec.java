package de.metanome.engine.api;

import java.io.Serializable;

/**
 * Describes one parameter that should be shown in the DPQL UI for a specific engine.
 *
 * This class is used for JSON serialization in the backend.
 */
public class EngineParameterSpec implements Serializable {

    private static final long serialVersionUID = 1L;

    private String key;
    private String label;
    private EngineParameterType type;
    private boolean required;

    // Values are represented as strings in the UI/transport; engines can parse as needed.
    private String defaultValue;
    private String placeholder;
    private String helpText;

    public EngineParameterSpec() {
    }

    public EngineParameterSpec(String key, String label, EngineParameterType type) {
        this.key = key;
        this.label = label;
        this.type = type;
    }

    public static EngineParameterSpec stringParam(String key, String label, String defaultValue, boolean required) {
        EngineParameterSpec s = new EngineParameterSpec(key, label, EngineParameterType.STRING);
        s.setDefaultValue(defaultValue);
        s.setRequired(required);
        return s;
    }

    public static EngineParameterSpec booleanParam(String key, String label, boolean defaultValue, boolean required) {
        EngineParameterSpec s = new EngineParameterSpec(key, label, EngineParameterType.BOOLEAN);
        s.setDefaultValue(Boolean.toString(defaultValue));
        s.setRequired(required);
        return s;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public EngineParameterType getType() {
        return type;
    }

    public void setType(EngineParameterType type) {
        this.type = type;
    }

    public boolean isRequired() {
        return required;
    }

    public void setRequired(boolean required) {
        this.required = required;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
    }

    public String getPlaceholder() {
        return placeholder;
    }

    public void setPlaceholder(String placeholder) {
        this.placeholder = placeholder;
    }

    public String getHelpText() {
        return helpText;
    }

    public void setHelpText(String helpText) {
        this.helpText = helpText;
    }
}
