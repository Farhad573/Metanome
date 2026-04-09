package de.metanome.engine.api;

/**
 * Minimal set of supported parameter types for DPQL engine execution.
 *
 * This is intentionally small to keep the API stable across engine versions.
 */
public enum EngineParameterType {
    STRING,
    BOOLEAN
}
