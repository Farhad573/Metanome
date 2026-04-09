package de.metanome.engine.api;

import java.util.ServiceLoader;

/**
 * Loads ProfilingQueryEngine implementations from the classpath using ServiceLoader.
 * The backend should always obtain its engine instance through this class.
 *
 * This ensures that the backend is fully decoupled from any specific engine
 * implementation and only depends on the ProfilingQueryEngine interface.
 */
public final class EngineProvider {

    private static ProfilingQueryEngine cached;

    private EngineProvider() {
    }

    /**
     * Loads the default ProfilingQueryEngine implementation.
     * If multiple implementations are available, the first discovered one is used.
     */
    public static synchronized ProfilingQueryEngine loadDefaultEngine() {
        // Return cached instance if already loaded
        if (cached != null) {
            return cached;
        }

        ServiceLoader<ProfilingQueryEngine> loader =
            ServiceLoader.load(ProfilingQueryEngine.class);

        java.util.Iterator<ProfilingQueryEngine> iterator = loader.iterator();
        if (!iterator.hasNext()) {
            throw new IllegalStateException(
                "No ProfilingQueryEngine implementation found on classpath");
        }
        cached = iterator.next();

        return cached;
    }
}
