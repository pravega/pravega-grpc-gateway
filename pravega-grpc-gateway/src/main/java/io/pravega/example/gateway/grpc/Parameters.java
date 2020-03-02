package io.pravega.example.gateway.grpc;

import java.net.URI;

/**
All parameters will come from environment variables. This makes it easy
to configure on Docker, Kubernetes, Gradle, etc..
*/
class Parameters {
    // By default, we will connect to a standalone Pravega running on localhost.
    public static URI getControllerURI() {
        return URI.create(getEnvVar("PRAVEGA_CONTROLLER", "tcp://localhost:9090"));
    }

    public static int getListenPort() {
        return Integer.parseInt(getEnvVar("LISTEN_PORT", "54672"));
    }

    public static long getCleanupPeriodSec() {
        return Long.parseLong(getEnvVar("CLEANUP_PERIOD_SEC", "60"));
    }

    private static String getEnvVar(String name, String defaultValue) {
        String value = System.getenv(name);
        if (value == null || value.isEmpty()) {
            return defaultValue;
        }
        return value;
    }
}
