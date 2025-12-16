package prerna.reactor.project.dev_environment;

import java.text.Normalizer;
import java.util.Locale;

/**
 * Shared helpers/constants for dev environment reactors that interact with
 * Kubernetes resources.
 */
final class DevEnvironmentUtils {

    private DevEnvironmentUtils() {}

    static final String APP_LABEL_KEY = "app";
    static final String APP_LABEL_VALUE = "dev-env";
    static final String PROJECT_LABEL_KEY = "semoss.org/project-id";

    static final String ANNOTATION_LAST_ACTIVITY = "semoss.org/last-activity";
    static final String ANNOTATION_PASSWORD = "semoss.org/dev-password";
    static final String ANNOTATION_ASSET_STATUS = "semoss.org/assets-status";

    static final String ASSET_STATUS_PENDING = "pending";
    static final String ASSET_STATUS_SYNCED = "synced";
    static final String ASSET_STATUS_FAILED = "failed";

    static String sanitizeLabelValue(String raw) {
        if (raw == null || raw.isEmpty()) {
            return "unknown";
        }
        String normalized = Normalizer.normalize(raw, Normalizer.Form.NFD)
                .replaceAll("[^A-Za-z0-9.-]", "-")
                .toLowerCase(Locale.ROOT);
        if (normalized.length() > 63) {
            normalized = normalized.substring(0, 63);
        }
        if (normalized.endsWith("-")) {
            normalized = normalized.replaceAll("-+$", "");
        }
        return normalized.isEmpty() ? "unknown" : normalized;
    }

    static String buildLabelSelector(String projectId) {
        return APP_LABEL_KEY + "=" + APP_LABEL_VALUE + "," +
                PROJECT_LABEL_KEY + "=" + sanitizeLabelValue(projectId);
    }

    static String podName(String projectId) {
        return "dev-env-" + projectId;
    }

    static String serviceName(String projectId) {
        return "dev-env-service-" + projectId;
    }

    static String ingressName(String projectId) {
        return "dev-env-ingress-" + projectId;
    }
}
