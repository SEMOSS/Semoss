package prerna.reactor.project.dev_environment;

import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.apis.NetworkingV1Api;
import io.kubernetes.client.openapi.models.V1Ingress;
import io.kubernetes.client.openapi.models.V1IngressRule;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodCondition;
import io.kubernetes.client.openapi.models.V1PodList;
import io.kubernetes.client.openapi.models.V1PodStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.KubernetesUtil;
import prerna.util.Utility;

import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class GetDevEnvironmentStatusReactor extends AbstractReactor {

    private static final Logger logger = LogManager.getLogger(GetDevEnvironmentStatusReactor.class);

    public GetDevEnvironmentStatusReactor() {
        this.keysToGet = new String[]{"projectId"};
    }

    @Override
    public NounMetadata execute() {
        DevEnvironmentUtils.ensureDevContainersEnabled();
        organizeKeys();
        String projectId = this.keyValue.get("projectId");
        if (projectId == null || projectId.isEmpty()) {
            throw new IllegalArgumentException("Project ID is required to check the dev environment status.");
        }

        User user = this.insight.getUser();
        if (!SecurityProjectUtils.userCanEditProject(user, projectId)) {
            throw new SecurityException("User does not have permission to inspect the dev environment for this project.");
        }

        try {
            ApiClient client = KubernetesUtil.getApiClient();
            CoreV1Api api = new CoreV1Api(client);
            NetworkingV1Api networkingApi = new NetworkingV1Api(client);

            String namespace = KubernetesUtil.getNamespace();
            V1Pod pod = findPod(api, namespace, projectId);
            if (pod == null) {
                return new NounMetadata("No dev environment pod found for this project.", PixelDataType.CONST_STRING);
            }

            String ingressUrl = resolveIngressUrl(networkingApi, namespace, projectId);
            String password = Optional.ofNullable(pod.getMetadata())
                    .map(m -> m.getAnnotations())
                    .map(a -> a.get(DevEnvironmentUtils.ANNOTATION_PASSWORD))
                    .orElse("not available yet");
            String assetStatus = Optional.ofNullable(pod.getMetadata())
                    .map(m -> m.getAnnotations())
                    .map(a -> a.get(DevEnvironmentUtils.ANNOTATION_ASSET_STATUS))
                    .orElse("unknown");

            V1PodStatus status = pod.getStatus();
            boolean ready = isPodReady(status) && "synced".equalsIgnoreCase(assetStatus);

            Map<String, Object> payload = new HashMap<>();
            payload.put("ready", ready);
            payload.put("url", ingressUrl != null ? ingressUrl : "");
            payload.put("passcode", password);

            if (!ready) {
                payload.put("phase", status != null ? status.getPhase() : "Unknown");
                payload.put("assets", assetStatus);
                payload.put("details", buildConditionSummary(status));
            }

            return new NounMetadata(payload, PixelDataType.MAP);
        } catch (Exception e) {
            logger.error("Unable to fetch dev environment status for {}", projectId, e);
            throw new RuntimeException("Failed to fetch dev environment status", e);
        }
    }

    private V1Pod findPod(CoreV1Api api, String namespace, String projectId) throws ApiException {
        V1PodList list = api.listNamespacedPod(namespace)
                .labelSelector(DevEnvironmentUtils.buildLabelSelector(projectId))
                .limit(1)
                .execute();
        return list.getItems().isEmpty() ? null : list.getItems().get(0);
    }

    private String resolveIngressUrl(NetworkingV1Api networkingApi, String namespace, String projectId) {
        try {
            V1Ingress ingress = networkingApi.readNamespacedIngress(
                    DevEnvironmentUtils.ingressName(projectId), namespace).execute();
            if (ingress.getSpec() != null && ingress.getSpec().getRules() != null) {
                for (V1IngressRule rule : ingress.getSpec().getRules()) {
                    if (rule.getHost() != null && !rule.getHost().isEmpty()) {
                        return ensureTrailingSlash("https://" + rule.getHost()) + projectId;
                    }
                }
            }
        } catch (ApiException e) {
            logger.debug("Unable to read ingress for project {}: {}", projectId, e.getResponseBody());
        }

        String baseUrl = Utility.getDIHelperProperty("kubernetes_ingress_base_url");
        if (baseUrl != null && !baseUrl.isEmpty()) {
            return ensureTrailingSlash(baseUrl) + projectId;
        }
        return null;
    }

    private String ensureTrailingSlash(String value) {
        if (value.endsWith("/")) {
            return value;
        }
        return value + "/";
    }

    private String buildConditionSummary(V1PodStatus status) {
        if (status == null || status.getConditions() == null) {
            return "Conditions unavailable";
        }
        StringBuilder sb = new StringBuilder();
        for (V1PodCondition condition : status.getConditions()) {
            if (sb.length() > 0) {
                sb.append("; ");
            }
            sb.append(formatCondition(condition));
        }
        return sb.toString();
    }

    private boolean isPodReady(V1PodStatus status) {
        if (status == null || status.getConditions() == null) {
            return false;
        }
        return status.getConditions().stream()
                .anyMatch(cond -> "Ready".equalsIgnoreCase(cond.getType()) && "True".equalsIgnoreCase(cond.getStatus()));
    }

    private String formatCondition(V1PodCondition condition) {
        if (condition == null) {
            return "unknown condition";
        }
        String type = condition.getType();
        String status = condition.getStatus();
        String reason = condition.getReason();
        OffsetDateTime timestamp = condition.getLastTransitionTime();
        if (timestamp == null && condition.getLastProbeTime() != null) {
            timestamp = condition.getLastProbeTime();
        }
        String humanTime = timestamp != null
                ? timestamp.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
                : "n/a";
        return type + "=" + status + (reason != null ? " (" + reason + ")" : "") + " @ " + humanTime;
    }
}
