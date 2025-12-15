package prerna.reactor.project.dev_environment;

import java.io.IOException;
import java.time.Duration;
import java.time.OffsetDateTime;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.apis.NetworkingV1Api;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodList;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.KubernetesUtil;
import prerna.util.Utility;
import prerna.sablecc2.om.PixelDataType;

public class IdleTimeoutReactor extends AbstractReactor {

    private static final Logger logger = LogManager.getLogger(IdleTimeoutReactor.class.getName());
    private static final long IDLE_TIMEOUT_HOURS = Long.parseLong(prerna.util.Utility.getDIHelperProperty("idle_timeout_hours"));

    public IdleTimeoutReactor() {
        this.keysToGet = new String[]{};
    }

    @Override
    public NounMetadata execute() {
        try {
            ApiClient client = KubernetesUtil.getApiClient();
            CoreV1Api api = new CoreV1Api(client);
            NetworkingV1Api networkingApi = new NetworkingV1Api(client);

            String namespace = KubernetesUtil.getNamespace();
            V1PodList list = api.listNamespacedPod(namespace, null, null, null, null, "app=dev-env", null, null, null, null);

            for (V1Pod item : list.getItems()) {
                String podName = item.getMetadata().getName();
                String lastActivity = item.getMetadata().getAnnotations().get("semoss.org/last-activity");
                if (lastActivity != null) {
                    OffsetDateTime lastActivityTimestamp = OffsetDateTime.parse(lastActivity);
                    long ageHours = Duration.between(lastActivityTimestamp, OffsetDateTime.now()).toHours();
                    if (ageHours >= IDLE_TIMEOUT_HOURS) {
                        String projectId = podName.substring("dev-env-".length());
                        String serviceName = "dev-env-service-" + projectId;
                        String ingressName = "dev-env-ingress-" + projectId;

                        // Delete the ingress
                        try {
                            networkingApi.deleteNamespacedIngress(ingressName, namespace, null, null, null, null, null, null);
                        } catch (ApiException e) {
                            // Ignore if not found
                        }

                        // Delete the service
                        try {
                            api.deleteNamespacedService(serviceName, namespace, null, null, null, null, null, null);
                        } catch (ApiException e) {
                            // Ignore if not found
                        }

                        // Delete the pod
                        try {
                            api.deleteNamespacedPod(podName, namespace, null, null, null, null, null, null);
                        } catch (ApiException e) {
                            // Ignore if not found
                        }

                        // remove the pod name from the project
                        try {
                            IProject project = Utility.getProject(projectId);
                            project.setDevContainerPodName(null);
                        } catch (Exception e) {
                            logger.error("Failed to remove pod name from project " + projectId, e);
                        }
                    }
                }
            }

            return new NounMetadata("Idle dev environments torn down successfully", PixelDataType.CONST_STRING);
        } catch (IOException | ApiException e) {
            throw new RuntimeException("Failed to tear down idle dev environments", e);
        }
    }
}
