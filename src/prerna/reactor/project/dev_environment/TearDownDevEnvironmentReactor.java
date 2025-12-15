package prerna.reactor.project.dev_environment;

import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.apis.NetworkingV1Api;
import prerna.auth.User;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.KubernetesUtil;
import prerna.util.Utility;
import prerna.sablecc2.om.PixelDataType;

import java.io.IOException;

public class TearDownDevEnvironmentReactor extends AbstractReactor {

    private static final Logger logger = LogManager.getLogger(TearDownDevEnvironmentReactor.class.getName());

    public TearDownDevEnvironmentReactor() {
        this.keysToGet = new String[]{"projectId"};
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();
        String projectId = this.keyValue.get("projectId");

        if (projectId == null || projectId.isEmpty()) {
            throw new IllegalArgumentException("Project ID is required to tear down the dev environment.");
        }

        User user = this.insight.getUser();
        if (!SecurityProjectUtils.userCanEditProject(user, projectId)) {
            throw new SecurityException("User does not have permission to tear down the dev environment for this project.");
        }

        try {
            ApiClient client = KubernetesUtil.getApiClient();
            CoreV1Api api = new CoreV1Api(client);
            NetworkingV1Api networkingApi = new NetworkingV1Api(client);

            String namespace = KubernetesUtil.getNamespace();
            String podName = "dev-env-" + projectId;
            String serviceName = "dev-env-service-" + projectId;
            String ingressName = "dev-env-ingress-" + projectId;

            // Delete the ingress
            try {
                networkingApi.deleteNamespacedIngress(ingressName, namespace).execute();
            } catch (ApiException e) {
                // Ignore if not found
            }

            // Delete the service
            try {
                api.deleteNamespacedService(serviceName, namespace).execute();
            } catch (ApiException e) {
                // Ignore if not found
            }

            // Delete the pod
            try {
                api.deleteNamespacedPod(podName, namespace).execute();
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

            return new NounMetadata("Dev environment torn down successfully", PixelDataType.CONST_STRING);
        } catch (IOException e) {
            throw new RuntimeException("Failed to tear down dev environment", e);
        }
    }
}
