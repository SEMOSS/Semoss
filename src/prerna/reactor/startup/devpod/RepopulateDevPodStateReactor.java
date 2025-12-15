package prerna.reactor.startup.devpod;

import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodList;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.KubernetesUtil;
import prerna.util.Utility;
import prerna.sablecc2.om.PixelDataType;

public class RepopulateDevPodStateReactor extends AbstractReactor {

    @Override
    public NounMetadata execute() {
        try {
            ApiClient client = KubernetesUtil.getApiClient();
            CoreV1Api api = new CoreV1Api(client);

            String namespace = KubernetesUtil.getNamespace();
            V1PodList list = api.listNamespacedPod(namespace, null, null, null, null, "app=dev-env", null, null, null, null);

            for (V1Pod item : list.getItems()) {
                String podName = item.getMetadata().getName();
                String projectId = podName.substring("dev-env-".length());
                try {
                    IProject project = Utility.getProject(projectId);
                    project.setDevContainerPodName(podName);
                } catch (Exception e) {
                    // Log this or handle it appropriately
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            // Log this or handle it appropriately
            e.printStackTrace();
        }
        return new NounMetadata("Dev pod state repopulated successfully", PixelDataType.CONST_STRING);
    }
}
