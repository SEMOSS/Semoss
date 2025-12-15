package prerna.reactor.project.dev_environment;

import io.kubernetes.client.Exec;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.apis.NetworkingV1Api;
import io.kubernetes.client.openapi.models.*;
import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.KubernetesUtil;
import prerna.util.Utility;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.io.BufferedInputStream;

public class LaunchDevEnvironmentReactor extends AbstractReactor {

    private static final long POD_READINESS_TIMEOUT_SECONDS = 120;

    public LaunchDevEnvironmentReactor() {
        this.keysToGet = new String[]{"projectId", "projectName"};
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();
        String projectId = this.keyValue.get("projectId");
        String projectName = this.keyValue.get("projectName");

        if (projectId == null || projectId.isEmpty()) {
            throw new IllegalArgumentException("Project ID is required to launch the dev environment.");
        }
        if (projectName == null || projectName.isEmpty()) {
            throw new IllegalArgumentException("Project name is required to launch the dev environment.");
        }

        User user = this.insight.getUser();
        if (!SecurityProjectUtils.userCanEditProject(user, projectId)) {
            throw new SecurityException("User does not have permission to launch the dev environment for this project.");
        }

        
        IProject project = Utility.getProject(this.insight.getProjectId());
        if (project.getDevContainerPodName() != null && !project.getDevContainerPodName().isEmpty()) {
            return new NounMetadata("Dev environment is already running for this project.", PixelDataType.CONST_STRING);
        }

        ApiClient client = null;
        CoreV1Api api = null;
        NetworkingV1Api networkingApi = null;
        String namespace = null;
        String podName = null;
        String serviceName = null;
        String ingressName = null;
        boolean podCreated = false;
        boolean serviceCreated = false;

        try {
            client = KubernetesUtil.getApiClient();
            api = new CoreV1Api(client);
            networkingApi = new NetworkingV1Api(client);

            namespace = KubernetesUtil.getNamespace();
            podName = "dev-env-" + projectId;
            serviceName = "dev-env-service-" + projectId;
            ingressName = "dev-env-ingress-" + projectId;
            String password = RandomStringUtils.randomAlphanumeric(16);

            // Create the pod
            String now = OffsetDateTime.now().toString();
            V1Pod pod = new V1PodBuilder()
                    .withNewMetadata()
                    .withName(podName)
                    .withLabels(Collections.singletonMap("app", "dev-env"))
                    .addToAnnotations("semoss.org/last-activity", now)
                    .endMetadata()
                    .withNewSpec()
                    .withSecurityContext(new V1PodSecurityContext().runAsUser(1000L).runAsGroup(1000L).fsGroup(1000L))
                    .addNewContainer()
                    .withName("code-server")
                    .withImage(prerna.util.Utility.getDIHelperProperty("code_server_image"))
                    .withArgs(Collections.singletonList("--auth=password"))
                    .addNewEnv().withName("PASSWORD").withValue(password).endEnv()
                    .withNewResources()
                    .addToRequests("cpu", prerna.util.Utility.getDIHelperProperty("kubernetes_cpu_request"))
                    .addToRequests("memory", prerna.util.Utility.getDIHelperProperty("kubernetes_mem_request"))
                    .addToLimits("cpu", prerna.util.Utility.getDIHelperProperty("kubernetes_cpu_limit"))
                    .addToLimits("memory", prerna.util.Utility.getDIHelperProperty("kubernetes_mem_limit"))
                    .endResources()
                    .endContainer()
                    .endSpec()
                    .build();
            api.createNamespacedPod(namespace, pod, null, null, null);
            podCreated = true;

            // Wait for the pod to be running
            long startTime = System.currentTimeMillis();
            while (true) {
                if (System.currentTimeMillis() - startTime > POD_READINESS_TIMEOUT_SECONDS * 1000) {
                    throw new RuntimeException("Pod readiness check timed out.");
                }
                V1Pod status = api.readNamespacedPodStatus(podName, namespace, null);
                if (status.getStatus().getPhase().equals("Running")) {
                    break;
                }
                Thread.sleep(1000);
            }

            // Create a tarball of the project assets and copy it to the pod
            String projectAssetFolder = AssetUtility.getProjectAssetsFolder(projectName, projectId);
            File tempTarball = File.createTempFile("assets", ".tar");
            try (TarArchiveOutputStream tarOutput = new TarArchiveOutputStream(new FileOutputStream(tempTarball))) {
                File assetDir = new File(projectAssetFolder);
                for (File file : assetDir.listFiles()) {
                    addFileToTar(tarOutput, file, "");
                }
            }

            Exec exec = new Exec(client);
            final Process proc = exec.exec(namespace, podName, new String[]{"tar", "-xf", "-", "-C", prerna.util.Utility.getDIHelperProperty("kubernetes_asset_directory")}, true);
            new Thread(() -> {
                try (FileInputStream fis = new FileInputStream(tempTarball)) {
                    fis.transferTo(proc.getOutputStream());
                    proc.getOutputStream().close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }).start();
            
            // Consume stdout and stderr to prevent deadlocks
            Executors.newSingleThreadExecutor().submit(() -> {
                try {
                    proc.getInputStream().transferTo(OutputStream.nullOutputStream());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            Executors.newSingleThreadExecutor().submit(() -> {
                try {
                    proc.getErrorStream().transferTo(OutputStream.nullOutputStream());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });

            proc.waitFor(60, TimeUnit.SECONDS);
            
            // Create the service
            V1Service service = new V1ServiceBuilder()
                    .withNewMetadata().withName(serviceName).endMetadata()
                    .withNewSpec()
                    .withSelector(Collections.singletonMap("app", "dev-env"))
                    .addNewPort().withProtocol("TCP").withPort(Integer.parseInt(prerna.util.Utility.getDIHelperProperty("kubernetes_container_port"))).withNewTargetPort(Integer.parseInt(prerna.util.Utility.getDIHelperProperty("kubernetes_container_port"))).endPort()
                    .endSpec()
                    .build();
            api.createNamespacedService(namespace, service, null, null, null);
            serviceCreated = true;

            // Create the ingress
            V1Ingress ingress = new V1IngressBuilder()
                    .withNewMetadata().withName(ingressName).endMetadata()
                    .withNewSpec()
                    .addNewRule()
                    .withNewHttp()
                    .addNewPath()
                    .withPath("/" + projectId)
                    .withPathType("Prefix")
                    .withNewBackend()
                    .withNewService()
                    .withName(serviceName)
                    .withNewPort().withNumber(Integer.parseInt(prerna.util.Utility.getDIHelperProperty("kubernetes_container_port"))).endPort()
                    .endService()
                    .endBackend()
                    .endPath()
                    .endHttp()
                    .endRule()
                    .endSpec()
                    .build();
            networkingApi.createNamespacedIngress(namespace, ingress, null, null, null);

            project.setDevContainerPodName(podName);

            return new NounMetadata("Dev environment launched successfully. Password: " + password, PixelDataType.CONST_STRING);
        } catch (Exception e) {
            // Cleanup logic
            if (networkingApi != null && ingressName != null) {
                try {
                    networkingApi.deleteNamespacedIngress(ingressName, namespace, null, null, null, null, null, null);
                } catch (ApiException apiEx) {
                    // Ignore if not found
                }
            }
            if (api != null && serviceCreated) {
                try {
                    api.deleteNamespacedService(serviceName, namespace, null, null, null, null, null, null);
                } catch (ApiException apiEx) {
                    // Ignore if not found
                }
            }
            if (api != null && podCreated) {
                try {
                    api.deleteNamespacedPod(podName, namespace, null, null, null, null, null, null);
                } catch (ApiException apiEx) {
                    // Ignore if not found
                }
            }
            throw new RuntimeException("Failed to launch dev environment", e);
        }
    }

    private void addFileToTar(TarArchiveOutputStream tarOutput, File file, String base) throws IOException {
        String entryName = base + file.getName();
        TarArchiveEntry tarEntry = new TarArchiveEntry(file, entryName);
        tarOutput.putArchiveEntry(tarEntry);

        if (file.isFile()) {
            try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file))) {
                byte[] buffer = new byte[1024];
                int count;
                while ((count = bis.read(buffer)) != -1) {
                    tarOutput.write(buffer, 0, count);
                }
            }
            tarOutput.closeArchiveEntry();
        } else {
            tarOutput.closeArchiveEntry();
            for (File childFile : file.listFiles()) {
                addFileToTar(tarOutput, childFile, entryName + "/");
            }
        }
    }
}
