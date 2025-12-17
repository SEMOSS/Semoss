package prerna.reactor.project.dev_environment;

import io.kubernetes.client.Exec;
import io.kubernetes.client.custom.IntOrString;
import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.custom.V1Patch;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.apis.NetworkingV1Api;
import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1EnvVar;
import io.kubernetes.client.openapi.models.V1HTTPIngressPath;
import io.kubernetes.client.openapi.models.V1HTTPIngressRuleValue;
import io.kubernetes.client.openapi.models.V1Ingress;
import io.kubernetes.client.openapi.models.V1IngressBackend;
import io.kubernetes.client.openapi.models.V1IngressRule;
import io.kubernetes.client.openapi.models.V1IngressServiceBackend;
import io.kubernetes.client.openapi.models.V1IngressSpec;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodList;
import io.kubernetes.client.openapi.models.V1PodSecurityContext;
import io.kubernetes.client.openapi.models.V1PodSpec;
import io.kubernetes.client.openapi.models.V1ResourceRequirements;
import io.kubernetes.client.openapi.models.V1Service;
import io.kubernetes.client.openapi.models.V1ServiceBackendPort;
import io.kubernetes.client.openapi.models.V1ServicePort;
import io.kubernetes.client.openapi.models.V1ServiceSpec;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.KubernetesUtil;
import prerna.util.Utility;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class LaunchDevEnvironmentReactor extends AbstractReactor {

    private static final Logger logger = LogManager.getLogger(LaunchDevEnvironmentReactor.class);
    private static final ExecutorService DEV_ENV_EXECUTOR = Executors.newCachedThreadPool();
    private static final long POD_READINESS_TIMEOUT_SECONDS = 300;

    public LaunchDevEnvironmentReactor() {
        this.keysToGet = new String[]{"projectId"};
    }

    @Override
    public NounMetadata execute() {
        DevEnvironmentUtils.ensureDevContainersEnabled();
        organizeKeys();
        String projectId = this.keyValue.get("projectId");

        if (projectId == null || projectId.isEmpty()) {
            throw new IllegalArgumentException("Project ID is required to launch the dev environment.");
        }

        User user = this.insight.getUser();
        if (!SecurityProjectUtils.userCanEditProject(user, projectId)) {
            throw new SecurityException("User does not have permission to launch the dev environment for this project.");
        }

        IProject project = Utility.getProject(projectId);
        if (project == null) {
            throw new IllegalStateException("Project not found: " + projectId);
        }
        String projectName = project.getProjectName() != null ? project.getProjectName() : projectId;

        try {
            ApiClient client = KubernetesUtil.getApiClient();
            CoreV1Api api = new CoreV1Api(client);
            NetworkingV1Api networkingApi = new NetworkingV1Api(client);
            String namespace = KubernetesUtil.getNamespace();
            V1Pod existingPod = findExistingPod(api, namespace, projectId);
            if (existingPod != null) {
                return new NounMetadata(
                        "Dev environment already exists. Use GetDevEnvironmentStatus to view its details.",
                        PixelDataType.CONST_STRING);
            }

            String password = RandomStringUtils.randomAlphanumeric(16);
            String podName = DevEnvironmentUtils.podName(projectId);
            String serviceName = DevEnvironmentUtils.serviceName(projectId);
            String ingressName = DevEnvironmentUtils.ingressName(projectId);
            int containerPort = Integer.parseInt(Utility.getDIHelperProperty("kubernetes_container_port"));
            String idleTimeoutSeconds = Utility.getDIHelperProperty("code_server_idle_timeout_seconds");
            if (idleTimeoutSeconds == null || idleTimeoutSeconds.isEmpty()) {
                idleTimeoutSeconds = "1800";
            }

            V1Pod pod = buildPod(projectId, podName, password, containerPort, idleTimeoutSeconds);
            api.createNamespacedPod(namespace, pod).execute();

            V1Service service = buildService(projectId, serviceName, containerPort);
            api.createNamespacedService(namespace, service).execute();

            V1Ingress ingress = buildIngress(projectId, serviceName, ingressName);
            networkingApi.createNamespacedIngress(namespace, ingress).execute();

            project.setDevContainerPodName(podName);

            scheduleAssetSync(client, namespace, podName, projectName, projectId);

            return new NounMetadata(
                    "Dev environment provisioning started. Use GetDevEnvironmentStatus to retrieve the connection details once the pod is ready.",
                    PixelDataType.CONST_STRING);
        } catch (Exception e) {
            logger.error("Failed to launch dev environment", e);
            throw new RuntimeException("Failed to launch dev environment", e);
        }
    }

    private V1Pod findExistingPod(CoreV1Api api, String namespace, String projectId) throws ApiException {
        V1PodList list = api.listNamespacedPod(namespace)
                .labelSelector(DevEnvironmentUtils.buildLabelSelector(projectId))
                .limit(1)
                .execute();
        return list.getItems().isEmpty() ? null : list.getItems().get(0);
    }

    private V1Pod buildPod(String projectId, String podName, String password, int containerPort, String idleTimeoutSeconds) {
        String now = OffsetDateTime.now().toString();
        Map<String, String> labels = new HashMap<>();
        labels.put(DevEnvironmentUtils.APP_LABEL_KEY, DevEnvironmentUtils.APP_LABEL_VALUE);
        labels.put(DevEnvironmentUtils.PROJECT_LABEL_KEY,
                DevEnvironmentUtils.sanitizeLabelValue(projectId));

        Map<String, String> annotations = new HashMap<>();
        annotations.put(DevEnvironmentUtils.ANNOTATION_LAST_ACTIVITY, now);
        annotations.put(DevEnvironmentUtils.ANNOTATION_PASSWORD, password);
        annotations.put(DevEnvironmentUtils.ANNOTATION_ASSET_STATUS,
                DevEnvironmentUtils.ASSET_STATUS_PENDING);

        V1ObjectMeta podMeta = new V1ObjectMeta()
                .name(podName)
                .labels(labels)
                .annotations(annotations);

        V1ResourceRequirements resources = new V1ResourceRequirements();
        Map<String, Quantity> requests = new HashMap<>();
        requests.put("cpu", Quantity.fromString(Utility.getDIHelperProperty("kubernetes_cpu_request")));
        requests.put("memory", Quantity.fromString(Utility.getDIHelperProperty("kubernetes_mem_request")));
        resources.setRequests(requests);
        Map<String, Quantity> limits = new HashMap<>();
        limits.put("cpu", Quantity.fromString(Utility.getDIHelperProperty("kubernetes_cpu_limit")));
        limits.put("memory", Quantity.fromString(Utility.getDIHelperProperty("kubernetes_mem_limit")));
        resources.setLimits(limits);

        V1Container container = new V1Container()
                .name("code-server")
                .image(Utility.getDIHelperProperty("code_server_image"))
                .args(Arrays.asList(
                        "--bind-addr", "0.0.0.0:" + containerPort,
                        "--auth=password",
                        "--idle-timeout-seconds", idleTimeoutSeconds
                ))
                .env(Collections.singletonList(new V1EnvVar().name("PASSWORD").value(password)))
                .resources(resources);

        V1PodSpec podSpec = new V1PodSpec()
                .securityContext(new V1PodSecurityContext().runAsUser(1000L).runAsGroup(1000L).fsGroup(1000L))
                .containers(Collections.singletonList(container));
        podSpec.setOverhead(null);

        return new V1Pod()
                .metadata(podMeta)
                .spec(podSpec);
    }

    private V1Service buildService(String projectId, String serviceName, int containerPort) {
        V1ObjectMeta serviceMeta = new V1ObjectMeta().name(serviceName);
        Map<String, String> selector = new HashMap<>();
        selector.put(DevEnvironmentUtils.APP_LABEL_KEY, DevEnvironmentUtils.APP_LABEL_VALUE);
        selector.put(DevEnvironmentUtils.PROJECT_LABEL_KEY,
                DevEnvironmentUtils.sanitizeLabelValue(projectId));

        V1ServicePort port = new V1ServicePort()
                .protocol("TCP")
                .port(containerPort)
                .targetPort(new IntOrString(containerPort));
        V1ServiceSpec serviceSpec = new V1ServiceSpec()
                .selector(selector)
                .ports(Collections.singletonList(port));

        return new V1Service()
                .metadata(serviceMeta)
                .spec(serviceSpec);
    }

    private V1Ingress buildIngress(String projectId, String serviceName, String ingressName) {
        int containerPort = Integer.parseInt(Utility.getDIHelperProperty("kubernetes_container_port"));
        V1IngressBackend backend = new V1IngressBackend()
                .service(new V1IngressServiceBackend()
                        .name(serviceName)
                        .port(new V1ServiceBackendPort().number(containerPort)));
        V1HTTPIngressPath path = new V1HTTPIngressPath()
                .path("/" + projectId)
                .pathType("Prefix")
                .backend(backend);
        V1IngressRule rule = new V1IngressRule()
                .http(new V1HTTPIngressRuleValue().paths(Collections.singletonList(path)));

        return new V1Ingress()
                .metadata(new V1ObjectMeta().name(ingressName))
                .spec(new V1IngressSpec().rules(Collections.singletonList(rule)));
    }

    private void scheduleAssetSync(ApiClient client, String namespace, String podName,
                                   String projectName, String projectId) {
        DEV_ENV_EXECUTOR.submit(() -> {
            File tempTarball = null;
            try {
                CoreV1Api api = new CoreV1Api(client);
                Exec exec = new Exec(client);
                waitForPodReady(api, namespace, podName);
                tempTarball = createProjectAssetTarball(projectName, projectId);
                streamTarballToPod(exec, namespace, podName, tempTarball);
                markAnnotation(api, namespace, podName,
                        DevEnvironmentUtils.ANNOTATION_ASSET_STATUS,
                        DevEnvironmentUtils.ASSET_STATUS_SYNCED);
            } catch (Exception ex) {
                logger.error("Failed to sync project assets to pod {}", podName, ex);
                try {
                    CoreV1Api api = new CoreV1Api(client);
                    markAnnotation(api, namespace, podName,
                            DevEnvironmentUtils.ANNOTATION_ASSET_STATUS,
                            DevEnvironmentUtils.ASSET_STATUS_FAILED);
                } catch (Exception inner) {
                    logger.debug("Failed to update asset status annotation for pod {}", podName, inner);
                }
            } finally {
                if (tempTarball != null && tempTarball.exists() && !tempTarball.delete()) {
                    logger.debug("Unable to delete temporary tarball {}", tempTarball.getAbsolutePath());
                }
            }
        });
    }

    private void waitForPodReady(CoreV1Api api, String namespace, String podName) throws InterruptedException, ApiException {
        long startTime = System.currentTimeMillis();
        while (true) {
            if (System.currentTimeMillis() - startTime > POD_READINESS_TIMEOUT_SECONDS * 1000) {
                throw new RuntimeException("Pod readiness check timed out.");
            }
            V1Pod status = api.readNamespacedPodStatus(podName, namespace).execute();
            if (status.getStatus() != null && "Running".equalsIgnoreCase(status.getStatus().getPhase())) {
                return;
            }
            Thread.sleep(2000);
        }
    }

    private File createProjectAssetTarball(String projectName, String projectId) throws IOException {
        String projectAssetFolder = AssetUtility.getProjectAssetsFolder(projectName, projectId);
        File tempTarball = File.createTempFile("assets", ".tar");
        try (TarArchiveOutputStream tarOutput = new TarArchiveOutputStream(new FileOutputStream(tempTarball))) {
            File assetDir = new File(projectAssetFolder);
            File[] files = assetDir.listFiles();
            if (files != null) {
                for (File file : files) {
                    addFileToTar(tarOutput, file, "");
                }
            }
        }
        return tempTarball;
    }

    private void streamTarballToPod(Exec exec, String namespace, String podName, File tarball) throws IOException, InterruptedException {
        String assetDir = Utility.getDIHelperProperty("kubernetes_asset_directory");
        Process proc;
		try {
			proc = exec.exec(namespace, podName, new String[]{"tar", "-xf", "-", "-C", assetDir}, true);
	
        DEV_ENV_EXECUTOR.submit(() -> {
            try {
                proc.getInputStream().transferTo(OutputStream.nullOutputStream());
            } catch (IOException ignored) {
            }
        });
        DEV_ENV_EXECUTOR.submit(() -> {
            try {
                proc.getErrorStream().transferTo(OutputStream.nullOutputStream());
            } catch (IOException ignored) {
            }
        });

        try (FileInputStream fis = new FileInputStream(tarball)) {
            fis.transferTo(proc.getOutputStream());
        }
        proc.getOutputStream().close();
        proc.waitFor(60, TimeUnit.SECONDS);
		} catch (ApiException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

    private void markAnnotation(CoreV1Api api, String namespace, String podName, String key, String value) throws ApiException {
        String escapedKey = key.replace("/", "~1");
        String patchJson = String.format("[{\"op\":\"add\",\"path\":\"/metadata/annotations/%s\",\"value\":\"%s\"}]",
                escapedKey, value);
        api.patchNamespacedPod(podName, namespace, new V1Patch(patchJson)).execute();
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
            File[] children = file.listFiles();
            if (children != null) {
                for (File childFile : children) {
                    addFileToTar(tarOutput, childFile, entryName + "/");
                }
            }
        }
    }
}
