package prerna.reactor.project.dev_environment;

import io.kubernetes.client.Exec;
import io.kubernetes.client.custom.IntOrString;
import io.kubernetes.client.custom.Quantity;
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
import io.kubernetes.client.openapi.models.V1PodSecurityContext;
import io.kubernetes.client.openapi.models.V1PodSpec;
import io.kubernetes.client.openapi.models.V1ResourceRequirements;
import io.kubernetes.client.openapi.models.V1Service;
import io.kubernetes.client.openapi.models.V1ServiceBackendPort;
import io.kubernetes.client.openapi.models.V1ServicePort;
import io.kubernetes.client.openapi.models.V1ServiceSpec;
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
import java.util.HashMap;
import java.util.Map;
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
    	
    	System.out.println(io.kubernetes.client.openapi.models.V1PodStatus.class
    		    .getProtectionDomain().getCodeSource().getLocation());
    		System.out.println(io.kubernetes.client.openapi.models.V1PodStatus.class
    		    .getPackage().getImplementationVersion());
    		
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

        
        IProject project = Utility.getProject(projectId);
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
            Map<String, String> labels = new HashMap<>();
            labels.put("app", "dev-env");
            Map<String, String> annotations = new HashMap<>();
            annotations.put("semoss.org/last-activity", now);

            V1ObjectMeta podMeta = new V1ObjectMeta()
                    .name(podName)
                    .labels(labels)
                    .annotations(annotations);

            V1ResourceRequirements resources = new V1ResourceRequirements();
            Map<String, Quantity> requests = new HashMap<>();
            requests.put("cpu", Quantity.fromString(prerna.util.Utility.getDIHelperProperty("kubernetes_cpu_request")));
            requests.put("memory", Quantity.fromString(prerna.util.Utility.getDIHelperProperty("kubernetes_mem_request")));
            resources.setRequests(requests);
            Map<String, Quantity> limits = new HashMap<>();
            limits.put("cpu", Quantity.fromString(prerna.util.Utility.getDIHelperProperty("kubernetes_cpu_limit")));
            limits.put("memory", Quantity.fromString(prerna.util.Utility.getDIHelperProperty("kubernetes_mem_limit")));
            resources.setLimits(limits);

            V1Container container = new V1Container()
                    .name("code-server")
                    .image(prerna.util.Utility.getDIHelperProperty("code_server_image"))
                    .args(Collections.singletonList("--auth=password"))
                    .env(Collections.singletonList(new V1EnvVar().name("PASSWORD").value(password)))
                    .resources(resources);

            V1PodSpec podSpec = new V1PodSpec()
                    .securityContext(new V1PodSecurityContext().runAsUser(1000L).runAsGroup(1000L).fsGroup(1000L))
                    .containers(Collections.singletonList(container));
            podSpec.setOverhead(null);

            V1Pod pod = new V1Pod()
                    .metadata(podMeta)
                    .spec(podSpec);

            api.createNamespacedPod(namespace, pod).execute();
            podCreated = true;

            // Wait for the pod to be running
            long startTime = System.currentTimeMillis();
            while (true) {
                if (System.currentTimeMillis() - startTime > POD_READINESS_TIMEOUT_SECONDS * 1000) {
                    throw new RuntimeException("Pod readiness check timed out.");
                }
                V1Pod status = api.readNamespacedPodStatus(podName, namespace).execute();
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

            System.out.println("asset dir = " + Utility.getDIHelperProperty("kubernetes_asset_directory"));
            
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
            V1ObjectMeta serviceMeta = new V1ObjectMeta().name(serviceName);
            int containerPort = Integer.parseInt(prerna.util.Utility.getDIHelperProperty("kubernetes_container_port"));
            V1ServicePort port = new V1ServicePort()
                    .protocol("TCP")
                    .port(containerPort)
                    .targetPort(new IntOrString(containerPort));
            V1ServiceSpec serviceSpec = new V1ServiceSpec()
                    .selector(Collections.singletonMap("app", "dev-env"))
                    .ports(Collections.singletonList(port));
            V1Service service = new V1Service()
                    .metadata(serviceMeta)
                    .spec(serviceSpec);
            api.createNamespacedService(namespace, service).execute();
            serviceCreated = true;

            // Create the ingress
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
            V1Ingress ingress = new V1Ingress()
                    .metadata(new V1ObjectMeta().name(ingressName))
                    .spec(new V1IngressSpec().rules(Collections.singletonList(rule)));
            networkingApi.createNamespacedIngress(namespace, ingress).execute();

            project.setDevContainerPodName(podName);

            return new NounMetadata("Dev environment launched successfully. Password: " + password, PixelDataType.CONST_STRING);
        } catch (Exception e) {
            // Cleanup logic
            if (networkingApi != null && ingressName != null) {
                try {
                    networkingApi.deleteNamespacedIngress(ingressName, namespace).execute();
                } catch (ApiException apiEx) {
                    // Ignore if not found
                }
            }
            if (api != null && serviceCreated) {
                try {
                    api.deleteNamespacedService(serviceName, namespace).execute();
                } catch (ApiException apiEx) {
                    // Ignore if not found
                }
            }
            if (api != null && podCreated) {
                try {
                    api.deleteNamespacedPod(podName, namespace).execute();
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
