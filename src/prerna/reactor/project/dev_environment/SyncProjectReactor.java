package prerna.reactor.project.dev_environment;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.OffsetDateTime;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;

import io.kubernetes.client.Exec;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.KubernetesUtil;
import prerna.sablecc2.om.PixelDataType;

public class SyncProjectReactor extends AbstractReactor {

    public SyncProjectReactor() {
        this.keysToGet = new String[]{"projectId", "projectName"};
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();
        String projectId = this.keyValue.get("projectId");
        String projectName = this.keyValue.get("projectName");

        if (projectId == null || projectId.isEmpty()) {
            throw new IllegalArgumentException("Project ID is required to sync the project.");
        }
        if (projectName == null || projectName.isEmpty()) {
            throw new IllegalArgumentException("Project name is required to sync the project.");
        }

        User user = this.insight.getUser();
        if (!SecurityProjectUtils.userCanEditProject(user, projectId)) {
            throw new SecurityException("User does not have permission to sync the dev environment for this project.");
        }

        try {
            ApiClient client = KubernetesUtil.getApiClient();
            CoreV1Api api = new CoreV1Api(client);
            Exec exec = new Exec(client);

            String namespace = KubernetesUtil.getNamespace();
            String podName = "dev-env-" + projectId;
            String projectAssetFolder =  AssetUtility.getProjectAssetsFolder(projectName, projectId);

            // Update the last-activity annotation
            String now = OffsetDateTime.now().toString();
            String jsonPatch = String.format("[{ \"op\": \"replace\", \"path\": \"/metadata/annotations/semoss.org~1last-activity\", \"value\": \"%s\" }]", now);
            api.patchNamespacedPod(podName, namespace, jsonPatch, null, null, null, null);

            // Create a tarball of the project assets in the container
            String[] command = new String[]{"/bin/sh", "-c", "tar -czf - -C " + prerna.util.Utility.getDIHelperProperty("kubernetes_asset_directory") + " ."};
            final Process proc = exec.exec(namespace, podName, command, false, true); // Stdin=false, Stderr=true

            // Consume stderr to prevent deadlocks
            Executors.newSingleThreadExecutor().submit(() -> {
                try {
                    proc.getErrorStream().transferTo(OutputStream.nullOutputStream());
                } catch (IOException e) {
                    // Log this or handle it appropriately
                    e.printStackTrace();
                }
            });

            // Copy the tarball from the container to the local filesystem
            try (InputStream inputStream = proc.getInputStream();
                 TarArchiveInputStream tarInput = new TarArchiveInputStream(inputStream)) {

                TarArchiveEntry entry;
                while ((entry = tarInput.getNextTarEntry()) != null) {
                    java.nio.file.Path extractTo = Paths.get(projectAssetFolder).resolve(entry.getName());
                    if (entry.isDirectory()) {
                        Files.createDirectories(extractTo);
                    } else {
                        Files.createDirectories(extractTo.getParent());
                        Files.copy(tarInput, extractTo, StandardCopyOption.REPLACE_EXISTING);
                    }
                }
            }
            
            proc.waitFor(60, TimeUnit.SECONDS);

            return new NounMetadata("Project synced successfully", PixelDataType.CONST_STRING);
        } catch (IOException | ApiException e) {
            throw new RuntimeException("Failed to sync project", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Project sync was interrupted", e);
        }
    }
}
