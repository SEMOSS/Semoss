package prerna.reactor.workflow;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Utility;
import prerna.util.git.GitRepoUtils;

public class SaveWorkflowReactor extends AbstractReactor {

    private static final Logger classLogger = LogManager.getLogger(SaveWorkflowReactor.class);

    public SaveWorkflowReactor() {
        this.keysToGet = new String[]{ "project", "json" };
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();
        String projectId = this.keyValue.get(this.keysToGet[0]);
        String jsonEncoded = this.keyValue.get(this.keysToGet[1]);

        if (projectId == null || projectId.isEmpty()) {
            throw new IllegalArgumentException("Must provide a project id");
        }
        if (jsonEncoded == null || jsonEncoded.isEmpty()) {
            throw new IllegalArgumentException("Must provide workflow JSON");
        }

        projectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);
        if (!SecurityProjectUtils.userCanEditProject(this.insight.getUser(), projectId)) {
            throw new IllegalArgumentException("Project does not exist or user does not have edit access");
        }

        String json;
        try {
            json = java.net.URLDecoder.decode(jsonEncoded, java.nio.charset.StandardCharsets.UTF_8);
        } catch (Exception e) {
            json = jsonEncoded;
        }

        IProject project = Utility.getProject(projectId);
        String portalsFolder = AssetUtility.getProjectPortalsFolder(projectId);
        File workflowFile = new File(portalsFolder + "/" + WorkflowConstants.WORKFLOW_FILE_NAME);

        try {
            workflowFile.getParentFile().mkdirs();
            java.nio.file.Files.writeString(workflowFile.toPath(), json, java.nio.charset.StandardCharsets.UTF_8);
        } catch (IOException e) {
            classLogger.error("Error saving workflow JSON", e);
            throw new IllegalArgumentException("Unable to save workflow: " + e.getMessage());
        }

        List<String> files = new Vector<>();
        files.add(workflowFile.getAbsolutePath());
        String versionFolder = AssetUtility.getProjectVersionFolder(project.getProjectName(), projectId);
        try {
            GitRepoUtils.addSpecificFiles(versionFolder, files);
            GitRepoUtils.commitAddedFiles(versionFolder, "Update workflow graph", this.insight.getUser());
        } catch (Exception e) {
            classLogger.warn("Git commit failed for workflow save", e);
        }

        if (ClusterUtil.IS_CLUSTER) {
            try {
                ClusterUtil.pushProjectFolder(project, versionFolder);
            } catch (Exception e) {
                classLogger.warn("Cluster push failed", e);
            }
        }

        SecurityProjectUtils.updateProjectLastEditedDate(projectId);
        return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.OPERATION);
    }
}
