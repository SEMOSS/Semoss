package prerna.reactor.workflow;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;

public class GetWorkflowConfigReactor extends AbstractReactor {

    private static final Logger classLogger = LogManager.getLogger(GetWorkflowConfigReactor.class);
    private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();

    public GetWorkflowConfigReactor() {
        this.keysToGet = new String[]{ "project" };
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();
        String projectId = this.keyValue.get(this.keysToGet[0]);

        if (projectId == null || projectId.isEmpty()) {
            throw new IllegalArgumentException("Must provide a project id");
        }

        projectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);
        if (!SecurityProjectUtils.userCanViewProject(this.insight.getUser(), projectId)) {
            throw new IllegalArgumentException("Project does not exist or user does not have access");
        }

        String portalsFolder = AssetUtility.getProjectPortalsFolder(projectId);
        File configFile = new File(portalsFolder + "/" + WorkflowConstants.WORKFLOW_CONFIG_FILE_NAME);

        if (!configFile.exists()) {
            return new NounMetadata(new ArrayList<>(), PixelDataType.VECTOR, PixelOperationType.OPERATION);
        }

        try {
            String json = java.nio.file.Files.readString(configFile.toPath(), java.nio.charset.StandardCharsets.UTF_8);
            // strip sensitive values before returning
            java.util.List<java.util.Map<String, Object>> entries = GSON.fromJson(json, new TypeToken<java.util.List<java.util.Map<String, Object>>>() {}.getType());
            if (entries != null) {
                for (java.util.Map<String, Object> entry : entries) {
                    Object sensitive = entry.get("sensitive");
                    if (Boolean.TRUE.equals(sensitive)) {
                        entry.put("value", "***");
                    }
                }
            }
            return new NounMetadata(entries != null ? entries : new ArrayList<>(), PixelDataType.VECTOR, PixelOperationType.OPERATION);
        } catch (IOException e) {
            classLogger.error("Error reading workflow config", e);
            return new NounMetadata(new ArrayList<>(), PixelDataType.VECTOR, PixelOperationType.OPERATION);
        }
    }
}
