package prerna.reactor.workflow;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Utility;

public class GetWorkflowReactor extends AbstractReactor {

    private static final Logger classLogger = LogManager.getLogger(GetWorkflowReactor.class);
    private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();

    public GetWorkflowReactor() {
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

        IProject project = Utility.getProject(projectId);
        if (project.requirePublish(true)) {
            classLogger.info("Pulled project {} from cluster", projectId);
        }

        String portalsFolder = AssetUtility.getProjectPortalsFolder(projectId);
        File workflowFile = new File(portalsFolder + "/" + WorkflowConstants.WORKFLOW_FILE_NAME);

        if (!workflowFile.exists() || !workflowFile.isFile()) {
            // return empty graph document for brand-new workflows
            Map<String, Object> empty = new HashMap<>();
            empty.put("version", 1);
            Map<String, Object> graph = new HashMap<>();
            graph.put("nodes", new java.util.ArrayList<>());
            graph.put("edges", new java.util.ArrayList<>());
            empty.put("graph", graph);
            return new NounMetadata(empty, PixelDataType.MAP, PixelOperationType.OPERATION);
        }

        try {
            String json = java.nio.file.Files.readString(workflowFile.toPath(), java.nio.charset.StandardCharsets.UTF_8);
            Map<String, Object> doc = GSON.fromJson(json, new TypeToken<Map<String, Object>>() {}.getType());
            return new NounMetadata(doc, PixelDataType.MAP, PixelOperationType.OPERATION);
        } catch (IOException e) {
            classLogger.error("Error reading workflow JSON", e);
            throw new IllegalArgumentException("Unable to read workflow: " + e.getMessage());
        }
    }
}
