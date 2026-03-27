package prerna.reactor.agent.mcp.tools;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.impl.SmssUtilities;
import prerna.project.api.IProject;
import prerna.project.impl.Project;
import prerna.reactor.AbstractReactor;
import prerna.reactor.IReactor;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.reactor.agent.mcp.MCPUtility.MCPExecution;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.EngineUtility;
import prerna.util.Utility;
import prerna.util.git.GitRepoUtils;

/**
 * Creates (or reuses) a dedicated MCP project for the room-file tools in this
 * package, sets the insight context to that project, and writes the
 * pixel_mcp.json with hardcoded tool definitions.
 *
 * The project is created as a global (public) system project with no owner.
 * Call {@link #initSystemProject()} during server bootup (after the security
 * database is loaded) to ensure the project exists before any user request.
 */
public class MakeRoomToolsMCPReactor extends AbstractReactor {

    private static final Logger classLogger = LogManager.getLogger(MakeRoomToolsMCPReactor.class);

    /** Well-known project name used for the Room Tools MCP project. */
    static final String PROJECT_NAME = "Room Tools MCP";

    /**
     * Deterministic project ID derived from the project name so it survives
     * restarts.
     */
    static final String PROJECT_ID = UUID.nameUUIDFromBytes(
            PROJECT_NAME.getBytes(StandardCharsets.UTF_8)).toString();

    private static final List<Class<? extends IReactor>> ROOM_TOOLS = Arrays.asList(
            ListRoomFilesReactor.class,
            ReadRoomFilesReactor.class,
            SearchRoomFilesWithContextReactor.class,
            GetRoomFileTokenStatsReactor.class,
            GetRoomTokenUsageReactor.class,
            ExecuteRoomShellCommandReactor.class);

    public MakeRoomToolsMCPReactor() {
        this.keysToGet = new String[0];
    }

    // ------------------------------------------------------------------ //
    // System-level bootup initialisation (no User required) //
    // ------------------------------------------------------------------ //

    /**
     * Ensures the Room Tools MCP project exists on disk and is registered in
     * the security database as a global project with no owner.
     * <p>
     * Safe to call on every boot — skips work that has already been done.
     * Must be called <b>after</b> the security database is loaded.
     */
    public static void initSystemProject() {
        try {
            ensureProjectOnDisk();
            registerWithDIHelper();
            SecurityProjectUtils.addProject(PROJECT_ID, true, null);
            writeMcpJson();
            classLogger.info("Room Tools MCP system project initialised (id={})", PROJECT_ID);
        } catch (Exception e) {
            classLogger.error("Failed to initialise Room Tools MCP system project", e);
        }
    }

    /**
     * Creates the project folder and SMSS file if they do not already exist.
     */
    private static void ensureProjectOnDisk() throws IOException {
        // project folder: <projectBase>/<ProjectName>__<projectId>/
        String projectBaseFolder = EngineUtility.getSpecificEngineBaseFolder(
                prerna.engine.api.IEngine.CATALOG_TYPE.PROJECT, PROJECT_ID, PROJECT_NAME);
        File projectFolder = new File(projectBaseFolder);
        if (!projectFolder.exists()) {
            projectFolder.mkdirs();
        }

        // SMSS file: <projectBase>/<ProjectName>__<projectId>.smss
        String smssFilePath = EngineUtility.PROJECT_FOLDER + "/"
                + SmssUtilities.getUniqueName(PROJECT_NAME, PROJECT_ID) + ".smss";
        File smssFile = new File(smssFilePath);
        if (!smssFile.exists()) {
            File tempSmss = SmssUtilities.createTemporaryProjectSmss(
                    PROJECT_ID, PROJECT_NAME, IProject.PROJECT_TYPE.CODE,
                    false, null, null, null, null);
            File target = new File(tempSmss.getAbsolutePath().replace(".temp", ".smss"));
            FileUtils.copyFile(tempSmss, target);
            tempSmss.delete();
            classLogger.info("Created SMSS file for Room Tools MCP at {}", target.getAbsolutePath());
        }
    }

    /**
     * Registers the project with DIHelper so that
     * {@link SecurityProjectUtils#addProject} can locate the SMSS file and
     * so that {@code ProjectWatcher.catalogProject} will see the project as
     * already loaded.
     */
    private static void registerWithDIHelper() {
        String smssFilePath = EngineUtility.PROJECT_FOLDER + "/"
                + SmssUtilities.getUniqueName(PROJECT_NAME, PROJECT_ID) + ".smss";

        DIHelper diHelper = DIHelper.getInstance();
        diHelper.setProjectProperty(PROJECT_ID + "_" + Constants.STORE, smssFilePath);

        // ensure projectId is in the projects list
        String projects = (String) diHelper.getProjectProperty(Constants.PROJECTS);
        if (projects == null) {
            projects = "";
        }
        if (!projects.startsWith(PROJECT_ID)
                && !projects.contains(";" + PROJECT_ID + ";")
                && !projects.endsWith(";" + PROJECT_ID)) {
            projects = projects + ";" + PROJECT_ID;
            diHelper.setProjectProperty(Constants.PROJECTS, projects);
        }

        // ensure the IProject instance is registered
        if (!Utility.projectLoaded(PROJECT_ID)) {
            IProject project = new Project();
            try {
                project.open(smssFilePath);
            } catch (Exception e) {
                classLogger.error("Unable to open Room Tools MCP project from SMSS", e);
            }
            diHelper.setProjectProperty(PROJECT_ID, project);
        }
    }

    /**
     * Writes (or overwrites) the {@code pixel_mcp.json} in the project assets
     * folder with the current set of room tool definitions.
     */
    private static void writeMcpJson() throws IOException {
        String projectAssetFolder = AssetUtility.getProjectAssetsFolder(PROJECT_NAME, PROJECT_ID);

        JSONArray toolsArray = new JSONArray();
        for (Class<? extends IReactor> reactorClass : ROOM_TOOLS) {
            IReactor reactor;
            try {
                reactor = reactorClass.getConstructor().newInstance();
            } catch (Exception e) {
                classLogger.error("Could not instantiate {}", reactorClass.getName(), e);
                continue;
            }
            JSONObject tool = reactor.asMcpTool();
            JSONObject meta = tool.optJSONObject("_meta");
            if (meta == null) {
                meta = new JSONObject();
            }
            meta.put(MCPUtility.SMSS_FUNCTION_NAME, tool.getString("name"));
            meta.put(MCPUtility.SMSS_MCP_EXECUTION, MCPExecution.AUTO.getValue());
            meta.put(MCPUtility.SMSS_MCP_UI, new JSONObject());
            tool.put("_meta", meta);
            toolsArray.put(tool);
        }

        JSONObject mcpJson = new JSONObject();
        mcpJson.put("tools", toolsArray);
        JSONObject topMeta = new JSONObject();
        topMeta.put("last_modified_date",
                LocalDate.now(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
        mcpJson.put("_meta", topMeta);

        String outputFileLoc = projectAssetFolder + "/mcp/pixel_mcp.json";
        File outputFile = new File(outputFileLoc);
        if (!outputFile.getParentFile().exists()) {
            outputFile.getParentFile().mkdirs();
        }
        try (FileWriter writer = new FileWriter(outputFile)) {
            writer.write(mcpJson.toString(4));
        }
    }

    // ------------------------------------------------------------------ //
    // Reactor execution (user-invoked) //
    // ------------------------------------------------------------------ //

    @Override
    public NounMetadata execute() {
        organizeKeys();

        User user = this.insight.getUser();
        if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
            throwAnonymousUserError();
        }

        // ensure the system project exists (idempotent)
        initSystemProject();

        // switch context to the project
        if (!this.insight.setContext(PROJECT_ID)) {
            throw new IllegalArgumentException(
                    "Unable to set context to project " + PROJECT_ID + ". User may lack view access.");
        }

        IProject project = Utility.getProject(PROJECT_ID);

        // tag and commit
        MCPUtility.addMCPTag(project);

        String versionGitFolder = AssetUtility.getProjectVersionFolder(PROJECT_NAME, PROJECT_ID);
        List<String> gitRelativeFilePaths = new ArrayList<>();
        gitRelativeFilePaths.add(Constants.ASSETS_FOLDER + DIR_SEPARATOR + "/mcp/pixel_mcp.json");

        AccessToken accessToken = user.getAccessToken(user.getPrimaryLogin());
        String email = accessToken.getEmail();
        String author = accessToken.getUsername();

        GitRepoUtils.addSpecificFiles(versionGitFolder, gitRelativeFilePaths);
        GitRepoUtils.commitAddedFiles(versionGitFolder, "add: MakeRoomToolsMCP executed", author, email);

        String assetFolder = AssetUtility.getProjectAssetsFolder(PROJECT_NAME, PROJECT_ID);
        ClusterUtil.pushProjectFolder(project, assetFolder);

        // return the MCP JSON that was written
        String mcpFilePath = assetFolder + "/mcp/pixel_mcp.json";
        try {
            String content = FileUtils.readFileToString(new File(mcpFilePath), StandardCharsets.UTF_8);
            return new NounMetadata(new JSONObject(content), PixelDataType.JSON_OBJECT);
        } catch (IOException e) {
            throw new IllegalArgumentException("Unable to read pixel_mcp.json after write: " + e.getMessage());
        }
    }

    @Override
    public String getReactorDescription() {
        return "Creates or reuses a dedicated MCP project for room-file tools, sets the context to that project, "
                + "and generates the pixel_mcp.json with all room tool definitions.";
    }
}
