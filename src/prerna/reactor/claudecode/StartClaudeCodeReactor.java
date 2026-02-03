package prerna.reactor.claudecode;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.reactor.AbstractReactor;
import prerna.reactor.database.CommandReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IEngine;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.CmdExecUtil;
import prerna.util.Constants;
import prerna.util.EngineUtility;
import prerna.util.Utility;
import prerna.util.AssetUtility;

public class StartClaudeCodeReactor extends AbstractReactor {
    
    private static final Logger classLogger = LogManager.getLogger(StartClaudeCodeReactor.class);
    
    public StartClaudeCodeReactor() {
        this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.ENGINE.getKey() };
        this.keyRequired = new int[] { 1, 1 };
    }
    
    @Override
    public NounMetadata execute() {
        organizeKeys();
        String projectId = this.keyValue.get(ReactorKeysEnum.PROJECT.getKey());
        String engineId = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());
        User user = this.insight.getUser();

		if (!SecurityEngineUtils.userCanViewEngine(user, engineId)) {
			throw new IllegalArgumentException(
					"Model " + engineId + " does not exist or user does not have access to this model");
		}
        
        
        boolean projectContext = this.insight.setContext(projectId);
        if (!projectContext) {
            throw new RuntimeException("Failed to set project context");
        }
        
        String projectAssetFolder = AssetUtility.getProjectAssetsFolder(projectId);
        String claudeFolderPath = (projectAssetFolder + "/" + ".claude").replace('\\', '/');
        File claudeFolder = new File(claudeFolderPath);
        if(claudeFolder.exists() && claudeFolder.isDirectory()) {
            throw new IllegalArgumentException("Folder already exists");
        }
        claudeFolder.mkdirs();
        
        String settingsFilePath = claudeFolderPath + "/settings.json";
        File settingsFile = new File(settingsFilePath);
        
        String jsonContent = "{\n" +
            "  \"model\": \"" + engineId + "\",\n" +
            "  \"env\": {\n" +
            "    \"ANTHROPIC_BASE_URL\": \"http://localhost:9090/Monolith/api/model/anthropic\",\n" +
            "    \"ANTHROPIC_AUTH_TOKEN\": \"790b8c5f-4817-4faf-b1d1-8647fe04e4be:d946f2fc-0f8b-4a7e-ae59-4e41d2e07cad\",\n" +
            "    \"ANTHROPIC_API_KEY\": \"790b8c5f-4817-4faf-b1d1-8647fe04e4be:d946f2fc-0f8b-4a7e-ae59-4e41d2e07cad\",\n" +
            "    \"CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC\": \"true\"\n" +
            "  }\n" +
            "}";
        
        try (FileWriter writer = new FileWriter(settingsFile)) {
            writer.write(jsonContent);
            classLogger.info("Created settings.json at: " + settingsFilePath);
        } catch (IOException e) {
            classLogger.error("Failed to create settings.json", e);
            throw new RuntimeException("Failed to create settings.json file", e);
        }
        
        CmdExecUtil cmdUtil = this.insight.getCmdUtil();
        
        // Is it on the PATH?
        String whichResult = cmdUtil.executeCommand("which claude");
        classLogger.info("which claude: " + whichResult);

        // Can it respond at all?
        String versionResult = cmdUtil.executeCommand("claude --version");
        classLogger.info("claude --version: " + versionResult);

        // What does the environment look like?
        String envResult = cmdUtil.executeCommand("env");
        classLogger.info("environment: " + envResult);
        
        String response = cmdUtil.executeCommand("claude --dangerously-skip-permissions");
        classLogger.info("Claude command response: " + response);

        return new NounMetadata(response, PixelDataType.CONST_STRING);
    }

}